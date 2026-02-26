/*
 * peer.cpp — Peer Node for Gossip-based P2P Network
 * Assignment 1, CSL3080 — Computer Networks
 *
 * Compile: g++ -std=c++17 -pthread -Wall -Wextra -O2 -o peer src/peer.cpp
 * Usage  : ./peer <port> [config_file]
 *
 * The PeerNode class:
 *  - Reads seed list from config file.
 *  - Registers with floor(n/2)+1 seeds (quorum consensus).
 *  - Merges peer lists from seeds; selects neighbors via power-law.
 *  - Generates gossip every 5s (max 10); forwards using ML dedup.
 *  - Pings neighbors; two-level dead-node consensus before reporting.
 *  - Logs to stdout + peer_output_<port>.txt.
 */

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <unordered_set>
#include <cstring>
#include <ctime>
#include <cmath>
#include <cstdlib>
#include <stdexcept>
#include <algorithm>
#include <random>
#include <chrono>
#include <functional>
#include <mutex>
#include <thread>
#include <atomic>

using namespace std;

// Type aliases ────────────────────────────────────────────────────────────── */
using sock_t = int;
static constexpr sock_t SOCK_INVALID = -1;
static constexpr int    SOCK_ERR     = -1;
#define CLOSE_SOCK(s) ::close(s)

// Helpers

static string now_str() {
    time_t t = time(nullptr);
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime(&t));
    return string(buf);
}

static bool send_msg(sock_t s, const string &msg) {
    string f = msg + "\n";
    int total = (int)f.size(), sent = 0;
    const char *p = f.c_str();
    while (sent < total) {
        int r = send(s, p + sent, total - sent, 0);
        if (r <= 0) return false;
        sent += r;
    }
    return true;
}

static bool recv_line(sock_t s, string &out) {
    out.clear();
    char ch;
    for (;;) {
        int r = recv(s, &ch, 1, 0);
        if (r <= 0) return false;
        if (ch == '\r') { recv(s, &ch, 1, 0); return true; }
        if (ch == '\n') return true;
        out += ch;
    }
}

static vector<string> str_split(const string &s, char d) {
    vector<string> v;
    stringstream ss(s);
    string t;
    while (getline(ss, t, d)) v.push_back(t);
    return v;
}

static long long unix_ts() {
    return (long long)chrono::duration_cast<chrono::seconds>(
        chrono::system_clock::now().time_since_epoch()).count();
}

// PeerInfo 
struct PeerInfo {
    string ip;
    int         port;
    string key() const { return ip + ":" + to_string(port); }
};

// PeerNode

class PeerNode {
public:
    PeerNode(const string &ip, int port, const string &cfg)
        : myIp_(ip), myPort_(port), running_(false), msgCount_(0)
    {
        loadSeeds(cfg);
        openLog();
    }

    ~PeerNode() {
        running_ = false;
        if (logFile_.is_open()) logFile_.close();
    }

    void start() {
        serverSock_ = makeServerSocket(myPort_);
        running_ = true;
        log("[PEER " + myIp_ + ":" + to_string(myPort_) + "] Started.");

        if (!registerWithSeeds()) {
            log("[FATAL] Registration failed.");
            return;
        }

        auto known = fetchMergedPeerList();
        selectNeighborsPowerLaw(known);
        connectToNeighbors();

        thread([this]{ acceptLoop();   }).detach();
        thread([this]{ gossipLoop();   }).detach();
        thread([this]{ livenessLoop(); }).detach();

        log("[PEER] All threads started.");
        while (running_.load()) this_thread::sleep_for(chrono::seconds(1));
    }

    void stop() { running_ = false; }

private:
    // Config
    void loadSeeds(const string &f) {
        ifstream fin(f);
        if (!fin) throw runtime_error("Cannot open: " + f);
        string line;
        while (getline(fin, line)) {
            if (line.empty()) continue;
            if (line.back() == '\r') line.pop_back();
            size_t c = line.find(':');
            if (c == string::npos) continue;
            string ip = line.substr(0, c);
            int pt = stoi(line.substr(c + 1));
            allSeeds_.push_back({ip, pt});
        }
        numSeeds_ = (int)allSeeds_.size();
        quorum_   = numSeeds_ / 2 + 1;
        log("[CONFIG] seeds=" + to_string(numSeeds_) +
            " quorum=" + to_string(quorum_));
    }

    void openLog() {
        string fname = "peer_output_" + to_string(myPort_) + ".txt";
        logFile_.open(fname, ios::app);
    }

    void log(const string &msg) {
        string line = "[" + now_str() + "] " + msg;
        cout << line << endl;
        lock_guard<mutex> lk(logMtx_);
        if (logFile_.is_open()) { logFile_ << line << "\n"; logFile_.flush(); }
    }

    // Sockets
    sock_t makeServerSocket(int port) {
        sock_t s = socket(AF_INET, SOCK_STREAM, 0);
        if (s == SOCK_INVALID) throw runtime_error("socket() failed");
        int opt = 1;
        setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons((unsigned short)port);
        if (bind(s, (struct sockaddr*)&addr, sizeof(addr)) == SOCK_ERR)
            throw runtime_error("bind() failed on " + to_string(port));
        listen(s, 64);
        return s;
    }

    sock_t connectTo(const string &ip, int port, int tSec = 3) {
        sock_t s = socket(AF_INET, SOCK_STREAM, 0);
        if (s == SOCK_INVALID) return SOCK_INVALID;
        struct timeval tv; tv.tv_sec = tSec; tv.tv_usec = 0;
        setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port   = htons((unsigned short)port);
        inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);
        if (connect(s, (struct sockaddr*)&addr, sizeof(addr)) == SOCK_ERR) {
            CLOSE_SOCK(s); return SOCK_INVALID;
        }
        return s;
    }

    // Registration
    bool registerWithSeeds() {
        string myKey = myIp_ + ":" + to_string(myPort_);
        vector<PeerInfo> shuffled = allSeeds_;
        mt19937 rng((unsigned)time(nullptr) ^ (unsigned)myPort_);
        shuffle(shuffled.begin(), shuffled.end(), rng);

        int acked = 0;
        for (auto &seed : shuffled) {
            if (acked >= quorum_) break;
            sock_t s = connectTo(seed.ip, seed.port);
            if (s == SOCK_INVALID) { log("[REG] Unreachable: " + seed.key()); continue; }
            send_msg(s, "REGISTER " + myKey);
            string resp;
            if (recv_line(s, resp)) {
                log("[REG] " + seed.key() + " → " + resp);
                if (resp.find("ACK_REG ok") != string::npos ||
                    resp.find("ACK_REG already") != string::npos) {
                    acked++;
                    contactedSeeds_.push_back(seed);
                }
            }
            CLOSE_SOCK(s);
        }
        log("[REG] Registered with " + to_string(acked) +
            "/" + to_string(quorum_) + " required seeds.");
        return acked >= quorum_;
    }

    // Peer list
    vector<PeerInfo> fetchMergedPeerList() {
        string myKey = myIp_ + ":" + to_string(myPort_);
        map<string, PeerInfo> merged;
        for (auto &seed : contactedSeeds_) {
            sock_t s = connectTo(seed.ip, seed.port);
            if (s == SOCK_INVALID) continue;
            send_msg(s, "GET_PEER_LIST");
            string resp;
            if (recv_line(s, resp)) {
                log("[PL] From " + seed.key() + ": " + resp);
                auto parts = str_split(resp, ' ');
                for (size_t j = 1; j < parts.size(); ++j) {
                    if (parts[j] == myKey) continue;
                    size_t c = parts[j].rfind(':');
                    if (c == string::npos) continue;
                    string pip = parts[j].substr(0, c);
                    int pp = stoi(parts[j].substr(c + 1));
                    merged[parts[j]] = {pip, pp};
                }
            }
            CLOSE_SOCK(s);
        }
        vector<PeerInfo> result;
        for (auto &[_, v] : merged) result.push_back(v);
        log("[PL] Merged: " + to_string(result.size()) + " peers.");
        return result;
    }

    // Power-law neighbor selection
    void selectNeighborsPowerLaw(const vector<PeerInfo> &peers) {
        if (peers.empty()) {
            log("[NEIGHBORS] No peers available.");
            return;
        }
        int k = max(1, min((int)peers.size(),
                          (int)log2((double)peers.size() + 1) + 2));

        vector<double> w(peers.size());
        for (size_t i = 0; i < peers.size(); ++i)
            w[i] = 1.0 / pow((double)(i + 1), 1.5);

        mt19937 rng((unsigned)time(nullptr) ^ (unsigned)myPort_);
        set<size_t> chosen;
        while ((int)chosen.size() < k && chosen.size() < peers.size()) {
            double total = 0;
            for (size_t i = 0; i < w.size(); ++i)
                if (!chosen.count(i)) total += w[i];
            uniform_real_distribution<double> dist(0.0, total);
            double r = dist(rng), acc = 0;
            for (size_t i = 0; i < w.size(); ++i) {
                if (chosen.count(i)) continue;
                acc += w[i];
                if (r <= acc) { chosen.insert(i); break; }
            }
        }

        lock_guard<mutex> lk(neighborMtx_);
        for (size_t idx : chosen) {
            neighbor_[peers[idx].key()] = peers[idx];
            log("[NEIGHBORS] Selected: " + peers[idx].key());
        }
    }

    // Connect
    void connectToNeighbors() {
        lock_guard<mutex> lk(neighborMtx_);
        for (auto &[key, info] : neighbor_) {
            sock_t s = connectTo(info.ip, info.port);
            if (s != SOCK_INVALID) {
                neighborSocks_[key] = s;
                send_msg(s, "HELLO " + myIp_ + ":" + to_string(myPort_));
                log("[CONNECT] → " + key);
            } else {
                log("[CONNECT] FAILED → " + key);
            }
        }
    }

    // Accept loop
    void acceptLoop() {
        while (running_.load()) {
            struct sockaddr_in cli; memset(&cli, 0, sizeof(cli));
            socklen_t cliLen = sizeof(cli);
            sock_t conn = accept(serverSock_, (struct sockaddr*)&cli, &cliLen);
            if (conn == SOCK_INVALID) {
                if (running_.load()) continue;
                break;
            }
            sock_t *cp = new sock_t(conn);
            thread([this, cp]() {
                sock_t c = *cp; delete cp;
                handleIncoming(c);
            }).detach();
        }
    }

    void handleIncoming(sock_t conn) {
        string line;
        while (recv_line(conn, line)) {
            if (line.size() > 6 && line.substr(0, 6) == "HELLO ") {
                string senderKey = line.substr(6);
                lock_guard<mutex> lk(neighborMtx_);
                if (!neighborSocks_.count(senderKey)) {
                    neighborSocks_[senderKey] = conn;
                    log("[HELLO] ← " + senderKey);

                    // If they connected to us, they are now a neighbor we should also monitor
                    if (!neighbor_.count(senderKey)) {
                        size_t c = senderKey.rfind(':');
                        if (c != string::npos) {
                            string pip = senderKey.substr(0, c);
                            int pp = stoi(senderKey.substr(c + 1));
                            neighbor_[senderKey] = {pip, pp};
                            log("[NEIGHBORS] Added mutual: " + senderKey);
                        }
                    }
                }
            } else if (line.size() > 7 && line.substr(0, 7) == "GOSSIP ") {
                receiveGossip(line.substr(7), conn);
            } else if (line == "IS_ALIVE?") {
                send_msg(conn, "ALIVE");
            } else if (line.size() > 9 && line.substr(0, 9) == "IS_DEAD? ") {
                handleIsDead(conn, line.substr(9));
            }
        }
        {
            lock_guard<mutex> lk(neighborMtx_);
            for (auto it = neighborSocks_.begin(); it != neighborSocks_.end(); ) {
                if (it->second == conn) it = neighborSocks_.erase(it);
                else ++it;
            }
        }
        CLOSE_SOCK(conn);
    }

    // Gossip
    void gossipLoop() {
        this_thread::sleep_for(chrono::seconds(3));
        while (running_.load() && msgCount_.load() < 10) {
            generateAndBroadcast();
            this_thread::sleep_for(chrono::seconds(5));
        }
        log("[GOSSIP] Generated max 10 messages.");
    }

    void generateAndBroadcast() {
        long long ts = unix_ts();
        int n = ++msgCount_;
        // Format: ts:IP:Port:msgNo
        string msg = to_string(ts) + ":" + myIp_ + ":" + 
                          to_string(myPort_) + ":" + to_string(n);
        log("[GOSSIP] Generated: " + msg);
        addToML(msg);
        broadcastGossip(msg, SOCK_INVALID);
    }

    void receiveGossip(const string &msg, sock_t from) {
        size_t h = hash<string>{}(msg);
        {
            lock_guard<mutex> lk(mlMtx_);
            if (ml_.count(h)) return;
            ml_.insert(h);
        }
        log("[GOSSIP] Received (first): " + msg);
        broadcastGossip(msg, from);
    }

    void broadcastGossip(const string &msg, sock_t except) {
        lock_guard<mutex> lk(neighborMtx_);
        for (auto &[_, s] : neighborSocks_) {
            if (s == except) continue;
            send_msg(s, "GOSSIP " + msg);
        }
    }

    void addToML(const string &msg) {
        lock_guard<mutex> lk(mlMtx_);
        ml_.insert(hash<string>{}(msg));
    }

    // Liveness
    static const int PING_INTERVAL  = 10;
    static const int FAIL_THRESHOLD = 3;

    void livenessLoop() {
        this_thread::sleep_for(chrono::seconds(15));
        while (running_.load()) {
            pingAllNeighbors();
            this_thread::sleep_for(chrono::seconds(PING_INTERVAL));
        }
    }

    void pingAllNeighbors() {
        vector<pair<string, PeerInfo>> targets;
        {
            lock_guard<mutex> lk(neighborMtx_);
            for (auto &[key, info] : neighbor_)
                targets.push_back({key, info});
        }
        for (auto &[key, info] : targets) {
            if (reportedDead_.count(key)) continue;
            bool alive = probePeer(info.ip, info.port);
            if (alive) {
                failCount_[key] = 0;
            } else {
                failCount_[key]++;
                log("[PING] " + key + " fail #" + to_string(failCount_[key]));
                if (failCount_[key] >= FAIL_THRESHOLD) {
                    log("[SUSPECT] " + key + " → consensus.");
                    if (peerConsensus(info)) reportDeadNode(info);
                }
            }
        }
    }

    bool probePeer(const string &ip, int port) {
        sock_t s = connectTo(ip, port, 2);
        if (s == SOCK_INVALID) return false;
        send_msg(s, "IS_ALIVE?");
        string resp;
        bool ok = recv_line(s, resp) && resp == "ALIVE";
        CLOSE_SOCK(s);
        return ok;
    }

    bool peerConsensus(const PeerInfo &dead) {
        vector<pair<string, PeerInfo>> others;
        {
            lock_guard<mutex> lk(neighborMtx_);
            for (auto &[key, info] : neighbor_)
                if (key != dead.key()) others.push_back({key, info});
        }
        int confirmations = 1, total = (int)others.size() + 1;
        for (auto &[_, info] : others) {
            sock_t s = connectTo(info.ip, info.port, 2);
            if (s == SOCK_INVALID) continue;
            send_msg(s, "IS_DEAD? " + dead.key());
            string resp;
            if (recv_line(s, resp) && resp == "CONFIRMED_DEAD") confirmations++;
            CLOSE_SOCK(s);
        }
        int pq = total / 2 + 1;
        log("[CONSENSUS] " + dead.key() + " conf=" + to_string(confirmations) +
            "/" + to_string(total) + " need=" + to_string(pq));
        return confirmations >= pq;
    }

    void handleIsDead(sock_t conn, const string &pKey) {
        size_t c = pKey.rfind(':');
        if (c == string::npos) { send_msg(conn, "UNKNOWN"); return; }
        string ip = pKey.substr(0, c);
        int pt = stoi(pKey.substr(c + 1));
        bool alive = probePeer(ip, pt);
        send_msg(conn, alive ? "NOT_DEAD" : "CONFIRMED_DEAD");
    }

    void reportDeadNode(const PeerInfo &dead) {
        if (reportedDead_.count(dead.key())) return;
        reportedDead_.insert(dead.key());

        long long ts = unix_ts();
        string report = "Dead Node:" + dead.ip + ":" +
                             to_string(dead.port) + ":" +
                             to_string(ts) + ":" + myIp_;
        log("[DEAD] Reporting: " + report);

        for (auto &seed : allSeeds_) {
            sock_t s = connectTo(seed.ip, seed.port, 3);
            if (s == SOCK_INVALID) continue;
            send_msg(s, "DEAD_NODE " + dead.ip + ":" + to_string(dead.port) +
                        ":" + to_string(ts) + ":" + myIp_ + ":" + to_string(myPort_));
            string r; recv_line(s, r);
            CLOSE_SOCK(s);
        }

        {
            lock_guard<mutex> lk(neighborMtx_);
            neighbor_.erase(dead.key());
            auto it = neighborSocks_.find(dead.key());
            if (it != neighborSocks_.end()) {
                CLOSE_SOCK(it->second);
                neighborSocks_.erase(it);
            }
        }
    }

    // Members
    string  myIp_;
    int myPort_;
    sock_t serverSock_ = SOCK_INVALID;
    atomic<bool> running_;
    atomic<int>  msgCount_;

    vector<PeerInfo> allSeeds_;
    vector<PeerInfo> contactedSeeds_;
    int numSeeds_ = 0;
    int quorum_   = 1;

    mutex neighborMtx_;
    map<string, PeerInfo> neighbor_;
    map<string, sock_t> neighborSocks_;

    mutex mlMtx_;
    unordered_set<size_t> ml_;

    map<string, int> failCount_;
    set<string> reportedDead_;

    mutex logMtx_;
    ofstream logFile_;
};

// main
int main(int argc, char *argv[]) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <port> [config_file]\n";
        return 1;
    }
    int port = stoi(argv[1]);
    string cfg = (argc >= 3) ? argv[2] : "config.txt";
    try {
        PeerNode node("127.0.0.1", port, cfg);
        node.start();
    } catch (const exception &e) {
        cerr << "[FATAL] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
