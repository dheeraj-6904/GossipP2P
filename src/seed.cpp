/*
 * seed.cpp — Seed Node for Gossip-based P2P Network
 * Assignment 1, CSL3080 — Computer Networks
 *
 * Compile: g++ -std=c++17 -pthread -Wall -Wextra -O2 -o seed src/seed.cpp
 * Usage  : ./seed <port> [config_file]
 *
 * The SeedNode class:
 *  - Listens for TCP connections from peers and other seed nodes.
 *  - Maintains a Peer List (PL) of registered peers.
 *  - Uses quorum-based (floor(n/2)+1) consensus across all seed nodes before admitting or removing a peer.
 *  - Logs all events to stdout AND seed_output_<port>.txt.
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
#include <cstring>
#include <ctime>
#include <stdexcept>
#include <algorithm>
#include <mutex>
#include <thread>
#include <atomic>

using namespace std;

// Type aliases
using sock_t = int;
static constexpr sock_t SOCK_INVALID = -1;
static constexpr int    SOCK_ERR     = -1;
#define CLOSE_SOCK(s) ::close(s)

//Helpers
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
        if (ch == '\r') {
            recv(s, &ch, 1, 0);
            return true;
        }
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

//PeerInfo
struct PeerInfo {
    string ip;
    int port;
    string key() const {
        return ip + ":" + to_string(port);
    }
};

// SeedNode
class SeedNode {
public:
    SeedNode(const string &ip, int port, const string &cfgFile)
        : myIp_(ip), myPort_(port), running_(false)
    {
        loadConfig(cfgFile);
        openLog();
    }

    ~SeedNode() {
        running_ = false;
        if (logFile_.is_open()) logFile_.close();
    }

    void start() {
        serverSock_ = makeServerSocket(myPort_);
        running_ = true;
        log("[SEED " + myIp_ + ":" + to_string(myPort_) + "] Listening.");

        while (running_.load()) {
            struct sockaddr_in cli;
            memset(&cli, 0, sizeof(cli));
            socklen_t cliLen = sizeof(cli);
            sock_t conn = accept(serverSock_, (struct sockaddr*)&cli, &cliLen);
            if (conn == SOCK_INVALID) {
                if (running_.load()) log("[WARN] accept() error");
                continue;
            }
            // create detached handler thread
            sock_t *cp = new sock_t(conn);
            thread([this, cp]() {
                sock_t c = *cp; delete cp;
                dispatch(c);
            }).detach();
        }
        CLOSE_SOCK(serverSock_);
    }

private:
    // Config 
    void loadConfig(const string &f) {
        ifstream fin(f);
        if (!fin) throw runtime_error("Cannot open config: " + f);
        string line;
        while (getline(fin, line)) {
            if (line.empty()) continue;
            if (line.back() == '\r') line.pop_back();
            size_t c = line.find(':');
            if (c == string::npos) continue;
            string ip = line.substr(0, c);
            int pt = stoi(line.substr(c + 1));
            if (ip == myIp_ && pt == myPort_) continue; // skip self
            otherSeeds_.push_back({ip, pt});
        }
        numSeeds_ = (int)otherSeeds_.size() + 1;
        quorum_   = numSeeds_ / 2 + 1;
        log("[CONFIG] seeds=" + to_string(numSeeds_) +
            " quorum=" + to_string(quorum_));
    }

    void openLog() {
        string fname = "seed_output_" + to_string(myPort_) + ".txt";
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
            throw runtime_error("bind() failed on port " + to_string(port));
        listen(s, 64);
        return s;
    }

    sock_t connectTo(const string &ip, int port) {
        sock_t s = socket(AF_INET, SOCK_STREAM, 0);
        if (s == SOCK_INVALID) return SOCK_INVALID;
        struct timeval tv; tv.tv_sec = 2; tv.tv_usec = 0;
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

    // Dispatch
    void dispatch(sock_t conn) {
        string line;
        if (!recv_line(conn, line)) { CLOSE_SOCK(conn); return; }

        if (line.size() >  9 && line.substr(0,  9) == "REGISTER " ) handleRegister(conn, line.substr(9));
        else if (line.size() >  9 && line.substr(0,  9) == "VOTE_REG " ) handleVoteReg(conn, line.substr(9));
        else if (line.size() > 11 && line.substr(0, 11) == "COMMIT_REG ") handleCommitReg(conn, line.substr(11));
        else if (line.size() >= 13 && line.substr(0,13) == "GET_PEER_LIST") handleGetPeerList(conn);
        else if (line.size() > 10 && line.substr(0, 10) == "DEAD_NODE " ) handleDeadNode(conn, line.substr(10));
        else if (line.size() > 12 && line.substr(0, 12) == "COMMIT_DEAD ") handleCommitDead(conn, line.substr(12));
        else log("[WARN] Unknown: " + line);

        CLOSE_SOCK(conn);
    }

    // Registration (quorum)
    void handleRegister(sock_t conn, const string &payload) {
        log("[REG] Request: " + payload);
        size_t c = payload.rfind(':');
        if (c == string::npos) {
            send_msg(conn, "ERR bad payload");
            return;
        }
        string pIp  = payload.substr(0, c);
        int pPort = stoi(payload.substr(c + 1));
        string pKey = pIp + ":" + to_string(pPort);

        {
            lock_guard<mutex> lk(plMtx_);
            if (peerList_.count(pKey)) {
                log("[REG] Already registered: " + pKey);
                send_msg(conn, "ACK_REG already_registered");
                return;
            }
        }

        int votes = 1;
        string proposer = myIp_ + ":" + to_string(myPort_);
        for (auto &seed : otherSeeds_) {
            sock_t s = connectTo(seed.ip, seed.port);
            if (s == SOCK_INVALID) { log("[REG] Unreachable: " + seed.key()); continue; }
            send_msg(s, "VOTE_REG " + pKey + ":" + proposer);
            string resp;
            if (recv_line(s, resp) && resp == "VOTE_OK") votes++;
            CLOSE_SOCK(s);
        }

        log("[REG] Votes=" + to_string(votes) + "/" + to_string(numSeeds_) +
            " need=" + to_string(quorum_));

        if (votes >= quorum_) {
            {
                lock_guard<mutex> lk(plMtx_);
                peerList_[pKey] = {pIp, pPort};
            }
            log("[REG] COMMITTED " + pKey);
            for (auto &seed : otherSeeds_) {
                sock_t s = connectTo(seed.ip, seed.port);
                if (s == SOCK_INVALID) continue;
                send_msg(s, "COMMIT_REG " + pKey);
                string r; recv_line(s, r);
                CLOSE_SOCK(s);
            }
            send_msg(conn, "ACK_REG ok");
        } else {
            log("[REG] Quorum NOT reached for " + pKey);
            send_msg(conn, "ACK_REG rejected");
        }
    }

    void handleVoteReg(sock_t conn, const string &payload) {
        log("[VOTE_REG] " + payload);
        send_msg(conn, "VOTE_OK");
    }

    void handleCommitReg(sock_t conn, const string &pKey) {
        size_t c = pKey.rfind(':');
        if (c == string::npos) {
            send_msg(conn, "ERR");
            return;
        }
        string ip = pKey.substr(0, c);
        int pt = stoi(pKey.substr(c + 1));
        {
            lock_guard<mutex> lk(plMtx_);
            peerList_[pKey] = {ip, pt};
        }
        log("[COMMIT_REG] " + pKey);
        send_msg(conn, "ACK_COMMIT");
    }

    // Peer list
    void handleGetPeerList(sock_t conn) {
        lock_guard<mutex> lk(plMtx_);
        string resp = "PEER_LIST";
        for (auto &[key, _] : peerList_) resp += " " + key;
        send_msg(conn, resp);
        log("[PL] Sent " + to_string(peerList_.size()) + " entries.");
    }

    // Dead-node handling
    void handleDeadNode(sock_t conn, const string &payload) {
        log("[DEAD] Report: " + payload);
        auto f = str_split(payload, ':');
        if (f.size() < 3) {
            send_msg(conn, "ACK_DEAD ignored");
            return;
        }
        string deadKey  = f[0] + ":" + f[1];
        string reporter = "unknown";
        if (f.size() >= 5)      reporter = f[3] + ":" + f[4];
        else if (f.size() >= 4) reporter = f[3];
        send_msg(conn, "ACK_DEAD received");

        int cnt = 0;
        {
            lock_guard<mutex> lk(deadMtx_);
            deadReports_[deadKey].insert(reporter);
            cnt = (int)deadReports_[deadKey].size();
        }
        log("[DEAD] Reports for " + deadKey + ": " + to_string(cnt) +
            "/" + to_string(quorum_));

        if (cnt >= quorum_) {
            bool already = false;
            {
                lock_guard<mutex> lk(deadMtx_);
                already = (committedDead_.count(deadKey) > 0);
                if (!already) committedDead_.insert(deadKey);
            }
            if (!already) {
                for (auto &seed : otherSeeds_) {
                    sock_t s = connectTo(seed.ip, seed.port);
                    if (s == SOCK_INVALID) continue;
                    send_msg(s, "COMMIT_DEAD " + deadKey);
                    string r; recv_line(s, r);
                    CLOSE_SOCK(s);
                }
                removePeer(deadKey);
            }
        }
    }

    void handleCommitDead(sock_t conn, const string &dKey) {
        bool already = false;
        {
            lock_guard<mutex> lk(deadMtx_);
            already = (committedDead_.count(dKey) > 0);
            if (!already) committedDead_.insert(dKey);
        }
        if (!already) removePeer(dKey);
        send_msg(conn, "ACK_COMMIT_DEAD");
    }

    void removePeer(const string &pKey) {
        lock_guard<mutex> lk(plMtx_);
        if (peerList_.erase(pKey))
            log("[REMOVE] Dead peer removed: " + pKey +
                " PL_size=" + to_string(peerList_.size()));
    }

    // Members 
    string  myIp_;
    int myPort_;
    sock_t serverSock_ = SOCK_INVALID;
    atomic<bool> running_;

    vector<PeerInfo> otherSeeds_;
    int numSeeds_ = 0;
    int quorum_   = 1;

    mutex plMtx_;
    map<string, PeerInfo> peerList_;

    mutex deadMtx_;
    map<string, set<string>> deadReports_;
    set<string> committedDead_;

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
        SeedNode node("127.0.0.1", port, cfg);
        node.start();
    } catch (const exception &e) {
        cerr << "[FATAL] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
