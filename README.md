# Gossip-based P2P Network — CSL3080 Assignment 1

A C++ implementation of a gossip-based peer-to-peer (P2P) network on Linux featuring:

- **Seed nodes** (`src/seed.cpp`) — bootstrap the network, maintain a Peer List (PL), and perform quorum-based consensus for peer registration and dead-node removal.
- **Peer nodes** (`src/peer.cpp`) — self-register with seeds, select TCP neighbors using power-law preferential attachment, broadcast gossip messages, detect dead nodes via two-level consensus (peer-level → seed-level).

Both nodes use C++17 OOP (`SeedNode` / `PeerNode`) with standard POSIX threads (`std::thread`, `std::mutex`).

---

## Project Structure

```
.
├── src/
│   ├── seed.cpp          # Seed node implementation
│   └── peer.cpp          # Peer node implementation
├── scripts/
│   └── test_network.py   # Automated test script (Python 3)
├── config.txt            # Seed list: one IP:Port per line
├── Makefile              # Build script
├── README.md             # This file
└── .gitignore
```

---

## Compilation

```bash
# Build both seed and peer (binaries placed in project root)
make

# Or individually
make seed
make peer

# Manual build
g++ -std=c++17 -pthread -Wall -Wextra -O2 -o seed src/seed.cpp
g++ -std=c++17 -pthread -Wall -Wextra -O2 -o peer src/peer.cpp
```

---

## Configuration (`config.txt`)

List all seed nodes, one per line:
```
127.0.0.1:5000
127.0.0.1:5001
127.0.0.1:5002
```

---

## Running (Manual)

Open separate terminals for each node:

```bash
# Seed nodes
./seed 5000 config.txt
./seed 5001 config.txt
./seed 5002 config.txt

# Peer nodes
./peer 6000 config.txt
./peer 6001 config.txt
# ... add more as needed
```

Each node writes logs to:
- Seed: `seed_output_<port>.txt`
- Peer: `peer_output_<port>.txt`

---

## Automated Testing

```bash
# Build first
make

# Run tests from project root
python3 scripts/test_network.py

# Or specify binary directory explicitly
python3 scripts/test_network.py --exe-dir .
```

The script:
1. Writes `config.txt` with 3 seed ports (5000–5002).
2. Spawns 3 seed nodes + 5 peer nodes.
3. Waits for gossip propagation (~40 s).
4. Kills one peer and waits for dead-node detection (~50 s).
5. Prints `PASS`/`FAIL` for each test case with a final summary.

---

## Protocol Summary

### Registration (Seed-Level Consensus)
1. Peer sends `REGISTER <IP>:<Port>` to ⌊n/2⌋+1 seeds.
2. Each contacted seed broadcasts `VOTE_REG` to other seeds.
3. On reaching quorum votes → `COMMIT_REG` broadcast; peer added to all PLs.
4. Seed replies `ACK_REG ok` to peer.

### Gossip Dissemination
- Each peer generates `<timestamp>:<IP>:<msgNo>` every 5 s, up to 10 messages.
- On receipt: hash stored in ML; forwarded to all neighbors except sender.
- Duplicates are silently dropped.

### Dead-Node Detection (Two-Level Consensus)
1. Peer misses `FAIL_THRESHOLD` (3) pings → enters suspicion.
2. Asks other neighbors `IS_DEAD? <IP>:<Port>`.
3. On peer-level quorum confirmation → sends `Dead Node:...` to all seeds.
4. Seeds collect reports; on seed-level quorum → remove peer from all PLs.
