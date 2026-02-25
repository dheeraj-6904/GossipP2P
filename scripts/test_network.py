#!/usr/bin/env python3
"""
test_network.py — Automated Test Script for CSL3080 Assignment 1
Gossip-based P2P Network

Tests:
  1. Seed nodes start successfully
  2. Peers register with seeds (consensus)
  3. Gossip propagates to all peers
  4. Dead-node detection works (kill a peer, others report it)

Usage: python scripts/test_network.py [--exe-dir <dir>]

Requirements:
  - Compiled 'seed' and 'peer' binaries in <exe-dir> (default: project root).
  - Python 3.7+
"""

import subprocess
import sys
import os
import time
import signal
import argparse
import socket
import re
from pathlib import Path

# ─── Configuration ──────────────────────────────────────────────────────────────

SEED_PORTS      = [5000, 5001, 5002]
PEER_PORTS      = [6000, 6001, 6002, 6003, 6004]
HOST            = "127.0.0.1"
STARTUP_WAIT    = 3   # seconds to wait after starting each process
GOSSIP_WAIT     = 40  # seconds to let gossip propagate
LIVENESS_WAIT   = 70  # seconds for liveness detection round-trip
CONNECT_TIMEOUT = 2   # socket connect timeout in seconds

# ─── Colour helpers ─────────────────────────────────────────────────────────────

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"

def ok(msg):   print(f"  {GREEN}✓ PASS{RESET}  {msg}")
def fail(msg): print(f"  {RED}✗ FAIL{RESET}  {msg}")
def info(msg): print(f"  {YELLOW}ℹ INFO{RESET}  {msg}")

# ─── Process management ─────────────────────────────────────────────────────────

procs = []

def spawn(binary, args, log_file):
    """Spawn a subprocess, redirect stdout+stderr to log_file."""
    with open(log_file, "w") as f:
        p = subprocess.Popen(
            [binary] + [str(a) for a in args],
            stdout=f, stderr=f,
        )
    procs.append(p)
    return p

def kill_all():
    for p in procs:
        try:
            p.terminate()
        except Exception:
            pass

def kill_proc(p):
    try:
        p.terminate()
    except Exception:
        pass

# ─── Port availability check ────────────────────────────────────────────────────

def wait_for_port(port, timeout=10):
    """Return True when something starts listening on port."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            try:
                s.connect((HOST, port))
                return True
            except (ConnectionRefusedError, OSError):
                pass
        time.sleep(0.5)
    return False

# ─── Log helpers ────────────────────────────────────────────────────────────────

def read_log(path):
    try:
        with open(path, encoding='utf-8', errors='replace') as f:
            return f.read()
    except FileNotFoundError:
        return ""

def logs_contain(path, pattern):
    return bool(re.search(pattern, read_log(path)))

# ─── Test helpers ────────────────────────────────────────────────────────────────

results = []

def assert_test(name, condition, detail=""):
    if condition:
        ok(name)
        results.append((name, True))
    else:
        fail(name + (f" — {detail}" if detail else ""))
        results.append((name, False))

# ─── Main test routine ──────────────────────────────────────────────────────────

def main(exe_dir):
    os.chdir(exe_dir)

    print("\n" + "="*60)
    print("  CSL3080 Assignment 1 — Automated Network Test")
    print("="*60 + "\n")

    # ── 0. Pre-flight: check binaries ─────────────────────────────────────────
    seed_path = Path(exe_dir) / "seed"
    peer_path = Path(exe_dir) / "peer"

    if not seed_path.exists():
        print(f"{RED}[ERROR]{RESET} seed binary not found at {seed_path}. Run 'make' first.")
        sys.exit(1)
    if not peer_path.exists():
        print(f"{RED}[ERROR]{RESET} peer binary not found at {peer_path}. Run 'make' first.")
        sys.exit(1)

    # ── 1. Write config.txt ───────────────────────────────────────────────────
    print("[STEP 1] Writing config.txt ...")
    with open("config.txt", "w") as f:
        for port in SEED_PORTS:
            f.write(f"{HOST}:{port}\n")
    info(f"config.txt written with {len(SEED_PORTS)} seeds.")

    # ── 2. Start seed nodes ───────────────────────────────────────────────────
    print("\n[STEP 2] Starting seed nodes ...")
    seed_procs = []
    for port in SEED_PORTS:
        log = f"seed_output_{port}.txt"
        p = spawn(str(seed_path), [port, "config.txt"], log)
        seed_procs.append(p)
        time.sleep(STARTUP_WAIT)

    for port in SEED_PORTS:
        up = wait_for_port(port, timeout=8)
        assert_test(f"Seed {port} is listening", up)

    # ── 3. Start peer nodes ───────────────────────────────────────────────────
    print("\n[STEP 3] Starting peer nodes ...")
    peer_procs = []
    for port in PEER_PORTS:
        log = f"peer_output_{port}.txt"
        p = spawn(str(peer_path), [port, "config.txt"], log)
        peer_procs.append(p)
        time.sleep(STARTUP_WAIT)

    # ── 4. Check peer registration via seed logs ──────────────────────────────
    print(f"\n[STEP 4] Verifying peer registration (waiting {STARTUP_WAIT}s extra) ...")
    time.sleep(STARTUP_WAIT)

    for peer_port in PEER_PORTS:
        registered_in_any_seed = False
        for seed_port in SEED_PORTS:
            if logs_contain(f"seed_output_{seed_port}.txt",
                            rf"COMMITTED.*{HOST}:{peer_port}"):
                registered_in_any_seed = True
                break
        assert_test(
            f"Peer {peer_port} committed in at least one seed",
            registered_in_any_seed,
            "Check seed_output_*.txt"
        )

    for peer_port in PEER_PORTS:
        acked = logs_contain(f"peer_output_{peer_port}.txt", r"Registered with")
        assert_test(f"Peer {peer_port} got registration ACK", acked)

    # ── 5. Wait for gossip propagation ────────────────────────────────────────
    print(f"\n[STEP 5] Waiting {GOSSIP_WAIT}s for gossip to propagate ...")
    time.sleep(GOSSIP_WAIT)

    gossip_senders = set()
    for peer_port in PEER_PORTS:
        matches = re.findall(r"GOSSIP.*?(\d+\.\d+\.\d+\.\d+):(\d+)",
                             read_log(f"peer_output_{peer_port}.txt"))
        for ip, msg_no in matches:
            gossip_senders.add(f"{ip}:{msg_no[:4]}")

    assert_test(
        "Gossip received by multiple peers",
        len(gossip_senders) > 0,
        f"Distinct gossip entries seen: {len(gossip_senders)}"
    )

    for peer_port in PEER_PORTS:
        generated = logs_contain(f"peer_output_{peer_port}.txt", r"Generated:")
        assert_test(f"Peer {peer_port} generated gossip messages", generated)

    cross_peer_gossip = False
    for peer_port in PEER_PORTS:
        content = read_log(f"peer_output_{peer_port}.txt")
        matches = re.findall(r"Received \(first\):.*?:(\d{4,5}):", content)
        for m_port in matches:
            if int(m_port) != peer_port:
                cross_peer_gossip = True
                break
        if cross_peer_gossip:
            break

    assert_test("Gossip propagated across at least 2 peers", cross_peer_gossip)

    # ── 6. Dead-node detection ────────────────────────────────────────────────
    print(f"\n[STEP 6] Killing peer {PEER_PORTS[-1]} to test dead-node detection ...")
    target_peer = peer_procs[-1]
    target_port = PEER_PORTS[-1]
    kill_proc(target_peer)
    info(f"Peer {target_port} killed. Waiting {LIVENESS_WAIT}s for detection ...")
    time.sleep(LIVENESS_WAIT)

    dead_reported_by_peer = False
    for peer_port in PEER_PORTS[:-1]:
        if logs_contain(f"peer_output_{peer_port}.txt",
                        rf"Dead.*{HOST}:{target_port}|DEAD.*{target_port}|Reporting.*{target_port}"):
            dead_reported_by_peer = True
            break

    assert_test(
        f"Peer {target_port} reported as dead by neighbors",
        dead_reported_by_peer,
        "Check peer_output_*.txt for 'Reporting' or 'DEAD' lines"
    )

    dead_removed_from_seed = False
    for seed_port in SEED_PORTS:
        if logs_contain(f"seed_output_{seed_port}.txt",
                        rf"Removed.*{HOST}:{target_port}|REMOVE.*{target_port}"):
            dead_removed_from_seed = True
            break

    assert_test(
        f"Seed removed dead peer {target_port} from PL",
        dead_removed_from_seed,
        "Check seed_output_*.txt for 'Removed' lines"
    )

    # ── 7. Shutdown & summary ─────────────────────────────────────────────────
    print("\n[STEP 7] Shutting down all nodes ...")
    kill_all()

    print("\n" + "="*60)
    print("  TEST SUMMARY")
    print("="*60)
    passed = sum(1 for _, r in results if r)
    total  = len(results)
    for name, r in results:
        status = f"{GREEN}PASS{RESET}" if r else f"{RED}FAIL{RESET}"
        print(f"  [{status}] {name}")
    print("-"*60)
    print(f"  Result: {passed}/{total} tests passed")
    if passed == total:
        print(f"\n  {GREEN}ALL TESTS PASSED ✓{RESET}\n")
        return 0
    else:
        print(f"\n  {RED}{total - passed} TEST(S) FAILED ✗{RESET}\n")
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="P2P Network Automated Test")
    parser.add_argument("--exe-dir", default=".",
                        help="Directory containing compiled seed/peer binaries (default: .)")
    args = parser.parse_args()

    exe_dir = os.path.abspath(args.exe_dir)
    try:
        sys.exit(main(exe_dir))
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Killing all nodes ...")
        kill_all()
        sys.exit(1)
