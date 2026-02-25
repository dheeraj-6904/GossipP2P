# Makefile — CSL3080 Assignment 1: Gossip P2P Network (Linux)
# Usage:
#   make          → build seed and peer
#   make seed     → build only seed
#   make peer     → build only peer
#   make clean    → remove binaries and log files

CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2
LDFLAGS  := -pthread

SRC_DIR  := src
BIN_DIR  := .

.PHONY: all seed peer clean

all: seed peer

seed: $(SRC_DIR)/seed.cpp
	$(CXX) $(CXXFLAGS) -o $(BIN_DIR)/seed $< $(LDFLAGS)
	@echo "Built: seed"

peer: $(SRC_DIR)/peer.cpp
	$(CXX) $(CXXFLAGS) -o $(BIN_DIR)/peer $< $(LDFLAGS)
	@echo "Built: peer"

clean:
	rm -f seed peer
	rm -f seed_output_*.txt peer_output_*.txt
	@echo "Cleaned."
