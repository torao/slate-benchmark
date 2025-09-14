# slate-benchmark

This repository contains benchmarks for the reference implementation of [Slate](https://github.com/torao/stratified-hash-tree) (Stratified Hash Tree).

## Overview

Slate is an append-optimized Hash Tree (Merkle Tree) structure designed for efficient storage and retrieval of time-series data or distributed transaction log. This benchmark suite evaluates its performance characteristics against other data structures.

## Requirements

- Ubuntu 24.02

## Running Benchmarks

All necessary set-up is done by the `setup` task of `mise`. You can change variables in [mise.toml] to change the data
size and the directory (storage device) used in benchmark.

```bash
sudo apt update
sudo apt upgrade -y
sudo apt install -y git gpg sudo wget curl
sudo timedatectl set-timezone Asia/Tokyo

lsblk -f
sudo mkfs.ext4 /dev/nvme1n1
sudo mkdir /mnt/bench
sudo mount /dev/nvme1n1 /mnt/bench
sudo chown ubuntu:ubuntu /mnt/bench
mkdir /mnt/bench/slate
df -h

# setup mise
sudo install -dm 755 /etc/apt/keyrings
wget -qO - https://mise.jdx.dev/gpg-key.pub | gpg --dearmor | sudo tee /etc/apt/keyrings/mise-archive-keyring.gpg 1> /dev/null
echo "deb [signed-by=/etc/apt/keyrings/mise-archive-keyring.gpg arch=amd64] https://mise.jdx.dev/deb stable main" | sudo tee /etc/apt/sources.list.d/mise.list
sudo apt update
sudo apt install -y mise
mise --version

git clone https://github.com/torao/slate-benchmark.git
cd slate-benchmark

mise run setup
mise run build
mise run bench
```

The results are stored in the `results/` directory in CSV format. These results can then be used to create a graph using
`. /make-plots.sh` to create a graph.

```bash
./make-plots.sh
```
