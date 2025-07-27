# slate-benchmark

This repository contains benchmarks for the reference implementation of [Slate](https://github.com/torao/stratified-hash-tree) (Stratified Hash Tree).

## Overview

Slate is an append-optimized Hash Tree (Merkle Tree) structure designed for efficient storage and retrieval of time-series data. This benchmark suite evaluates its performance characteristics against other data structures.

## Requirements

- Rust 1.88.0 or later
- Cargo

## Running Benchmarks

```bash
# perform benchmarking by specifying the directory to be used for I/O
cargo run --release -- /tmp
```

I've confirmed that this works on Windows and Linux (probably also on mac OS).
CMake and C-compiler are required to build RocksDB with `cargo build`.

```bash
sudo apt install llvm-dev libclang-dev clang
```
