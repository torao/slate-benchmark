# slate-benchmark

This repository contains benchmarks for the reference implementation of [Slate](https://github.com/torao/stratified-hash-tree) (Stratified Hash Tree).

## Overview

Slate is an append-optimized hash tree structure designed for efficient storage and retrieval of time-series data. This benchmark suite evaluates its performance characteristics against other data structures.

## Requirements

- Rust 1.88.0 or later
- Cargo

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark group
cargo bench append
cargo bench random_get
cargo bench recent_get
cargo bench range_scan

# Generate baseline for comparison
cargo bench -- --save-baseline main

# Compare against baseline
cargo bench -- --baseline main
```
