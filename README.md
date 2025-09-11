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
mise run setup
mise run bench
```

The results are stored in the `results/` directory in CSV format. These results can then be used to create a graph using
`. /make-plots.sh` to create a graph.

```bash
./make-plots.sh
```
