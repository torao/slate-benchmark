package main

import (
	"slate_benchmark/common"
	"slate_benchmark/iavl"
	"slate_benchmark/doltdb"
)

func main() {
	config := common.ParseCommandLine([]string{
		"query-iavl-leveldb",
		"append-iavl-leveldb",
		"query-doltdb",
		"append-doltdb",
	}, "Performance Benchmark Tool", `Performance Benchmark Tool

  This tool performs comprehensive performance benchmarking of IAVL+ (Immutable
  AVL+) trees using LevelDB and DoltDB as the persistent storage backend. It
	measures both time and space complexity for append operations and query
	performance across different data sizes.
`)
	common.PrintSystemInfo("Benchmark", "File", config)

	iavl := iavl.NewIAVLCUT(config.DatabasePath("iavl-leveldb"))
	common.BenchmarkAppend(config, "append-iavl-leveldb", "volume-iavl-leveldb", &iavl)
	common.BenchmarkGet(config, "get-iavl-leveldb", &iavl)
	iavl.Close()

	doltdb := doltdb.NewDoltDBCUT(config.DatabasePath("doltdb-file"))
	common.BenchmarkAppend(config, "append-doltdb-file", "volume-doltdb-file", &doltdb)
	common.BenchmarkGet(config, "get-doltdb-file", &doltdb)
	doltdb.Close()
}
