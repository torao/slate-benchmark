package main

import (
	"fmt"
	"runtime"
	"time"

	"github.com/cosmos/iavl"
	"github.com/cosmos/iavl/db"

	"mt_bench/common"
)

// 指定されたディレクトリに IAVL を作成します。
func measureAppend(path string, n uint64) (time.Duration, uint64) {

	// 永続化ストレージ上で新しい IAVL データベースを作成 (既に存在する場合はロード)
	leveldb, err := db.NewGoLevelDB("slate", path)
	if err != nil {
		panic(fmt.Errorf("failed to create leveldb: %v", err))
	}
	defer leveldb.Close()
	db := iavl.NewMutableTree(leveldb, 0, false, iavl.NewNopLogger())
	defer db.Close()
	_, err = db.Load()
	if err != nil {
		panic(fmt.Errorf("failed to load tree:", err))
	}

	runtime.GC()
	start := time.Now()
	for uint64(db.Size()) < n {
		i := uint64(db.Size() + 1)
		_, err := db.Set(common.IntToKey(i), common.IntToValue(i))
		if err != nil {
			panic(fmt.Errorf("failed to update iavl database: %v", err))
		}
		db.Hash()
		_, _, err = db.SaveVersion()
		if err != nil {
			panic(fmt.Errorf("failed to version iavl database: %v", err))
		}
	}
	duration := time.Since(start)

	db.Close()
	leveldb.Close()
	return duration, uint64(common.FileOrDirectorySize(path))
}

// 既存のルートハッシュから IAVL をロードし、値を取得
func measureQuery(path string, is []uint64) map[uint64]time.Duration {

	// 保存された場所から IAVL をロード
	leveldb, err := db.NewGoLevelDB("slate", path)
	if err != nil {
		panic(fmt.Errorf("failed to create leveldb: %v", err))
	}
	defer leveldb.Close()
	db := iavl.NewMutableTree(leveldb, 0, false, iavl.NewNopLogger())
	defer db.Close()
	_, err = db.Load()
	if err != nil {
		panic(fmt.Errorf("failed to load tree:", err))
	}

	result := make(map[uint64]time.Duration)
	for _, i := range is {
		runtime.GC()
		start := time.Now()
		bytes, err := db.GetVersioned(common.IntToKey(i), int64(i))
		if err != nil {
			panic(err)
		}
		duration := time.Since(start)
		result[i] = duration
		value := common.ValueToInt(bytes)
		if value != common.Splitmix64(i) {
			panic(fmt.Errorf("The value read for i=%d is incorrect: %d != %d", i, common.Splitmix64(i), value))
		}
	}
	return result
}

func main() {
	config := common.ParseCommandLine([]string{
		"query-iavl-leveldb",
		"append-iavl-leveldb",
	}, "IAVL+ Performance Benchmark Tool", `IAVL+ Performance Benchmark Tool

  This tool performs comprehensive performance benchmarking of IAVL+ (Immutable
  AVL+) trees using LevelDB as the persistent storage backend. It measures both
  time and space complexity for append operations and query performance across
  different data sizes.
`)
	common.PrintSystemInfo("Cosmos IAVL+ Benchmark (LevelDB-based)", "File (Leveldb)", config)
	common.BenchmarkGet(config, "get-iavl-leveldb", measureAppend, measureQuery)
	common.BenchmarkAppend(config, "append-iavl-leveldb", "volume-iavl-leveldb", measureAppend)
}
