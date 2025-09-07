package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"runtime"
	"time"

	_ "github.com/dolthub/driver"

	"mt_bench/common"
)

// 指定されたディレクトリに DoltDB を作成します。
func measureAppend(path string, n uint64) (time.Duration, uint64) {

	// 永続化ストレージ上で新しい DoltDB データベースを作成 (既に存在する場合は続きから)
	common.CreateDirectory(path)
	dsn := fmt.Sprintf("file://%s?commitname=%s&commitemail=%s&database=%s",
		path,
		url.QueryEscape("TAKAMI Torao"),
		"koiroha@gmail.com",
		"slate",
	)
	db, err := sql.Open("dolt", dsn)
	if err != nil {
		panic(fmt.Errorf("failed to create doltdb: %v", err))
	}
	defer db.Close()

	db.Exec(`CREATE DATABASE slate`)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS sequence_data(i BIGINT PRIMARY KEY, value BIGINT)`)
	if err != nil {
		panic(fmt.Errorf("failed to create table: %v", err))
	}
	var count int64
	err = db.QueryRow(`SELECT COUNT(*) FROM sequence_data`).Scan(&count)
	if err != nil {
		panic(err)
	}

	runtime.GC()
	start := time.Now()
	for i := uint64(count) + 1; i <= n; i++ {
		value := int64(common.Splitmix64(i))
		_, err = db.Exec(`
		INSERT INTO sequence_data(i, value) VALUES(?, ?);
		CALL DOLT_COMMIT('-a', '-m', 'commit');
		`, i, value)
		if err != nil {
			panic(fmt.Errorf("failed to prepare statement: %v", err))
		}
	}
	duration := time.Since(start)

	return duration, uint64(common.FileOrDirectorySize(path))
}

// 既存のルートハッシュから DoltDB をロードし、値を取得
func measureQuery(path string, is []uint64) map[uint64]time.Duration {

	// 保存された場所から DoltDB をロード
	dsn := fmt.Sprintf("file://%s?commitname=%s&commitemail=%s&database=%s",
		path,
		url.QueryEscape("TAKAMI Torao"),
		"koiroha@gmail.com",
		"slate",
	)
	db, err := sql.Open("dolt", dsn)
	if err != nil {
		panic(fmt.Errorf("failed to create doltdb: %v", err))
	}
	defer db.Close()

	result := make(map[uint64]time.Duration)
	for _, i := range is {
		runtime.GC()
		var value int64
		start := time.Now()
		err = db.QueryRow(`SELECT value FROM sequence_data WHERE i=?`, i).Scan(&value)
		if err != nil {
			panic(err)
		}
		duration := time.Since(start)
		result[i] = duration
		if value != int64(common.Splitmix64(i)) {
			panic(fmt.Errorf("The value read for i=%d is incorrect: %d != %d", i, common.Splitmix64(i), value))
		}
	}
	return result
}

func main() {
	config := common.ParseCommandLine([]string{
		"query-doltdb",
		"append-doltdb",
	}, "DoltDB Performance Benchmark Tool", `DoltDB Performance Benchmark Tool

	This tool performs comprehensive performance benchmarks on DoltDB with a ProllyTree structure.
	It measures the temporal and spatial complexity of additional operations and query performance
	across different data sizes.
`)
	common.PrintSystemInfo("DoltDB Benchmark", "File (DoltDB)", config)
	common.BenchmarkGet(config, "get-doltdb-file", measureAppend, measureQuery)
	common.BenchmarkAppend(config, "append-doltdb-file", "volume-doltdb-file", measureAppend)
}
