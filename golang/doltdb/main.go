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

	// 永続化ストレージ上で新しい DoltDB データベースを作成 (既に存在する場合はロード)
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

	db.Exec(`DROP TABLE IF EXISTS sequence_data`)
	_, err := db.Exec(`CREATE TABLE sequence_data(i BIGINT UNSIGNED PRIMARY KEY, value BIGINT UNSIGNED)`)
	if err != nil {
		panic(fmt.Errorf("failed to create table: %v", err))
	}

	runtime.GC()
	start := time.Now()
	for i := 1; i <= n; i++ {
		tx, err := db.Begin()
		if err != nil {
			panic(fmt.Errorf("failed to begin transactio: %v", err))
		}
		stmt, err := tx.Prepare(`INSERT INTO sequence_data(i, value) VALUES(?, ?)`)
		if err != nil {
			panic(fmt.Errorf("failed to prepare statement: %v", err))
		}
		_, err = stmt.Exec(i, common.Splitmix64(i))
		if err != nil {
			panic(fmt.Errorf("failed to execute statement: %v", err))
		}
		err = tx.Commit()
		if err != nil {
			panic(fmt.Errorf("failed to commit transaction: %v", err))
		}
		_, err = db.Exec(`CALL DOLT_ADD('.'); CALL DOLT_COMMIT('-m', 'your message');`)
		if err != nil {
			panic(fmt.Errorf("failed to version iavl database: %v", err))
		}
	}
	duration := time.Since(start)

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
		"query-doltdb",
		"append-doltdb",
	}, "DoltDB Performance Benchmark Tool", `DoltDB Performance Benchmark Tool

	This tool performs comprehensive performance benchmarks on DoltDB with a ProllyTree structure.
	It measures the temporal and spatial complexity of additional operations and query performance
	across different data sizes.
`)
	common.PrintSystemInfo("DoltDB Benchmark", "File (DoltDB)", config)
	common.BenchmarkQuery(config, "query-doltdb", measureAppend, measureQuery)
	common.BenchmarkAppend(config, "append-doltdb", "volume-doltdb", measureAppend)
}
