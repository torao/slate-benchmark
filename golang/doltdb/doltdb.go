package doltdb

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"time"

	_ "github.com/dolthub/driver"

	"slate_benchmark/common"
)

type DoltDBCUT struct {
	Path string
	Db   *sql.DB
}

var _ common.CUT = (*DoltDBCUT)(nil)

func NewDoltDBCUT(path string) DoltDBCUT {
	return DoltDBCUT{Path: path}
}

func (c *DoltDBCUT) Open() {
	if c.Db != nil {
		return
	}
	common.CreateDirectory(c.Path)
	dsn := fmt.Sprintf("file://%s?commitname=%s&commitemail=%s&database=%s",
		c.Path,
		url.QueryEscape("TAKAMI Torao"),
		"koiroha@gmail.com",
		"slate",
	)
	db, err := sql.Open("dolt", dsn)
	if err != nil {
		panic(fmt.Errorf("failed to create doltdb: %v", err))
	}
	c.Db = db
}

func (c *DoltDBCUT) Close() {
	if c.Db != nil {
		c.Db.Close()
		c.Db = nil
	}
	if _, err := os.Stat(c.Path); err == nil {
		if err = os.RemoveAll(c.Path); err != nil {
			panic(fmt.Errorf("failed to remove file or directory: %v; %s", err, c.Path))
		}
	}
}

// 指定されたディレクトリに DoltDB を作成します。
func (c *DoltDBCUT) MeasureAppend(n uint64) (time.Duration, uint64) {
	c.Db.Exec(`CREATE DATABASE slate`)
	_, err := c.Db.Exec(`CREATE TABLE IF NOT EXISTS sequence_data(i BIGINT PRIMARY KEY, value BIGINT)`)
	if err != nil {
		panic(fmt.Errorf("failed to create table: %v", err))
	}
	var count int64
	err = c.Db.QueryRow(`SELECT COUNT(*) FROM sequence_data`).Scan(&count)
	if err != nil {
		panic(err)
	}

	runtime.GC()
	start := time.Now()
	for i := uint64(count) + 1; i <= n; i++ {
		value := int64(common.Splitmix64(i))
		_, err = c.Db.Exec(`
		INSERT INTO sequence_data(i, value) VALUES(?, ?);
		CALL DOLT_COMMIT('-a', '-m', 'commit');
		`, i, value)
		if err != nil {
			panic(fmt.Errorf("failed to prepare statement: %v", err))
		}
	}
	duration := time.Since(start)

	return duration, uint64(common.FileOrDirectorySize(c.Path))
}

// 既存のルートハッシュから DoltDB をロードし、値を取得
func (c *DoltDBCUT) MeasureGets(is []uint64) map[uint64]time.Duration {
	result := make(map[uint64]time.Duration)
	for _, i := range is {
		runtime.GC()
		var value int64
		start := time.Now()
		err := c.Db.QueryRow(`SELECT value FROM sequence_data WHERE i=?`, i).Scan(&value)
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
