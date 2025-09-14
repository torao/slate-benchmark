package iavl

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/cosmos/iavl"
	"github.com/cosmos/iavl/db"

	"slate_benchmark/common"
)

type IAVLCUT struct {
	Path    string
	LevelDB *db.GoLevelDB
	Tree    *iavl.MutableTree
}

var _ common.CUT = (*IAVLCUT)(nil)

func NewIAVLCUT(path string) IAVLCUT {
	return IAVLCUT{Path: path}
}

func (c *IAVLCUT) Open() {
	if c.Tree != nil {
		return
	}

	leveldb, err := db.NewGoLevelDB("slate", c.Path)
	if err != nil {
		panic(fmt.Errorf("failed to create leveldb: %v", err))
	}
	c.LevelDB = leveldb
	c.Tree = iavl.NewMutableTree(leveldb, 0, false, iavl.NewNopLogger())
	_, err = c.Tree.Load()
	if err != nil {
		panic(fmt.Errorf("failed to load tree: %v", err))
	}
}

func (c *IAVLCUT) Close() {
	if c.Tree != nil {
		c.Tree.Close()
		c.Tree = nil
	}
	if c.LevelDB != nil {
		c.LevelDB.Close()
		c.LevelDB = nil
	}
	if _, err := os.Stat(c.Path); err == nil {
		if err = os.RemoveAll(c.Path); err != nil {
			panic(fmt.Errorf("failed to remove file or directory: %v; %s", err, c.Path))
		}
	}
}

// 指定されたディレクトリに IAVL を作成します。
func (c *IAVLCUT) MeasureAppend(n uint64) (time.Duration, uint64) {
	runtime.GC()
	start := time.Now()
	for uint64(c.Tree.Size()) < n {
		i := uint64(c.Tree.Size() + 1)
		_, err := c.Tree.Set(common.IntToKey(i), common.IntToValue(i))
		if err != nil {
			panic(fmt.Errorf("failed to update iavl database: %v", err))
		}
		c.Tree.Hash()
		_, _, err = c.Tree.SaveVersion()
		if err != nil {
			panic(fmt.Errorf("failed to version iavl database: %v", err))
		}
	}
	duration := time.Since(start)
	return duration, uint64(common.FileOrDirectorySize(c.Path))
}

// 既存のルートハッシュから IAVL をロードし、値を取得
func (c *IAVLCUT) MeasureGets(is []uint64) map[uint64]time.Duration {
	result := make(map[uint64]time.Duration)
	for _, i := range is {
		runtime.GC()
		start := time.Now()
		bytes, err := c.Tree.Get(common.IntToKey(i))
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
