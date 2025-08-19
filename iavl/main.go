package main

import (
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/cosmos/iavl"
	"github.com/cosmos/iavl/db"
)

// ベンチマーク設定
const (
	MaxTrials        = 1000        // 最大試行回数
	MinTrials        = 5           // 最小試行回数
	StdDevThreshold  = 0.05        // 標準偏差/平均値のしきい値 (5%)
	MaxDataSize      = 1024 * 1024 // 最大データサイズ
	AppendDivision   = 10          // Append 測定での分割数
	QueryDivision    = 100         // Query 測定での分割数
	MaxDuration      = 3 * time.Minute
	DefaultResultDir = "." // デフォルトの結果出力ディレクトリ

)

// 指定されたディレクトリに IAVL を作成します。
func measureAppend(path string, n uint64, stats *Stats) int64 {

	// 永続化ストレージ上で新しい IAVL データベースを作成
	leveldb, err := db.NewGoLevelDB("slate", path)
	if err != nil {
		panic(fmt.Errorf("failed to create leveldb: %v", err))
	}
	defer leveldb.Close()
	db := iavl.NewMutableTree(leveldb, 0, false, iavl.NewNopLogger())
	defer db.Close()

	start := time.Now()
	for i := uint64(1); i <= n; i++ {
		_, err := db.Set(intToKey(i), intToValue(i))
		if err != nil {
			panic(fmt.Errorf("failed to update iavl database: %v", err))
		}
		_, _, err = db.SaveVersion()
		if err != nil {
			panic(fmt.Errorf("failed to version iavl database: %v", err))
		}
	}
	duration := time.Since(start)
	stats.Add(n, float64(duration.Nanoseconds())/1000.0/1000.0)

	return fileOrDirectorySize(path)
}

// 既存のルートハッシュから Trie をロードし、値を取得
func measureQuery(path string, is []uint64, stats *Stats) {

	// 保存された場所から IAVL をロード
	leveldb, err := db.NewGoLevelDB("slate", path)
	if err != nil {
		panic(fmt.Errorf("failed to create leveldb: %v", err))
	}
	defer leveldb.Close()
	db := iavl.NewMutableTree(leveldb, 0, false, iavl.NewNopLogger())
	defer db.Close()

	for _, i := range is {
		start := time.Now()
		bytes, err := db.GetVersioned(intToKey(i), int64(i))
		if err != nil {
			panic(err)
		}
		duration := time.Since(start)
		stats.Add(i, float64(duration.Nanoseconds())/1000.0/1000.0)
		value := valueToInt(bytes)
		if value != splitmix64(i) {
			panic(fmt.Errorf("The value read for i=%d is incorrect: %d != %d", i, splitmix64(i), value))
		}
	}
}

// Append 性能のベンチマーク
func benchmarkAppend(config *Config, id string) {
	fmt.Println("=== Append Benchmark ===")
	fmt.Println("DataSize\tMean(ms)\tStdDev(ms)\tCV(%)\t\tTrials")
	fmt.Println("--------\t--------\t----------\t-----\t\t------")

	ns := linspace(1, MaxDataSize, AppendDivision)
	timeComplexity := NewStats()
	spaceComplexity := NewStats()
	for _, n := range ns {
		start := time.Now()
		for i := 0; i < MaxTrials; i++ {
			config.RemoveDatabase("iavl")
			space := measureAppend(config.DatabasePath("iavl"), n, timeComplexity)
			spaceComplexity.AddLarger(n, float64(space))
			if i+1 >= MinTrials {
				mean, stddev, _ := timeComplexity.Calculate(n)
				if 2*stddev/mean <= StdDevThreshold || time.Since(start) >= MaxDuration {
					break
				}
			}
		}
		mean, stddev, size := timeComplexity.Calculate(n)
		fmt.Printf("%d\t\t%.2fms\t\t%.2fms\t\t%.2f\t\t%d\n",
			n, mean, stddev, 2*stddev/mean, size)
	}
	config.RemoveDatabase("iavl")

	timeComplexity.Save(config.ResultFile(fmt.Sprintf("%s_time", id)), "SIZE", "TIME")
	spaceComplexity.Save(config.ResultFile(fmt.Sprintf("%s_space", id)), "SIZE", "BYTES")
}

// Query 性能のベンチマーク
func benchmarkQuery(config *Config, id string) {
	fmt.Println("\n=== Query Benchmark ===")

	// データベースを作成
	fmt.Printf("Creating iavl with %d entries...\n", MaxDataSize)
	config.RemoveDatabase("iavl")
	measureAppend(config.DatabasePath("iavl"), MaxDataSize, NewStats())

	fmt.Println("Position\tMean(μs)\tStdDev(μs)\tCV(%)\t\tTrials")
	fmt.Println("--------\t--------\t----------\t-----\t\t------")

	is := logspace(1, MaxDataSize, QueryDivision)
	rand.Seed(time.Now().UnixNano())
	timeComplexity := NewStats()
	start := time.Now()
	for i := 0; i < MaxTrials; i++ {
		rand.Shuffle(len(is), func(i, j int) {
			is[i], is[j] = is[j], is[i]
		})
		measureQuery(config.DatabasePath("iavl"), is, timeComplexity)
		if i+1 >= MinTrials {
			if timeComplexity.MaxRelative() <= StdDevThreshold || time.Since(start) >= MaxDuration {
				break
			}
		}
		if (i+1)%100 == 0 {
			fmt.Printf("  [%d/%d] n=%d: cv=%.3f\n", i+1, MaxTrials, MaxDataSize, timeComplexity.MaxRelative())
		}
	}
	config.RemoveDatabase("iavl")

	timeComplexity.Save(config.ResultFile(id), "SIZE", "TIME")
}

// 統計情報
type Stats struct {
	trials map[uint64][]float64
}

func NewStats() *Stats {
	return &Stats{
		trials: make(map[uint64][]float64),
	}
}

func (s *Stats) Add(key uint64, value float64) {
	trials, ok := s.trials[key]
	if !ok {
		trials = []float64{value}
	} else {
		trials = append(trials, value)
	}
	s.trials[key] = trials
}

func (s *Stats) AddLarger(key uint64, value float64) {
	trials, ok := s.trials[key]
	if !ok {
		s.trials[key] = []float64{value}
	} else {
		trials = append(trials, value)
		if trials[0] < value {
			s.trials[key] = []float64{value}
		}
	}
}

func (s *Stats) Calculate(key uint64) (float64, float64, int) {
	trials, ok := s.trials[key]
	if !ok || len(trials) == 0 {
		return 0, 0, len(trials)
	}
	sum := 0.0
	for _, v := range trials {
		sum += v
	}
	mean := sum / float64(len(trials))
	sumSquaredDiff := 0.0
	for _, v := range trials {
		diff := v - mean
		sumSquaredDiff += diff * diff
	}
	stddev := 0.0
	if len(trials)-1 >= 1 {
		variance := sumSquaredDiff / float64(len(trials)-1)
		stddev = math.Sqrt(variance)
	}
	return mean, stddev, len(trials)
}

func (s *Stats) MaxRelative() float64 {
	relative := math.NaN()
	for key, _ := range s.trials {
		mean, stddev, _ := s.Calculate(key)
		r := 2.0 * stddev / mean
		if math.IsNaN(relative) || r > relative {
			relative = r
		}
	}
	return relative
}

func (s *Stats) Save(path, column1, column2 string) {
	file, err := os.Create(path)
	if err != nil {
		panic(fmt.Errorf("failed to save statistics: %w", err))
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{column1, column2}); err != nil {
		panic(fmt.Errorf("failed to save header: %w", err))
	}

	keys := make([]uint64, 0, len(s.trials))
	for key := range s.trials {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	for _, key := range keys {
		values := s.trials[key]
		record := make([]string, len(values)+1)
		record[0] = strconv.FormatUint(key, 10)
		for i, value := range values {
			record[i+1] = strconv.FormatFloat(value, 'f', -1, 64)
		}
		if err := writer.Write(record); err != nil {
			panic(fmt.Errorf("failed to save data: %w", err))
		}
	}
}

// コマンドライン引数
type Config struct {
	WorkDir   string
	ResultDir string
	SessionID string
}

func (c *Config) DatabasePath(name string) string {
	return filepath.Join(c.WorkDir, fmt.Sprintf("slate-benchmark-%s.db", name))
}

func (c *Config) RemoveDatabase(name string) {
	path := c.DatabasePath(name)
	os.RemoveAll(path)
}

func (c *Config) ResultFile(id string) string {
	return filepath.Join(c.ResultDir, fmt.Sprintf("%s-%s.csv", c.SessionID, id))
}

// コマンドライン引数の解析
func parseCommandLine() *Config {
	config := &Config{
		WorkDir:   os.TempDir(),
		ResultDir: DefaultResultDir,
		SessionID: time.Now().Format("20060102150405"),
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [dir] --output [output] --session [session] --clear\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Arguments:\n")
		fmt.Fprintf(os.Stderr, "  dir             Directory for data input/output (default: %s)\n", os.TempDir())
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		fmt.Fprintf(os.Stderr, "  --output DIR    Directory to save result CSV files (default: %s)\n", config.ResultDir)
		fmt.Fprintf(os.Stderr, "  --session ID    Session name for result file naming (default: %s)\n", config.SessionID)
		fmt.Fprintf(os.Stderr, "  --clean         Remove all cached files and exit\n")
		fmt.Fprintf(os.Stderr, "  --help          Show this help message\n")
	}

	flag.StringVar(&config.ResultDir, "output", DefaultResultDir, "Directory to save result CSV files")
	flag.StringVar(&config.SessionID, "session", time.Now().Format("20060102150405"), "Session name for result file naming")
	cleanFlag := flag.Bool("clean", false, "Remove all cached files and exit")
	helpFlag := flag.Bool("help", false, "Show help message")

	flag.Parse()

	if *helpFlag {
		flag.Usage()
		os.Exit(0)
	}

	if flag.NArg() > 1 {
		panic(fmt.Errorf("too many %d arguments, expected at most one directory path", flag.NArg()))
	}
	dir := os.TempDir()
	if flag.NArg() = 1 {
		dir = flag.Arg(0)
	}
	config.WorkDir = createDirectory(dir)
	config.ResultDir = createDirectory(config.ResultDir)

	if *cleanFlag {
		config.RemoveDatabase("iavl")
		os.Exit(0)
	}

	return config
}

func createDirectory(path string) string {
	absPath, err := filepath.Abs(path)
	if err != nil {
		panic(fmt.Errorf("Error: Failed to get absolute path for '%s': %v\n", path, err))
	}
	if err := os.MkdirAll(absPath, 0755); err != nil {
		panic(fmt.Errorf("Error: Failed to create working directory '%s': %v\n", absPath, err))
	}
	return absPath
}

func fileOrDirectorySize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		panic(fmt.Errorf("cannot access to path: '%s': %v", path, err))
	}

	if !info.IsDir() {
		return info.Size()
	}

	var totalSize int64
	filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			panic(fmt.Errorf("cannot access to path: '%s': %v", path, err))
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				panic(fmt.Errorf("cannot access to path: '%s': %v", path, err))
			}
			totalSize += info.Size()
		}
		return nil
	})
	return totalSize
}

func splitmix64(x uint64) uint64 {
	z := x
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func intToKey(value uint64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, value)
	return key
}

func intToValue(value uint64) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, splitmix64(value))
	return data
}

func valueToInt(bytes []byte) uint64 {
	if len(bytes) != 8 {
		panic(fmt.Errorf("invalid value byte size: %d", len(bytes)))
	}
	return binary.LittleEndian.Uint64(bytes)
}

func linspace(min, max uint64, n int) []uint64 {
	if n <= 1 {
		panic("n must be greater than 1")
	}
	result := make([]uint64, n)
	step := float64(max-min) / float64(n-1)
	for i := 0; i < n; i++ {
		val := float64(min) + step*float64(i)
		result[i] = uint64(math.Round(val))
	}
	return result
}

// logspace は、minからmaxまでをn個の対数的に分割されたu64に分割する
func logspace(min, max uint64, n int) []uint64 {
	if min == 0 {
		panic("min must be positive for logspace")
	}
	if n <= 1 {
		panic("n must be greater than 1")
	}
	result := make([]uint64, n)
	logMin := math.Log(float64(min))
	logMax := math.Log(float64(max))
	step := (logMax - logMin) / float64(n-1)
	for i := 0; i < n; i++ {
		val := math.Exp(logMin + step*float64(i))
		result[i] = uint64(math.Round(val))
	}
	return result
}

// システム情報の表示
func printSystemInfo(config *Config) {
	fmt.Println("=== Ethereum MPT Benchmark (File-based) ===")
	fmt.Printf("Database type: LevelDB (file-based)\n")
	fmt.Printf("Working directory: %s\n", config.WorkDir)
	fmt.Printf("Result directory: %s\n", config.ResultDir)
	fmt.Printf("Session ID: %s\n", config.SessionID)
	fmt.Printf("Max data size: %d\n", MaxDataSize)
	fmt.Printf("Max trials: %d\n", MaxTrials)
	fmt.Printf("Min trials: %d\n", MinTrials)
	fmt.Printf("StdDev threshold: %.1f%%\n", StdDevThreshold*100)
	fmt.Printf("Data type: 8-byte integers\n")
	fmt.Printf("Append test divisions: %d\n", AppendDivision)
	fmt.Printf("Query test divisions: %d\n", QueryDivision)
	fmt.Println()
}

func main() {
	config := parseCommandLine()
	printSystemInfo(config)
	benchmarkQuery(config, "query-iavl-leveldb")
	benchmarkAppend(config, "append-iavl-leveldb")
}
