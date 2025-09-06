package common

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io/fs"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

// ベンチマーク設定
const (
	MaxTrials        = 1000 // 最大試行回数
	MinTrials        = 5    // 最小試行回数
	CVThreshold      = 0.05 // 標準偏差/平均値のしきい値 (5%)
	AppendDivision   = 10   // Append 測定での分割数
	QueryDivision    = 100  // Query 測定での分割数
	MaxDuration      = 10 * time.Minute
	DefaultResultDir = "." // デフォルトの結果出力ディレクトリ
)

// コマンドライン引数
type Config struct {
	DataSize  uint64
	WorkDir   string
	ResultDir string
	SessionID string
}

// Append 性能のベンチマーク
func BenchmarkAppend(
	config *Config,
	append_id, volume_id string,
	measureAppend func(string, uint64) (time.Duration, uint64),
) {
	fmt.Println("=== Append Benchmark ===")
	fmt.Println("DataSize\tMean[ms]\tStdDev[ms]\tCV[%]\t\tTrials")
	fmt.Println("--------\t--------\t----------\t-----\t\t------")

	ns := Linspace(1, config.DataSize, AppendDivision)
	timeComplexity := NewStats()
	spaceComplexity := NewStats()
	start := time.Now()
	for i := 0; i < MaxTrials; i++ {
		if i > MinTrials {
			ns = FilterCvSufficient(ns, timeComplexity)
		}
		if len(ns) == 0 || time.Since(start) >= MaxDuration {
			break
		}

		config.RemoveDatabase(append_id)
		for _, n := range ns {
			tm, space := measureAppend(config.DatabasePath(append_id), n)
			timeComplexity.Add(n, float64(tm.Nanoseconds())/1000.0/1000.0)
			if i == 0 {
				spaceComplexity.Add(n, float64(space))
			}
		}
		config.RemoveDatabase(append_id)
		if i%100 == 99 {
			mean, stddev, size := timeComplexity.Calculate(config.DataSize)
			fmt.Printf("%d\t\t%.2fms\t\t%.2fms\t\t%.3f\t\t%d\n",
				config.DataSize, mean, stddev, stddev/mean, size)
		}
	}

	timeComplexity.Save(config.ResultFile(append_id), "SIZE", "MILLISECONDS")
	spaceComplexity.Save(config.ResultFile(volume_id), "SIZE", "BYTES")
}

// Query 性能のベンチマーク
func BenchmarkQuery(
	config *Config,
	query_id string,
	measureAppend func(string, uint64) (time.Duration, uint64),
	measureQuery func(string, []uint64) map[uint64]time.Duration,
) {
	fmt.Println("\n=== Query Benchmark ===")

	// データベースを作成
	fmt.Printf("Creating database with %d entries...\n", config.DataSize)
	config.RemoveDatabase(query_id)
	t0 := time.Now()
	measureAppend(config.DatabasePath(query_id), config.DataSize)
	tm := time.Since(t0)
	fmt.Printf("created: %.3f [msec]\n", float64(tm.Nanoseconds())/1000.0/1000.0)

	fmt.Println("DataSize\tCV[%]\t\tTrials")
	fmt.Println("--------\t--------\t----------")

	is := Logspace(1, config.DataSize, QueryDivision)
	rand.Seed(time.Now().UnixNano())
	timeComplexity := NewStats()
	start := time.Now()
	for i := 0; i < MaxTrials; i++ {
		rand.Shuffle(len(is), func(i, j int) {
			is[i], is[j] = is[j], is[i]
		})
		result := measureQuery(config.DatabasePath(query_id), is)
		for j, duration := range result {
			timeComplexity.Add(j, float64(duration.Nanoseconds())/1000.0/1000.0)
		}
		if i+1 >= MinTrials {
			if timeComplexity.MaxRelative() <= CVThreshold || time.Since(start) >= MaxDuration {
				break
			}
		}
		if (i+1)%100 == 0 {
			fmt.Printf("%d\t\t%.3f\t\t%d/%d\n", config.DataSize, timeComplexity.MaxRelative(), i+1, MaxTrials)
		}
	}
	config.RemoveDatabase(query_id)

	timeComplexity.Save(config.ResultFile(query_id), "SIZE", "TIME")
}

// コマンドライン引数の解析
func ParseCommandLine(names []string, short, long string) *Config {
	config := &Config{
		DataSize:  256,
		WorkDir:   "",
		ResultDir: "",
		SessionID: "",
	}

	rootCmd := &cobra.Command{
		Use:   fmt.Sprintf("%s [data-size]", os.Args[0]),
		Short: short,
		Long:  long,
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 1 {
				num, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					fmt.Printf("変換エラー: %v\n", err)
					return
				}
				config.DataSize = num
			}
		},
	}
	flags := rootCmd.Flags()
	workDirFlag := flags.StringP("dir", "d", os.TempDir(), "Database directory used for benchmarking")
	resultDirFlag := flags.StringP("output", "o", DefaultResultDir, "Directory to save result CSV files")
	sessionIdFlag := flags.StringP("session", "s", time.Now().Format("20060102150405"), "Session name for result file naming")
	cleanFlag := flags.BoolP("clean", "c", false, "Remove all cached files and exit")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	config.WorkDir = CreateDirectory(*workDirFlag)
	config.ResultDir = CreateDirectory(*resultDirFlag)
	config.SessionID = *sessionIdFlag

	if *cleanFlag {
		for _, name := range names {
			config.RemoveDatabase(name)
			fmt.Fprintf(os.Stderr, "The databse is deleted: %s\n", config.DatabasePath(name))
		}
		os.Exit(0)
	}

	return config
}

// システム情報の表示
func PrintSystemInfo(title, dbType string, config *Config) {
	fmt.Printf("=== %s ===\n", title)
	fmt.Printf("Database type: %s\n", dbType)
	fmt.Printf("Working directory: %s\n", config.WorkDir)
	fmt.Printf("Result directory: %s\n", config.ResultDir)
	fmt.Printf("Session ID: %s\n", config.SessionID)
	fmt.Printf("Max data size: %d\n", config.DataSize)
	fmt.Printf("Max trials: %d\n", MaxTrials)
	fmt.Printf("Min trials: %d\n", MinTrials)
	fmt.Printf("StdDev threshold: %.1f%%\n", CVThreshold*100)
	fmt.Printf("Data type: 8-byte integers\n")
	fmt.Printf("Append test divisions: %d\n", AppendDivision)
	fmt.Printf("Query test divisions: %d\n", QueryDivision)
	fmt.Println()
}

func FilterCvSufficient(gauge []uint64, s *Stats) []uint64 {
	var result []uint64
	for _, i := range gauge {
		if !s.IsCVSufficient(i, CVThreshold) {
			result = append(result, i)
		}
	}
	return result
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

func (s *Stats) IsCVSufficient(x uint64, cv float64) bool {
	mean, stddev, count := s.Calculate(x)
	if count <= 2 {
		return false
	}
	return stddev/mean < cv
}

func (s *Stats) MaxRelative() float64 {
	relative := math.NaN()
	for x, _ := range s.trials {
		mean, stddev, _ := s.Calculate(x)
		r := stddev / mean
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

func (c *Config) DatabasePath(name string) string {
	return filepath.Join(c.WorkDir, fmt.Sprintf("slate_benchmark-%s.db", name))
}

func (c *Config) RemoveDatabase(name string) {
	path := c.DatabasePath(name)
	os.RemoveAll(path)
}

func (c *Config) ResultFile(id string) string {
	return filepath.Join(c.ResultDir, fmt.Sprintf("%s-%s.csv", c.SessionID, id))
}

func CreateDirectory(path string) string {
	absPath, err := filepath.Abs(path)
	if err != nil {
		panic(fmt.Errorf("Error: Failed to get absolute path for '%s': %v\n", path, err))
	}
	if err := os.MkdirAll(absPath, 0755); err != nil {
		panic(fmt.Errorf("Error: Failed to create working directory '%s': %v\n", absPath, err))
	}
	return absPath
}

func FileOrDirectorySize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot access to path: '%s': %v\n", path, err)
		return 0
	}

	if !info.IsDir() {
		return info.Size()
	}

	var totalSize int64
	filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot access to path: '%s': %v\n", path, err)
			return nil
		} else if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				fmt.Fprintf(os.Stderr, "cannot access to path: '%s': %v\n", path, err)
			} else {
				totalSize += info.Size()
			}
		}
		return nil
	})
	return totalSize
}

func Splitmix64(x uint64) uint64 {
	z := x
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func IntToKey(value uint64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, value)
	return key
}

func IntToValue(value uint64) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, Splitmix64(value))
	return data
}

func ValueToInt(bytes []byte) uint64 {
	if len(bytes) != 8 {
		panic(fmt.Errorf("invalid value byte size: %d", len(bytes)))
	}
	return binary.LittleEndian.Uint64(bytes)
}

func Linspace(min, max uint64, n int) []uint64 {
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
func Logspace(min, max uint64, n int) []uint64 {
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
