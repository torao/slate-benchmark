package common

import (
	"fmt"
	"strings"
	"time"
)

// ExpirationTimer represents a timer with progress tracking and ETA calculation
type ExpirationTimer struct {
	start          time.Time
	deadline       time.Duration
	lastNoticed    time.Time
	noticeInterval time.Duration
	maxTrials      int
	current        int
	interval       int
}

// NewExpirationTimer creates a new ExpirationTimer
func NewExpirationTimer(deadline time.Duration, minutes int, maxTrials int, div int) *ExpirationTimer {
	start := time.Now()
	return &ExpirationTimer{
		start:          start,
		deadline:       deadline,
		lastNoticed:    start,
		noticeInterval: time.Duration(minutes) * time.Minute,
		maxTrials:      maxTrials,
		current:        0,
		interval:       maxTrials / div,
	}
}

// Expired checks if the timer has expired
func (et *ExpirationTimer) Expired() bool {
	return time.Since(et.start) >= et.deadline
}

// Elapsed returns the elapsed time since start
func (et *ExpirationTimer) Elapsed() time.Duration {
	return time.Since(et.start)
}

// EstimatedEndTime calculates the estimated end time based on current progress
func (et *ExpirationTimer) EstimatedEndTime() time.Time {
	if et.current == 0 {
		return et.start.Add(et.deadline)
	}

	avgPerTrial := et.Elapsed() / time.Duration(et.current)
	totalEstimate := avgPerTrial * time.Duration(et.maxTrials)
	return et.start.Add(totalEstimate)
}

// ETA returns a formatted string showing estimated time of arrival
func (et *ExpirationTimer) ETA() string {
	estimatedEnd := et.EstimatedEndTime()
	now := time.Now()
	diff := estimatedEnd.Sub(now)

	// Determine format based on time difference
	var format string
	if estimatedEnd.Format("2006-01-02") != now.Format("2006-01-02") {
		format = "01-02 15:04"
	} else if diff.Hours() >= 1 {
		format = "15:04"
	} else {
		format = "15:04:05"
	}

	eta := estimatedEnd.Format(format)

	// Calculate remaining time
	totalSeconds := int(diff.Seconds())
	if totalSeconds < 0 {
		totalSeconds = 0
	}

	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60

	var remaining string
	if hours > 0 {
		remaining = fmt.Sprintf("%dh%02dm", hours, minutes)
	} else if minutes > 0 {
		remaining = fmt.Sprintf("%dm%02ds", minutes, seconds)
	} else {
		remaining = fmt.Sprintf("%ds", seconds)
	}

	return fmt.Sprintf("%s (%s)", eta, remaining)
}

// CarriedOut updates the progress and returns true if should notify
func (et *ExpirationTimer) CarriedOut(amount int) bool {
	current := et.current
	et.current += amount

	shouldNotify := (time.Since(et.lastNoticed) >= et.noticeInterval) ||
		et.current >= et.maxTrials ||
		(current != 0 && (et.current/et.interval != current/et.interval))

	if shouldNotify {
		et.lastNoticed = time.Now()
		return true
	}

	return false
}

// HeadingMS prints the header for millisecond statistics
func (et *ExpirationTimer) HeadingMS() {
	columns := []Column{
		{Type: DataSize, UInt64Val: 0},
		{Type: MeanMS, Float64Val: 0.0},
		{Type: StdDevMS, Float64Val: 0.0},
		{Type: CV, Float64Val: 0.0},
		{Type: Trials, IntVal: 0},
		{Type: ETA, StringVal: ""},
	}
	et.printHeading(columns)
}

// SummaryMS prints a summary line for millisecond statistics
func (et *ExpirationTimer) SummaryMS(dataSize uint64, mean, stdDev float64) {
	columns := []Column{
		{Type: DataSize, UInt64Val: dataSize},
		{Type: MeanMS, Float64Val: mean},
		{Type: StdDevMS, Float64Val: stdDev},
		{Type: CV, Float64Val: stdDev / mean * 100.0},
		{Type: Trials, IntVal: et.current},
		{Type: ETA, StringVal: et.ETA()},
	}
	et.printSummary(columns)
}

// HeadingMaxCV prints the header for max CV statistics
func (et *ExpirationTimer) HeadingMaxCV() {
	columns := []Column{
		{Type: DataSize, UInt64Val: 0},
		{Type: CV, Float64Val: 0.0},
		{Type: Trials, IntVal: 0},
		{Type: ETA, StringVal: ""},
	}
	et.printHeading(columns)
}

// SummaryMaxCV prints a summary line for max CV statistics
func (et *ExpirationTimer) SummaryMaxCV(dataSize uint64, maxCV float64) {
	columns := []Column{
		{Type: DataSize, UInt64Val: dataSize},
		{Type: CV, Float64Val: maxCV * 100.0},
		{Type: Trials, IntVal: et.current},
		{Type: ETA, StringVal: et.ETA()},
	}
	et.printSummary(columns)
}

func (et *ExpirationTimer) printHeading(columns []Column) {
	headings := make([]string, len(columns))
	lines := make([]string, len(columns))

	for i, col := range columns {
		headings[i] = col.Heading()
		lines[i] = col.Line()
	}

	fmt.Println(strings.Join(headings, " "))
	fmt.Println(strings.Join(lines, " "))
}

func (et *ExpirationTimer) printSummary(columns []Column) {
	formatted := make([]string, len(columns))

	for i, col := range columns {
		formatted[i] = col.Format()
	}

	fmt.Println(strings.Join(formatted, " "))
}

// ColumnType represents the type of column
type ColumnType int

const (
	DataSize ColumnType = iota
	MeanMS
	StdDevMS
	CV
	Trials
	ETA
)

// Column represents a table column with formatting
type Column struct {
	Type       ColumnType
	UInt64Val  uint64
	Float64Val float64
	IntVal     int
	StringVal  string
}

// Label returns the column label
func (c *Column) Label() string {
	switch c.Type {
	case DataSize:
		return "DataSize"
	case MeanMS:
		return "Mean[ms]"
	case StdDevMS:
		return "StdDev[ms]"
	case CV:
		return "CV[%]"
	case Trials:
		return "Trials"
	case ETA:
		return "ETA"
	default:
		return ""
	}
}

// Width returns the column width
func (c *Column) Width() int {
	labelLen := len(c.Label())
	var minWidth int

	switch c.Type {
	case DataSize:
		minWidth = 10
	case MeanMS:
		minWidth = 9
	case StdDevMS:
		minWidth = 9
	case CV:
		minWidth = 6
	case Trials:
		minWidth = 9
	case ETA:
		minWidth = 18
	default:
		minWidth = labelLen
	}

	if labelLen > minWidth {
		return labelLen
	}
	return minWidth
}

// Heading returns the formatted column heading
func (c *Column) Heading() string {
	label := c.Label()
	width := c.Width()

	// Center align the heading
	padding := width - len(label)
	leftPad := padding / 2
	rightPad := padding - leftPad

	return strings.Repeat(" ", leftPad) + label + strings.Repeat(" ", rightPad)
}

// Line returns the separator line for the column
func (c *Column) Line() string {
	return strings.Repeat("-", c.Width())
}

// Format returns the formatted column value
func (c *Column) Format() string {
	width := c.Width()

	switch c.Type {
	case DataSize:
		return fmt.Sprintf("%*d", width, c.UInt64Val)
	case MeanMS:
		return fmt.Sprintf("%*.3f", width, c.Float64Val)
	case StdDevMS:
		return fmt.Sprintf("%*.3f", width, c.Float64Val)
	case CV:
		return fmt.Sprintf("%*.1f", width, c.Float64Val)
	case Trials:
		return fmt.Sprintf("%*d", width, c.IntVal)
	case ETA:
		return fmt.Sprintf("%-*s", width, c.StringVal)
	default:
		return fmt.Sprintf("%*s", width, "")
	}
}

// Example usage
func main() {
	// Create a timer that expires in 1 hour, notifies every 5 minutes,
	// with max 1000 trials, and progress intervals of 100
	timer := NewExpirationTimer(1*time.Hour, 5, 1000, 10)

	// Print header for millisecond statistics
	timer.HeadingMS()

	// Simulate some work
	for i := 0; i < 100; i++ {
		// Simulate work
		time.Sleep(10 * time.Millisecond)

		// Update progress
		if timer.CarriedOut(1) {
			// Print progress summary (example values)
			timer.SummaryMS(1024, 15.5, 2.3)
		}

		// Check if expired
		if timer.Expired() {
			fmt.Println("Timer expired!")
			break
		}
	}

	fmt.Println("\nMax CV format:")
	timer.HeadingMaxCV()
	timer.SummaryMaxCV(2048, 0.15)
}
