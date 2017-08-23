package metrics

import (
	"time"
)

// Increment only metric
type Counter interface {
	Set(float64) // Introduced discontinuity
	Inc()
	Add(float64) // Only positive
}

// Increse and decrese metric
type Gauge interface {
	Set(float64) // Introduced discontinuity
	Inc()
	Dec()
	Add(float64)
	Sub(float64)
}

type Histogram interface {
	Observe(float64) // Adds observation to Histogram
}

type Summary interface {
	Observe(float64) // Adds observation to Summary
}

// Consult http://godoc.org/github.com/prometheus/client_golang/prometheus#SummaryOpts
type SummaryOpts struct {
	Objectives map[float64]float64
	MaxAge     time.Duration
	AgeBuckets uint32
	BufCap     uint32
}

type Creator interface {
	Counter() Counter
	Gauge() Gauge
	Histogram(buckets []float64) Histogram

	// opts cannot be nil, use empty summary instance
	Summary(opts SummaryOpts) Summary
}
