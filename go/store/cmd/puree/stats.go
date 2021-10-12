package main

import (
	"fmt"
	"strings"

	"github.com/dolthub/dolt/go/store/metrics"
	"github.com/dolthub/dolt/go/store/types"
)

// WriteAmplificationStats records sequenceChunker write writeSizes by tree level
type WriteAmplificationStats struct {
	stats []metrics.Histogram
}

func (was *WriteAmplificationStats) Sample(stats []types.WriteStats) {
	for i := len(was.stats); i < len(stats); i++ {
		was.stats = append(was.stats, metrics.NewByteHistogram())
	}

	for i, writes := range stats {
		for _, w := range writes {
			was.stats[i].Sample(w)
		}
	}
}

func (was WriteAmplificationStats) Count() (c uint64) {
	for _, hist := range was.stats {
		c += hist.Samples()
	}
	return
}

func (was WriteAmplificationStats) Sum() (s uint64) {
	for _, h := range was.stats {
		s += h.Sum()
	}
	return
}

func (was WriteAmplificationStats) Summary(samples int64) string {
	var s strings.Builder

	s.WriteString("| level | chunks |    bytes |\n")
	for level, hist := range was.stats {
		c := float64(hist.Samples()) / float64(samples)
		b := float64(hist.Sum()) / float64(samples)
		r := fmt.Sprintf("| %5d | %6.2f | %8.2f |\n", level, c, b)
		s.WriteString(r)
	}
	return s.String()
}
