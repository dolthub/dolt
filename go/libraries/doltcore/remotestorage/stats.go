// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remotestorage

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/fatih/color"
)

var StatsFactory func() StatsRecorder = NullStatsRecorderFactory
var StatsFlusher func(StatsRecorder) = func(StatsRecorder) {}

func NullStatsRecorderFactory() StatsRecorder {
	return NullStatsRecorder{}
}

func HistogramStatsRecorderFactory() StatsRecorder {
	return NewHistorgramStatsRecorder()
}

func StatsFlusherToColorError(r StatsRecorder) {
	r.WriteSummaryTo(color.Error)
}

func init() {
	if _, ok := os.LookupEnv(doltdb.EnvRemoteVersionDownloadStats); ok {
		StatsFactory = HistogramStatsRecorderFactory
		StatsFlusher = StatsFlusherToColorError
	}
}

type StatsRecorder interface {
	RecordTimeToFirstByte(hedge, retry int, size uint64, d time.Duration)
	RecordDownloadAttemptStart(hedge, retry int, offset, size uint64)
	RecordDownloadComplete(hedge, retry int, size uint64, d time.Duration)
	WriteSummaryTo(io.Writer) error
}

var _ StatsRecorder = NullStatsRecorder{}

type NullStatsRecorder struct {
}

func (NullStatsRecorder) RecordTimeToFirstByte(hedge, retry int, size uint64, d time.Duration) {
}

func (NullStatsRecorder) RecordDownloadAttemptStart(hedge, retry int, offset, size uint64) {
}

func (NullStatsRecorder) RecordDownloadComplete(hedge, retry int, size uint64, d time.Duration) {
}

func (NullStatsRecorder) WriteSummaryTo(io.Writer) error {
	return nil
}

type HistogramStatsRecorder struct {
	mu                 *sync.Mutex
	sizes              *hdrhistogram.Histogram
	downloadTimeMillis *hdrhistogram.Histogram
	firstByteMillis    *hdrhistogram.Histogram
	retryCount         int
	hedgeCount         int
	hedgeCompleteCount int
}

func NewHistorgramStatsRecorder() *HistogramStatsRecorder {
	return &HistogramStatsRecorder{
		new(sync.Mutex),
		hdrhistogram.New(128, 4294967296, 3), // 128 bytes - 4 GB
		hdrhistogram.New(10, 3600000, 3),     // 10 ms - 1 hr
		hdrhistogram.New(10, 300000, 3),      // 10 ms - 5 mins
		0,
		0,
		0,
	}
}

func (r *HistogramStatsRecorder) RecordTimeToFirstByte(hedge, retry int, size uint64, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.firstByteMillis.RecordValue(int64(d / time.Millisecond))
}

func (r *HistogramStatsRecorder) RecordDownloadAttemptStart(hedge, retry int, offset, size uint64) {
	if hedge == 1 && retry == 0 {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.sizes.RecordValue(int64(size))
	}
	if retry > 0 {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.retryCount += 1
	} else if hedge > 1 {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.hedgeCount += 1
	}
}

func (r *HistogramStatsRecorder) RecordDownloadComplete(hedge, retry int, size uint64, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.downloadTimeMillis.RecordValue(int64(d / time.Millisecond))
	if hedge > 1 {
		r.hedgeCompleteCount += 1
	}
}

func (r *HistogramStatsRecorder) WriteSummaryTo(w io.Writer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := fmt.Fprintf(w, "total downloads: %d, retries: %d, hedges: %d, completed hedges: %d\n",
		r.sizes.TotalCount(), r.retryCount, r.hedgeCount, r.hedgeCompleteCount)
	if err != nil {
		return err
	}
	err = writeHistogram(w, "sizes (bytes)", r.sizes)
	if err != nil {
		return err
	}
	err = writeHistogram(w, "time to first byte (millis)", r.firstByteMillis)
	if err != nil {
		return err
	}
	err = writeHistogram(w, "download time (millis)", r.downloadTimeMillis)
	if err != nil {
		return err
	}
	return nil
}

func writeHistogram(w io.Writer, prefix string, h *hdrhistogram.Histogram) error {
	_, err := fmt.Fprintf(w, "%s: p50: %d, p90: %d, p99: %d, avg: %.2f, max: %d\n", prefix,
		h.ValueAtQuantile(50), h.ValueAtQuantile(90), h.ValueAtQuantile(99), h.Mean(), h.Max())
	return err
}
