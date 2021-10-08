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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package metrics

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dolthub/vitess/go/race"
	"github.com/dustin/go-humanize"

	"github.com/dolthub/dolt/go/store/d"
)

// Histogram is a shameless and low-rent knock of the chromium project's
// histogram:
//   https://chromium.googlesource.com/chromium/src/base/+/master/metrics/histogram.h
//
// It logically stores a running histogram of uint64 values and shares some
// important features of its inspiration:
//   * It acccepts a correctness deficit in return for not needing to lock.
//     IOW, concurrent calls to Sample may clobber each other.
//   * It trades compactness and ease of arithmatic across histograms for
//     precision. Samples lose precision up to the range of the values which
//     are stored in a bucket
//
// Only implemented: Log2-based histogram

const bucketCount = 64

type HistogramType uint64

const (
	UnspecifiedHistogram HistogramType = iota
	TimeHistogram
	ByteHistogram
)

type Histogram struct {
	// this structure needs to be a multiple of 8 bytes in size. This is necessary for 32-bit architectures and
	// guarantees 8 byte alignment for multiple Histograms laid out side by side in memory.
	sum      uint64
	buckets  [bucketCount]uint64
	histType HistogramType
}

// Histogram is intended to be lock free
// |histLock| is only used under the race-detector
var histLock *sync.Mutex

func init() {
	if race.Enabled {
		histLock = &sync.Mutex{}
	}
	// if !race.Enabled |histLock| will nil panic
}

// Sample adds a uint64 data point to the histogram
func (h *Histogram) Sample(v uint64) {
	if race.Enabled {
		histLock.Lock()
		defer histLock.Unlock()
	}

	d.PanicIfTrue(v == 0)

	h.sum += v

	pot := 0
	for v > 0 {
		v = v >> 1
		pot++
	}

	h.buckets[pot-1]++
}

func (h *Histogram) Clone() *Histogram {
	n := &Histogram{histType: h.histType}
	n.Add(h)
	return n
}

// SampleTimeSince is a convenience wrapper around Sample which takes the
// duration since |t|, if 0, rounds to 1 and passes to Sample() as an uint64
// number of nanoseconds.
func (h *Histogram) SampleTimeSince(t time.Time) {
	dur := time.Since(t)
	if dur == 0 {
		dur = 1
	}
	h.Sample(uint64(dur))
}

// SampleLen is a convenience wrapper around Sample which internally type
// asserts the int to a uint64
func (h *Histogram) SampleLen(l int) {
	h.Sample(uint64(l))
}

func (h Histogram) bucketVal(bucket int) uint64 {
	return 1 << (uint64(bucket))
}

// Sum return the sum of sampled values, note that Sum can be overflowed without
// overflowing the histogram buckets.
func (h Histogram) Sum() uint64 {
	if race.Enabled {
		histLock.Lock()
		defer histLock.Unlock()
	}

	return h.sum
}

// Add returns a new Histogram which is the result of adding this and other
// bucket-wise.
func (h *Histogram) Add(other *Histogram) {
	if race.Enabled {
		histLock.Lock()
		defer histLock.Unlock()
	}

	h.sum += other.sum

	for i := 0; i < bucketCount; i++ {
		h.buckets[i] += other.buckets[i]
	}
}

// Mean returns 0 if there are no samples, and h.Sum()/h.Samples otherwise.
func (h Histogram) Mean() uint64 {
	samples := h.Samples()
	if samples == 0 {
		return 0
	}

	return h.Sum() / samples
}

// Samples returns the number of samples contained in the histogram
func (h Histogram) Samples() uint64 {
	if race.Enabled {
		histLock.Lock()
		defer histLock.Unlock()
	}

	s := uint64(0)
	for i := 0; i < bucketCount; i++ {
		s += h.buckets[i]
	}
	return s
}

func uintToString(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func timeToString(v uint64) string {
	return time.Duration(v).String()
}

func (h Histogram) String() string {
	if race.Enabled {
		histLock.Lock()
		defer histLock.Unlock()
	}

	var f func(uint64) string
	switch h.histType {
	case UnspecifiedHistogram:
		f = uintToString
	case ByteHistogram:
		f = humanize.Bytes
	case TimeHistogram:
		f = timeToString
	}

	return fmt.Sprintf("Mean: %s, Sum: %s, Samples: %d", f(h.Mean()), f(h.Sum()), h.Samples())
}

func NewTimeHistogram() Histogram {
	return Histogram{
		histType: TimeHistogram,
	}
}

// NewByteHistogram stringifies values using humanize over byte values
func NewByteHistogram() Histogram {
	return Histogram{
		histType: ByteHistogram,
	}
}
