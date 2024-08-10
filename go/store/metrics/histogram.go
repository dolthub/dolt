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
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/dolthub/dolt/go/store/d"
)

// Histogram is a shameless and low-rent knock of the chromium project's
// histogram:
//   https://chromium.googlesource.com/chromium/src/base/+/master/metrics/histogram.h
//
// It logically stores a running histogram of uint64 values and shares some
// important features of its inspiration:
//   * It accepts a correctness deficit in return for not needing to lock.
//     IOW, concurrent calls to Sample may clobber each other.
//   * It trades compactness and ease of arithmetic across histograms for
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

// Sample adds a uint64 data point to the histogram
func (h *Histogram) Sample(v uint64) {
	d.PanicIfTrue(v == 0)

	atomic.AddUint64(&h.sum, v)

	pot := 0
	for v > 0 {
		v = v >> 1
		pot++
	}

	atomic.AddUint64(&h.buckets[pot-1], 1)
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
	d := time.Since(t)
	if d == 0 {
		d = 1
	}
	h.Sample(uint64(d))
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
	return atomic.LoadUint64(&h.sum)
}

// Add returns a new Histogram which is the result of adding this and other
// bucket-wise.
func (h *Histogram) Add(other *Histogram) {
	atomic.AddUint64(&h.sum, atomic.LoadUint64(&other.sum))

	for i := 0; i < bucketCount; i++ {
		atomic.AddUint64(&h.buckets[i], atomic.LoadUint64(&other.buckets[i]))
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
	s := uint64(0)
	for i := 0; i < bucketCount; i++ {
		s += atomic.LoadUint64(&h.buckets[i])
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
	return Histogram{histType: TimeHistogram}
}

// NewByteHistogram stringifies values using humanize over byte values
func NewByteHistogram() Histogram {
	return Histogram{histType: ByteHistogram}
}
