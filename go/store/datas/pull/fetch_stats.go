// Copyright 2026 Dolthub, Inc.
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

package pull

import (
	"sync/atomic"
	"time"

	"github.com/dolthub/dolt/go/store/nbs"
)

// fetchStatsRecorder is a per-pull implementation of nbs.StatsRecorder. It
// accumulates aggregate counters about the byte ranges a remote ChunkFetcher
// downloads, so they can be reported to the push log alongside the puller's own
// stats.
//
// The bytes counted here are the lengths of the (possibly coalesced) ranges
// downloaded over the wire, which include any "dark" bytes fetched between
// requested chunks as a result of range coalescing. Contrast this with the
// puller's fetchedSourceBytes, which counts only the decompressed bytes of the
// chunks that were actually wanted; the difference between the two is the
// coalescing + compression overhead.
type fetchStatsRecorder struct {
	completedDownloads atomic.Uint64
	retries            atomic.Uint64
	downloadedBytes    atomic.Uint64
	inFlight           atomic.Int64
	peakInFlight       atomic.Int64
}

var _ nbs.StatsRecorder = (*fetchStatsRecorder)(nil)

func (s *fetchStatsRecorder) RecordTimeToFirstByte(retry int, size uint64, d time.Duration) {}

func (s *fetchStatsRecorder) RecordDownloadAttemptStart(retry int, offset, size uint64) {
	if retry == 0 {
		// First attempt for a range: a logical download is now in flight,
		// which (with one HTTP request per download goroutine at a time) is a
		// proxy for the number of concurrent connections open.
		n := s.inFlight.Add(1)
		for {
			peak := s.peakInFlight.Load()
			if n <= peak || s.peakInFlight.CompareAndSwap(peak, n) {
				break
			}
		}
	} else {
		s.retries.Add(1)
	}
}

func (s *fetchStatsRecorder) RecordDownloadComplete(retry int, size uint64, d time.Duration) {
	s.completedDownloads.Add(1)
	s.downloadedBytes.Add(size)
	// NB: a range download that fails terminally never reaches here, so a
	// failed pull may leave inFlight overcounted. That is acceptable: a
	// terminal failure aborts the whole pull, so no further stats are logged.
	s.inFlight.Add(-1)
}

// fetchStats is an immutable snapshot of a fetchStatsRecorder.
type fetchStats struct {
	CompletedDownloads uint64
	Retries            uint64
	// DownloadedBytes is the total bytes downloaded over the wire, including
	// dark bytes fetched as part of range coalescing.
	DownloadedBytes uint64
	InFlight        int64
	PeakInFlight    int64
}

func (s *fetchStatsRecorder) read() fetchStats {
	return fetchStats{
		CompletedDownloads: s.completedDownloads.Load(),
		Retries:            s.retries.Load(),
		DownloadedBytes:    s.downloadedBytes.Load(),
		InFlight:           s.inFlight.Load(),
		PeakInFlight:       s.peakInFlight.Load(),
	}
}
