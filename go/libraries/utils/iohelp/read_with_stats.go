// Copyright 2021 Dolthub, Inc.
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

package iohelp

import (
	"io"
	"sync/atomic"
	"time"
)

const updateFrequency = 500 * time.Millisecond

type ReadStats struct {
	Read    uint64
	Elapsed time.Duration
	Percent float64
}

type ReaderWithStats struct {
	rd      io.Reader
	size    int64
	start   time.Time
	read    uint64
	closeCh chan struct{}
}

func NewReaderWithStats(rd io.Reader, size int64) *ReaderWithStats {
	return &ReaderWithStats{
		rd:      rd,
		size:    size,
		closeCh: make(chan struct{}),
	}
}

func (rws *ReaderWithStats) Start(updateFunc func(ReadStats)) {
	rws.start = time.Now()
	go func() {
		timer := time.NewTimer(updateFrequency)
		for {
			select {
			case <-rws.closeCh:
				return
			case <-timer.C:
				read := atomic.LoadUint64(&rws.read)
				elapsed := time.Since(rws.start)
				percent := float64(read) / float64(rws.size)
				updateFunc(ReadStats{Read: read, Elapsed: elapsed, Percent: percent})
				timer.Reset(updateFrequency)
			}
		}
	}()
}

func (rws *ReaderWithStats) Stop() {
	close(rws.closeCh)

	if closer, ok := rws.rd.(io.Closer); ok {
		_ = closer.Close()
	}
}

func (rws *ReaderWithStats) Read(p []byte) (int, error) {
	n, err := rws.rd.Read(p)

	atomic.AddUint64(&rws.read, uint64(n))

	return n, err
}

func (rws *ReaderWithStats) Size() int64 {
	return rws.size
}
