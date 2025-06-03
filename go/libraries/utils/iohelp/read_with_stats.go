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
        read      uint64
        size      int64
        rd        io.Reader
        start     time.Time
        closeCh   chan struct{}
        runningCh chan struct{}
}

func NewReaderWithStats(rd io.Reader, size int64) *ReaderWithStats {
	return &ReaderWithStats{
		rd:      rd,
		size:    size,
		closeCh: make(chan struct{}),
	}
}

func (rws *ReaderWithStats) Start(updateFunc func(ReadStats)) {
        if rws.runningCh != nil {
                panic("cannot start ReaderWithStats more than once.")
        }
        rws.runningCh = make(chan struct{})
        defer close(rws.runningCh)
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
				var percent float64
				if rws.size != 0 {
					percent = float64(read) / float64(rws.size)
				}
				updateFunc(ReadStats{Read: read, Elapsed: elapsed, Percent: percent})
				timer.Reset(updateFrequency)
			}
		}
	}()
}

func (rws *ReaderWithStats) Close() error {
        // Ensure that we never call |updateFunc| after Close() returns.
        if rws.runningCh != nil {
                <-rws.runningCh
        }
	close(rws.closeCh)

	if closer, ok := rws.rd.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

func (rws *ReaderWithStats) Read(p []byte) (int, error) {
	n, err := rws.rd.Read(p)

	atomic.AddUint64(&rws.read, uint64(n))

	return n, err
}

func (rws *ReaderWithStats) Size() int64 {
	return rws.size
}
