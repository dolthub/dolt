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
