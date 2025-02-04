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

package pull

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// ErrDBUpToDate is the error code returned from NewPuller in the event that there is no work to do.
var ErrDBUpToDate = errors.New("the database does not need to be pulled as it's already up to date")

// ErrIncompatibleSourceChunkStore is the error code returned from NewPuller in
// the event that the source ChunkStore does not implement `NBSCompressedChunkStore`.
var ErrIncompatibleSourceChunkStore = errors.New("the chunk store of the source database does not implement NBSCompressedChunkStore.")

type WalkAddrs func(chunks.Chunk, func(hash.Hash, bool) error) error

// Puller is used to sync data between to Databases
type Puller struct {
	waf WalkAddrs

	srcChunkStore nbs.NBSCompressedChunkStore
	sinkDBCS      chunks.ChunkStore
	hashes        hash.HashSet

	wr *PullTableFileWriter
	rd nbs.ChunkFetcher

	pushLog *log.Logger

	statsCh chan Stats
	stats   *stats
}

// NewPuller creates a new Puller instance to do the syncing.  If a nil puller is returned without error that means
// that there is nothing to pull and the sinkDB is already up to date.
func NewPuller(
	ctx context.Context,
	tempDir string,
	chunksPerTF int,
	srcCS, sinkCS chunks.ChunkStore,
	walkAddrs WalkAddrs,
	hashes []hash.Hash,
	statsCh chan Stats,
) (*Puller, error) {
	// Sanity Check
	hs := hash.NewHashSet(hashes...)
	missing, err := srcCS.HasMany(ctx, hs)
	if err != nil {
		return nil, err
	}
	if missing.Size() != 0 {
		return nil, errors.New("not found")
	}

	hs = hash.NewHashSet(hashes...)
	missing, err = sinkCS.HasMany(ctx, hs)
	if err != nil {
		return nil, err
	}
	if missing.Size() == 0 {
		return nil, ErrDBUpToDate
	}

	if srcCS.Version() != sinkCS.Version() {
		return nil, fmt.Errorf("cannot pull from src to sink; src version is %v and sink version is %v", srcCS.Version(), sinkCS.Version())
	}

	srcChunkStore, ok := srcCS.(nbs.NBSCompressedChunkStore)
	if !ok {
		return nil, ErrIncompatibleSourceChunkStore
	}

	wr := NewPullTableFileWriter(ctx, PullTableFileWriterConfig{
		ConcurrentUploads:    2,
		ChunksPerFile:        chunksPerTF,
		MaximumBufferedFiles: 8,
		TempDir:              tempDir,
		DestStore:            sinkCS.(chunks.TableFileStore),
	})

	rd := GetChunkFetcher(ctx, srcChunkStore)

	var pushLogger *log.Logger
	if dbg, ok := os.LookupEnv(dconfig.EnvPushLog); ok && strings.EqualFold(dbg, "true") {
		logFilePath := filepath.Join(tempDir, "push.log")
		f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)

		if err == nil {
			pushLogger = log.New(f, "", log.Lmicroseconds)
		}
	}

	p := &Puller{
		waf:           walkAddrs,
		srcChunkStore: srcChunkStore,
		sinkDBCS:      sinkCS,
		hashes:        hash.NewHashSet(hashes...),
		wr:            wr,
		rd:            rd,
		pushLog:       pushLogger,
		statsCh:       statsCh,
		stats: &stats{
			wrStatsGetter: wr.GetStats,
		},
	}

	if lcs, ok := sinkCS.(chunks.LoggingChunkStore); ok {
		lcs.SetLogger(p)
	}

	return p, nil
}

func (p *Puller) Logf(fmt string, args ...interface{}) {
	if p.pushLog != nil {
		p.pushLog.Printf(fmt, args...)
	}
}

type readable interface {
	Reader() (io.ReadCloser, error)
	Remove() error
}

type tempTblFile struct {
	id          string
	read        readable
	numChunks   int
	chunksLen   uint64
	contentLen  uint64
	contentHash []byte
}

type countingReader struct {
	io.ReadCloser
	cnt *uint64
}

func (c countingReader) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	atomic.AddUint64(c.cnt, uint64(n))
	return n, err
}

func emitStats(s *stats, ch chan Stats) (cancel func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	cancel = func() {
		close(done)
		wg.Wait()
	}

	go func() {
		defer wg.Done()
		sampleduration := 100 * time.Millisecond
		samplesinsec := uint64((1 * time.Second) / sampleduration)
		weight := 0.1
		ticker := time.NewTicker(sampleduration)
		defer ticker.Stop()
		var lastSendBytes, lastFetchedBytes uint64
		for {
			select {
			case <-ticker.C:
				wrStats := s.wrStatsGetter()
				newSendBytes := wrStats.FinishedSendBytes
				newFetchedBytes := atomic.LoadUint64(&s.fetchedSourceBytes)
				sendBytesDiff := newSendBytes - lastSendBytes
				fetchedBytesDiff := newFetchedBytes - lastFetchedBytes

				newSendBPS := float64(sendBytesDiff * samplesinsec)
				newFetchedBPS := float64(fetchedBytesDiff * samplesinsec)

				curSendBPS := math.Float64frombits(atomic.LoadUint64(&s.sendBytesPerSec))
				curFetchedBPS := math.Float64frombits(atomic.LoadUint64(&s.fetchedSourceBytesPerSec))

				smoothedSendBPS := newSendBPS
				if curSendBPS != 0 {
					smoothedSendBPS = curSendBPS + weight*(newSendBPS-curSendBPS)
				}

				smoothedFetchBPS := newFetchedBPS
				if curFetchedBPS != 0 {
					smoothedFetchBPS = curFetchedBPS + weight*(newFetchedBPS-curFetchedBPS)
				}

				if smoothedSendBPS < 1 {
					smoothedSendBPS = 0
				}
				if smoothedFetchBPS < 1 {
					smoothedFetchBPS = 0
				}

				atomic.StoreUint64(&s.sendBytesPerSec, math.Float64bits(smoothedSendBPS))
				atomic.StoreUint64(&s.fetchedSourceBytesPerSec, math.Float64bits(smoothedFetchBPS))

				lastSendBytes = newSendBytes
				lastFetchedBytes = newFetchedBytes
			case <-done:
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		updateduration := 1 * time.Second
		ticker := time.NewTicker(updateduration)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ch <- s.read()
			case <-done:
				ch <- s.read()
				return
			}
		}
	}()

	return cancel
}

type stats struct {
	sendBytesPerSec uint64

	totalSourceChunks        uint64
	fetchedSourceChunks      uint64
	fetchedSourceBytes       uint64
	fetchedSourceBytesPerSec uint64

	sendBytesPerSecF          float64
	fetchedSourceBytesPerSecF float64

	wrStatsGetter func() PullTableFileWriterStats
}

type Stats struct {
	FinishedSendBytes uint64
	BufferedSendBytes uint64
	SendBytesPerSec   float64

	TotalSourceChunks        uint64
	FetchedSourceChunks      uint64
	FetchedSourceBytes       uint64
	FetchedSourceBytesPerSec float64
}

func (s *stats) read() Stats {
	wrStats := s.wrStatsGetter()

	var ret Stats
	ret.FinishedSendBytes = wrStats.FinishedSendBytes
	ret.BufferedSendBytes = wrStats.BufferedSendBytes
	ret.SendBytesPerSec = math.Float64frombits(atomic.LoadUint64(&s.sendBytesPerSec))
	ret.TotalSourceChunks = atomic.LoadUint64(&s.totalSourceChunks)
	ret.FetchedSourceChunks = atomic.LoadUint64(&s.fetchedSourceChunks)
	ret.FetchedSourceBytes = atomic.LoadUint64(&s.fetchedSourceBytes)
	ret.FetchedSourceBytesPerSec = math.Float64frombits(atomic.LoadUint64(&s.fetchedSourceBytesPerSec))
	return ret
}

// Pull executes the sync operation
func (p *Puller) Pull(ctx context.Context) error {
	if p.statsCh != nil {
		c := emitStats(p.stats, p.statsCh)
		defer c()
	}

	eg, ctx := errgroup.WithContext(ctx)

	const batchSize = 64 * 1024
	tracker := NewPullChunkTracker(ctx, p.hashes, TrackerConfig{
		BatchSize: batchSize,
		HasManyer: p.sinkDBCS,
	})

	// One thread calls ChunkFetcher.Get on each batch.
	eg.Go(func() error {
		for {
			toFetch, hasMore, err := tracker.GetChunksToFetch()
			if err != nil {
				return err
			}
			if !hasMore {
				return p.rd.CloseSend()
			}

			atomic.AddUint64(&p.stats.totalSourceChunks, uint64(len(toFetch)))
			err = p.rd.Get(ctx, toFetch)
			if err != nil {
				return err
			}
		}
	})

	// One thread reads the received chunks, walks their addresses and writes them to the table file.
	eg.Go(func() error {
		for {
			cChk, err := p.rd.Recv(ctx)
			if err == io.EOF {
				// This means the requesting thread
				// successfully saw all chunk addresses and
				// called CloseSend and that all requested
				// chunks were successfully delivered to this
				// thread. Calling wr.Close() here will block
				// on uploading any table files and will write
				// the new table files to the destination's
				// manifest.
				return p.wr.Close()
			}
			if err != nil {
				return err
			}
			if cChk.IsGhost() {
				return fmt.Errorf("attempted to push or pull ghost chunk: %w", nbs.ErrGhostChunkRequested)
			}
			if cChk.FullCompressedChunkLen() == 0 {
				return errors.New("failed to get all chunks.")
			}

			atomic.AddUint64(&p.stats.fetchedSourceBytes, uint64(cChk.FullCompressedChunkLen()))
			atomic.AddUint64(&p.stats.fetchedSourceChunks, uint64(1))

			chnk, err := cChk.ToChunk()
			if err != nil {
				return err
			}
			err = p.waf(chnk, func(h hash.Hash, _ bool) error {
				tracker.Seen(h)
				return nil
			})
			if err != nil {
				return err
			}
			tracker.TickProcessed()

			if compressedChunk, ok := cChk.(nbs.CompressedChunk); ok {
				err = p.wr.AddCompressedChunk(ctx, compressedChunk)
				if err != nil {
					return err
				}
			} else if _, ok := cChk.(remotestorage.ArchiveToChunker); ok {
				// NM4 - Until we can write quickly to archives.....
				cc := nbs.ChunkToCompressedChunk(chnk)

				err = p.wr.AddCompressedChunk(ctx, cc)
				if err != nil {
					return err
				}
			}
		}
	})

	// Always close the reader outside of the errgroup threads above.
	// Closing the reader will cause Get() to start returning errors, and
	// we don't need to deliver that error to the Get thread. Both threads
	// should exit and return any errors they encounter, after which the
	// errgroup will report the error.
	wErr := eg.Wait()
	rErr := p.rd.Close()
	return errors.Join(wErr, rErr)
}
