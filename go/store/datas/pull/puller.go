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

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// ErrDBUpToDate is the error code returned from NewPuller in the event that there is no work to do.
var ErrDBUpToDate = errors.New("the database does not need to be pulled as it's already up to date")

// ErrIncompatibleSourceChunkStore is the error code returned from NewPuller in
// the event that the source ChunkStore does not implement `NBSCompressedChunkStore`.
var ErrIncompatibleSourceChunkStore = errors.New("the chunk store of the source database does not implement NBSCompressedChunkStore.")

const (
	maxChunkWorkers       = 2
	outstandingTableFiles = 2
)

// FilledWriters store CmpChunkTableWriter that have been filled and are ready to be flushed.  In the future will likely
// add the md5 of the data to this structure to be used to verify table upload calls.
type FilledWriters struct {
	wr *nbs.CmpChunkTableWriter
}

// CmpChnkAndRefs holds a CompressedChunk and all of it's references
type CmpChnkAndRefs struct {
	cmpChnk nbs.CompressedChunk
}

type WalkAddrs func(chunks.Chunk, func(hash.Hash, bool) error) error

// Puller is used to sync data between to Databases
type Puller struct {
	waf WalkAddrs

	srcChunkStore nbs.NBSCompressedChunkStore
	sinkDBCS      chunks.ChunkStore
	hashes        hash.HashSet

	wr            *nbs.CmpChunkTableWriter
	tablefileSema *semaphore.Weighted
	tempDir       string
	chunksPerTF   int

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

	wr, err := nbs.NewCmpChunkTableWriter(tempDir)

	if err != nil {
		return nil, err
	}

	var pushLogger *log.Logger
	if dbg, ok := os.LookupEnv("PUSH_LOG"); ok && strings.ToLower(dbg) == "true" {
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
		tablefileSema: semaphore.NewWeighted(outstandingTableFiles),
		tempDir:       tempDir,
		wr:            wr,
		chunksPerTF:   chunksPerTF,
		pushLog:       pushLogger,
		statsCh:       statsCh,
		stats:         &stats{},
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
				newSendBytes := atomic.LoadUint64(&s.finishedSendBytes)
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
	finishedSendBytes uint64
	bufferedSendBytes uint64
	sendBytesPerSec   uint64

	totalSourceChunks        uint64
	fetchedSourceChunks      uint64
	fetchedSourceBytes       uint64
	fetchedSourceBytesPerSec uint64

	sendBytesPerSecF          float64
	fetchedSourceBytesPerSecF float64
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
	var ret Stats
	ret.FinishedSendBytes = atomic.LoadUint64(&s.finishedSendBytes)
	ret.BufferedSendBytes = atomic.LoadUint64(&s.bufferedSendBytes)
	ret.SendBytesPerSec = math.Float64frombits(atomic.LoadUint64(&s.sendBytesPerSec))
	ret.TotalSourceChunks = atomic.LoadUint64(&s.totalSourceChunks)
	ret.FetchedSourceChunks = atomic.LoadUint64(&s.fetchedSourceChunks)
	ret.FetchedSourceBytes = atomic.LoadUint64(&s.fetchedSourceBytes)
	ret.FetchedSourceBytesPerSec = math.Float64frombits(atomic.LoadUint64(&s.fetchedSourceBytesPerSec))
	return ret
}

func (p *Puller) uploadTempTableFile(ctx context.Context, tmpTblFile tempTblFile) error {
	fileSize := tmpTblFile.contentLen
	defer func() {
		_ = tmpTblFile.read.Remove()
	}()

	// By tracking the number of bytes uploaded here,
	// we can add bytes on to our bufferedSendBytes when
	// we have to retry a table file write.
	var localUploaded uint64
	return p.sinkDBCS.(chunks.TableFileStore).WriteTableFile(ctx, tmpTblFile.id, tmpTblFile.numChunks, tmpTblFile.contentHash, func() (io.ReadCloser, uint64, error) {
		rc, err := tmpTblFile.read.Reader()
		if err != nil {
			return nil, 0, err
		}

		if localUploaded == 0 {
			// So far, we've added all the bytes for the compressed chunk data.
			// We add the remaining bytes here --- bytes for the index and the
			// table file footer.
			atomic.AddUint64(&p.stats.bufferedSendBytes, uint64(fileSize)-tmpTblFile.chunksLen)
		} else {
			// A retry. We treat it as if what was already uploaded was rebuffered.
			atomic.AddUint64(&p.stats.bufferedSendBytes, uint64(localUploaded))
			localUploaded = 0
		}
		fWithStats := countingReader{countingReader{rc, &localUploaded}, &p.stats.finishedSendBytes}

		return fWithStats, uint64(fileSize), nil
	})
}

func (p *Puller) processCompletedTables(ctx context.Context, completedTables <-chan FilledWriters) error {
	fileIdToNumChunks := make(map[string]int)

LOOP:
	for {
		select {
		case tblFile, ok := <-completedTables:
			if !ok {
				break LOOP
			}
			p.tablefileSema.Release(1)

			// content length before we finish the write, which will
			// add the index and table file footer.
			chunksLen := tblFile.wr.ContentLength()

			id, err := tblFile.wr.Finish()
			if err != nil {
				return err
			}

			ttf := tempTblFile{
				id:          id,
				read:        tblFile.wr,
				numChunks:   tblFile.wr.ChunkCount(),
				chunksLen:   chunksLen,
				contentLen:  tblFile.wr.ContentLength(),
				contentHash: tblFile.wr.GetMD5(),
			}
			err = p.uploadTempTableFile(ctx, ttf)
			if err != nil {
				return err
			}

			fileIdToNumChunks[id] = ttf.numChunks
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return p.sinkDBCS.(chunks.TableFileStore).AddTableFilesToManifest(ctx, fileIdToNumChunks)
}

// Pull executes the sync operation
func (p *Puller) Pull(ctx context.Context) error {
	if p.statsCh != nil {
		c := emitStats(p.stats, p.statsCh)
		defer c()
	}

	eg, ctx := errgroup.WithContext(ctx)

	completedTables := make(chan FilledWriters, 8)

	eg.Go(func() error {
		return p.processCompletedTables(ctx, completedTables)
	})

	eg.Go(func() (err error) {
		if err = p.tablefileSema.Acquire(ctx, 1); err != nil {
			return err
		}

		const batchSize = 64 * 1024
		// refs are added to |visited| on first sight
		visited := p.hashes
		// |absent| are visited, un-batched refs
		absent := p.hashes.Copy()
		// |batches| are visited, un-fetched refs
		batches := make([]hash.HashSet, 0, 64)

		for absent.Size() > 0 || len(batches) > 0 {
			if absent.Size() >= batchSize {
				var bb []hash.HashSet
				absent, bb = batchNovel(absent, batchSize)
				batches = append(batches, bb...)
			}
			if len(batches) == 0 {
				batches = append(batches, absent)
				absent = make(hash.HashSet)
			}

			b := batches[len(batches)-1]
			batches = batches[:len(batches)-1]

			b, err = p.sinkDBCS.HasMany(ctx, b)
			if err != nil {
				return err
			} else if b.Size() == 0 {
				continue
			}

			err = p.getCmp(ctx, b, absent, visited, completedTables)
			if err != nil {
				return err
			}
		}

		if p.wr != nil && p.wr.ChunkCount() > 0 {
			select {
			case completedTables <- FilledWriters{p.wr}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		close(completedTables)
		return nil
	})

	return eg.Wait()
}

// batchNovel returns a slice of |batch| size HashSets and partial |remainder| HashSet.
func batchNovel(absent hash.HashSet, batch int) (remainder hash.HashSet, batches []hash.HashSet) {
	curr := make(hash.HashSet, batch)
	for h := range absent {
		curr.Insert(h)
		if curr.Size() >= batch {
			batches = append(batches, curr)
			curr = make(hash.HashSet, batch)
		}
	}
	remainder = curr
	return
}

func (p *Puller) getCmp(ctx context.Context, batch, absent, visited hash.HashSet, completedTables chan FilledWriters) error {
	found := make(chan nbs.CompressedChunk, 4096)
	processed := make(chan CmpChnkAndRefs, 4096)

	atomic.AddUint64(&p.stats.totalSourceChunks, uint64(len(batch)))
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		err := p.srcChunkStore.GetManyCompressed(ctx, batch, func(ctx context.Context, c nbs.CompressedChunk) {
			atomic.AddUint64(&p.stats.fetchedSourceBytes, uint64(len(c.FullCompressedChunk)))
			atomic.AddUint64(&p.stats.fetchedSourceChunks, uint64(1))
			select {
			case found <- c:
			case <-ctx.Done():
			}
		})
		if err != nil {
			return err
		}
		close(found)
		return nil
	})

	eg.Go(func() error {
	LOOP:
		for {
			select {
			case cmpChnk, ok := <-found:
				if !ok {
					break LOOP
				}

				chnk, err := cmpChnk.ToChunk()
				if err != nil {
					return err
				}
				err = p.waf(chnk, func(h hash.Hash, _ bool) error {
					if !visited.Has(h) {
						// first sight of |h|
						visited.Insert(h)
						absent.Insert(h)
					}
					return nil
				})
				if err != nil {
					return err
				}
				select {
				case processed <- CmpChnkAndRefs{cmpChnk: cmpChnk}:
				case <-ctx.Done():
					return ctx.Err()
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		close(processed)
		return nil
	})

	eg.Go(func() error {
		var seen int
	LOOP:
		for {
			select {
			case cmpAndRef, ok := <-processed:
				if !ok {
					break LOOP
				}
				seen++

				err := p.wr.AddCmpChunk(cmpAndRef.cmpChnk)
				if err != nil {
					return err
				}

				atomic.AddUint64(&p.stats.bufferedSendBytes, uint64(len(cmpAndRef.cmpChnk.FullCompressedChunk)))

				if p.wr.ChunkCount() >= p.chunksPerTF {
					select {
					case completedTables <- FilledWriters{p.wr}:
					case <-ctx.Done():
						return ctx.Err()
					}
					p.wr = nil

					if err := p.tablefileSema.Acquire(ctx, 1); err != nil {
						return err
					}
					p.wr, err = nbs.NewCmpChunkTableWriter(p.tempDir)
					if err != nil {
						return err
					}
				}

				cmpAndRef.cmpChnk.FullCompressedChunk = nil
				cmpAndRef.cmpChnk.CompressedData = nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if seen != len(batch) {
			return errors.New("failed to get all chunks.")
		}
		return nil
	})

	err := eg.Wait()
	if err != nil {
		return err
	}
	return nil
}
