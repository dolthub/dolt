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

	"github.com/dolthub/dolt/go/libraries/utils/file"
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
	refs    map[hash.Hash]bool
}

// Puller is used to sync data between to Databases
type Puller struct {
	waf WalkAddrs

	srcChunkStore nbs.NBSCompressedChunkStore
	sinkDBCS      chunks.ChunkStore
	rootChunkHash hash.Hash
	downloaded    hash.HashSet

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
func NewPuller(ctx context.Context, tempDir string, chunksPerTF int, srcCS, sinkCS chunks.ChunkStore, walkAddrs WalkAddrs, rootChunkHash hash.Hash, statsCh chan Stats) (*Puller, error) {
	// Sanity Check
	exists, err := srcCS.Has(ctx, rootChunkHash)

	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, errors.New("not found")
	}

	exists, err = sinkCS.Has(ctx, rootChunkHash)

	if err != nil {
		return nil, err
	}

	if exists {
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
		rootChunkHash: rootChunkHash,
		downloaded:    hash.HashSet{},
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

type tempTblFile struct {
	id          string
	path        string
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
	fi, err := os.Stat(tmpTblFile.path)
	if err != nil {
		return err
	}

	fileSize := fi.Size()
	defer func() {
		_ = file.Remove(tmpTblFile.path)
	}()

	// By tracking the number of bytes uploaded here,
	// we can add bytes on to our bufferedSendBytes when
	// we have to retry a table file write.
	var localUploaded uint64
	return p.sinkDBCS.(nbs.TableFileStore).WriteTableFile(ctx, tmpTblFile.id, tmpTblFile.numChunks, tmpTblFile.contentHash, func() (io.ReadCloser, uint64, error) {
		f, err := os.Open(tmpTblFile.path)
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
		fWithStats := countingReader{countingReader{f, &localUploaded}, &p.stats.finishedSendBytes}

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

			path := filepath.Join(p.tempDir, id)
			err = tblFile.wr.FlushToFile(path)
			if err != nil {
				return err
			}

			ttf := tempTblFile{
				id:          id,
				path:        path,
				numChunks:   tblFile.wr.Size(),
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

	return p.sinkDBCS.(nbs.TableFileStore).AddTableFilesToManifest(ctx, fileIdToNumChunks)
}

// Pull executes the sync operation
func (p *Puller) Pull(ctx context.Context) error {
	if p.statsCh != nil {
		c := emitStats(p.stats, p.statsCh)
		defer c()
	}

	leaves := make(hash.HashSet)
	absent := make(hash.HashSet)
	absent.Insert(p.rootChunkHash)

	eg, ctx := errgroup.WithContext(ctx)

	completedTables := make(chan FilledWriters, 8)

	eg.Go(func() error {
		return p.processCompletedTables(ctx, completedTables)
	})

	eg.Go(func() error {
		if err := p.tablefileSema.Acquire(ctx, 1); err != nil {
			return err
		}
		for len(absent) > 0 {
			limitToNewChunks(absent, p.downloaded)

			var err error
			absent, err = p.sinkDBCS.HasMany(ctx, absent)
			if err != nil {
				return err
			}

			if len(absent) > 0 {
				leaves, absent, err = p.getCmp(ctx, leaves, absent, completedTables)
				if err != nil {
					return err
				}
			}
		}

		if p.wr != nil && p.wr.Size() > 0 {
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

func limitToNewChunks(absent hash.HashSet, downloaded hash.HashSet) {
	smaller := absent
	longer := downloaded
	if len(absent) > len(downloaded) {
		smaller = downloaded
		longer = absent
	}

	for k := range smaller {
		if longer.Has(k) {
			absent.Remove(k)
		}
	}
}

func (p *Puller) getCmp(ctx context.Context, leaves, batch hash.HashSet, completedTables chan FilledWriters) (hash.HashSet, hash.HashSet, error) {
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
				p.downloaded.Insert(cmpChnk.H)
				if leaves.Has(cmpChnk.H) {
					select {
					case processed <- CmpChnkAndRefs{cmpChnk: cmpChnk}:
					case <-ctx.Done():
						return ctx.Err()
					}
				} else {
					chnk, err := cmpChnk.ToChunk()
					if err != nil {
						return err
					}
					refs := make(map[hash.Hash]bool)
					err = p.waf(chnk, func(h hash.Hash, isleaf bool) error {
						refs[h] = isleaf
						return nil
					})
					if err != nil {
						return err
					}
					select {
					case processed <- CmpChnkAndRefs{cmpChnk: cmpChnk, refs: refs}:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		close(processed)
		return nil
	})

	batchSize := len(batch)
	nextLeaves := make(hash.HashSet, batchSize)
	nextLevel := make(hash.HashSet, batchSize)

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

				if p.wr.Size() >= p.chunksPerTF {
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

				for h, isleaf := range cmpAndRef.refs {
					nextLevel.Insert(h)
					if isleaf {
						nextLeaves.Insert(h)
					}
				}
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
		return nil, nil, err
	}
	return nextLeaves, nextLevel, nil
}
