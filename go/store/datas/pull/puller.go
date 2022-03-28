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
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
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
	refs    map[hash.Hash]int
}

// Puller is used to sync data between to Databases
type Puller struct {
	wrf WalkRefs

	srcChunkStore nbs.NBSCompressedChunkStore
	sinkDBCS      chunks.ChunkStore
	rootChunkHash hash.Hash
	downloaded    hash.HashSet

	wr            *nbs.CmpChunkTableWriter
	tablefileSema *semaphore.Weighted
	tempDir       string
	chunksPerTF   int

	eventCh chan PullerEvent
	pushLog *log.Logger
}

type PullerEventType int

const (
	NewLevelTWEvent PullerEventType = iota
	DestDBHasTWEvent
	LevelUpdateTWEvent
	LevelDoneTWEvent

	TableFileClosedEvent
	StartUploadTableFileEvent
	UploadTableFileUpdateEvent
	EndUploadTableFileEvent
)

type TreeWalkEventDetails struct {
	TreeLevel        int
	ChunksInLevel    int
	ChunksAlreadyHad int
	ChunksBuffered   int
	ChildrenFound    int
}

type TableFileEventDetails struct {
	CurrentFileSize int64
	Stats           iohelp.ReadStats
}

type PullerEvent struct {
	EventType      PullerEventType
	TWEventDetails TreeWalkEventDetails
	TFEventDetails TableFileEventDetails
}

func NewTWPullerEvent(et PullerEventType, details *TreeWalkEventDetails) PullerEvent {
	return PullerEvent{EventType: et, TWEventDetails: *details}
}

func NewTFPullerEvent(et PullerEventType, details *TableFileEventDetails) PullerEvent {
	return PullerEvent{EventType: et, TFEventDetails: *details}
}

// NewPuller creates a new Puller instance to do the syncing.  If a nil puller is returned without error that means
// that there is nothing to pull and the sinkDB is already up to date.
func NewPuller(ctx context.Context, tempDir string, chunksPerTF int, srcCS, sinkCS chunks.ChunkStore, walkRefs WalkRefs, rootChunkHash hash.Hash, eventCh chan PullerEvent) (*Puller, error) {
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
		wrf:           walkRefs,
		srcChunkStore: srcChunkStore,
		sinkDBCS:      sinkCS,
		rootChunkHash: rootChunkHash,
		downloaded:    hash.HashSet{},
		tablefileSema: semaphore.NewWeighted(outstandingTableFiles),
		tempDir:       tempDir,
		wr:            wr,
		chunksPerTF:   chunksPerTF,
		eventCh:       eventCh,
		pushLog:       pushLogger,
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
	contentLen  uint64
	contentHash []byte
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

	return p.sinkDBCS.(nbs.TableFileStore).WriteTableFile(ctx, tmpTblFile.id, tmpTblFile.numChunks, tmpTblFile.contentHash, func() (io.ReadCloser, uint64, error) {
		f, err := os.Open(tmpTblFile.path)
		if err != nil {
			return nil, 0, err
		}

		fWithStats := iohelp.NewReaderWithStats(f, fileSize)
		fWithStats.Start(func(stats iohelp.ReadStats) {
			p.addEvent(ctx, NewTFPullerEvent(UploadTableFileUpdateEvent, &TableFileEventDetails{
				CurrentFileSize: fileSize,
				Stats:           stats,
			}))
		})

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
			if err := p.addEvent(ctx, NewTFPullerEvent(StartUploadTableFileEvent, &TableFileEventDetails{
				CurrentFileSize: int64(tblFile.wr.ContentLength()),
			})); err != nil {
				return err
			}

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
				contentLen:  tblFile.wr.ContentLength(),
				contentHash: tblFile.wr.GetMD5(),
			}
			err = p.uploadTempTableFile(ctx, ttf)
			if err != nil {
				return err
			}

			if err := p.addEvent(ctx, NewTFPullerEvent(EndUploadTableFileEvent, &TableFileEventDetails{
				CurrentFileSize: int64(ttf.contentLen),
			})); err != nil {
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
	twDetails := &TreeWalkEventDetails{TreeLevel: -1}

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

			chunksInLevel := len(absent)
			twDetails.ChunksInLevel = chunksInLevel
			if err := p.addEvent(ctx, NewTWPullerEvent(NewLevelTWEvent, twDetails)); err != nil {
				return err
			}

			var err error
			absent, err = p.sinkDBCS.HasMany(ctx, absent)
			if err != nil {
				return err
			}

			twDetails.ChunksAlreadyHad = chunksInLevel - len(absent)
			if err := p.addEvent(ctx, NewTWPullerEvent(DestDBHasTWEvent, twDetails)); err != nil {
				return err
			}

			if len(absent) > 0 {
				leaves, absent, err = p.getCmp(ctx, twDetails, leaves, absent, completedTables)
				if err != nil {
					return err
				}
			}
		}

		if p.wr != nil {
			if err := p.addEvent(ctx, NewTFPullerEvent(TableFileClosedEvent, &TableFileEventDetails{
				CurrentFileSize: int64(p.wr.ContentLength()),
			})); err != nil {
				return err
			}
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

func (p *Puller) getCmp(ctx context.Context, twDetails *TreeWalkEventDetails, leaves, batch hash.HashSet, completedTables chan FilledWriters) (hash.HashSet, hash.HashSet, error) {
	found := make(chan nbs.CompressedChunk, 4096)
	processed := make(chan CmpChnkAndRefs, 4096)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		err := p.srcChunkStore.GetManyCompressed(ctx, batch, func(ctx context.Context, c nbs.CompressedChunk) {
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
					refs := make(map[hash.Hash]int)
					err = p.wrf(chnk, func(h hash.Hash, height uint64) error {
						refs[h] = int(height)
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

	var maxHeight int
	batchSize := len(batch)
	nextLeaves := make(hash.HashSet, batchSize)
	nextLevel := make(hash.HashSet, batchSize)

	eg.Go(func() error {
		twDetails.ChunksBuffered = 0
LOOP:
		for {
			select {
			case cmpAndRef, ok := <-processed:
				if !ok {
					break LOOP
				}
				twDetails.ChunksBuffered++

				if twDetails.ChunksBuffered%100 == 0 {
					if err := p.addEvent(ctx, NewTWPullerEvent(LevelUpdateTWEvent, twDetails)); err != nil {
						return err
					}
				}

				err := p.wr.AddCmpChunk(cmpAndRef.cmpChnk)
				if err != nil {
					return err
				}

				if p.wr.Size() >= p.chunksPerTF {
					if err := p.addEvent(ctx, NewTFPullerEvent(TableFileClosedEvent, &TableFileEventDetails{
						CurrentFileSize: int64(p.wr.ContentLength()),
					})); err != nil {
						return err
					}

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

				for h, height := range cmpAndRef.refs {
					nextLevel.Insert(h)
					twDetails.ChildrenFound++

					if height == 1 {
						nextLeaves.Insert(h)
					}

					if height > maxHeight {
						maxHeight = height
					}
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if twDetails.ChunksBuffered != len(batch) {
			return errors.New("failed to get all chunks.")
		}
		if err := p.addEvent(ctx, NewTWPullerEvent(LevelDoneTWEvent, twDetails)); err != nil {
			return err
		}
		twDetails.TreeLevel = maxHeight
		return nil
	})

	err := eg.Wait()
	if err != nil {
		return nil, nil, err
	}
	return nextLeaves, nextLevel, nil
}

func (p *Puller) addEvent(ctx context.Context, evt PullerEvent) error {
	if p.eventCh != nil {
		select {
		case p.eventCh <- evt:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
