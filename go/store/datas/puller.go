// Copyright 2019 Liquidata, Inc.
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

package datas

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/nbs"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type FileReaderWithSize struct {
	*os.File
	size int64
}

func (rd FileReaderWithSize) Size() int64 {
	return rd.size
}

// ErrDBUpToDate is the error code returned from NewPuller in the event that there is no work to do.
var ErrDBUpToDate = errors.New("the database does not need to be pulled as it's already up to date")

const (
	maxChunkWorkers = 2
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
	fmt *types.NomsBinFormat

	srcDB         Database
	sinkDB        Database
	rootChunkHash hash.Hash
	downloaded    hash.HashSet

	wr          *nbs.CmpChunkTableWriter
	tempDir     string
	chunksPerTF int

	eventCh chan PullerEvent
}

type PullerEventType int

const (
	NewLevelTWEvent PullerEventType = iota
	DestDBHasTWEvent
	LevelUpdateTWEvent
	LevelDoneTWEvent
	StartUploadTableFile
	EndUpdateTableFile
)

type TreeWalkEventDetails struct {
	TreeLevel           int
	ChunksInLevel       int
	ChunksAlreadyHad    int
	ChunksBuffered      int
	ChildrenFound       int
	TableFilesGenerated int
}

type TableFileEventDetails struct {
	TableFileCount     int
	TableFilesUploaded int
	CurrentFileSize    int64
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
func NewPuller(ctx context.Context, tempDir string, chunksPerTF int, srcDB, sinkDB Database, rootChunkHash hash.Hash, eventCh chan PullerEvent) (*Puller, error) {
	if eventCh == nil {
		panic("eventCh is required")
	}

	// Sanity Check
	exists, err := srcDB.chunkStore().Has(ctx, rootChunkHash)

	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, errors.New("not found")
	}

	exists, err = sinkDB.chunkStore().Has(ctx, rootChunkHash)

	if err != nil {
		return nil, err
	}

	if exists {
		return nil, ErrDBUpToDate
	}

	if srcDB.chunkStore().Version() != sinkDB.chunkStore().Version() {
		return nil, fmt.Errorf("cannot pull from src to sink; src version is %v and sink version is %v", srcDB.chunkStore().Version(), sinkDB.chunkStore().Version())
	}

	wr, err := nbs.NewCmpChunkTableWriter()

	if err != nil {
		return nil, err
	}

	return &Puller{
		fmt:           srcDB.Format(),
		srcDB:         srcDB,
		sinkDB:        sinkDB,
		rootChunkHash: rootChunkHash,
		downloaded:    hash.HashSet{},
		tempDir:       tempDir,
		wr:            wr,
		chunksPerTF:   chunksPerTF,
		eventCh:       eventCh,
	}, nil
}

func (p *Puller) processCompletedTables(ctx context.Context, ae *atomicerr.AtomicError, completedTables <-chan FilledWriters) {
	type tempTblFile struct {
		id        string
		path      string
		numChunks int
	}

	var tblFiles []tempTblFile

	var err error
	for tblFile := range completedTables {
		if err != nil {
			continue // drain
		}

		var id string
		id, err = tblFile.wr.Finish()

		if ae.SetIfError(err) {
			continue
		}

		path := filepath.Join(p.tempDir, id)
		err = tblFile.wr.FlushToFile(path)

		if ae.SetIfError(err) {
			continue
		}

		tblFiles = append(tblFiles, tempTblFile{id, path, tblFile.wr.Size()})
	}

	if ae.IsSet() {
		return
	}

	details := &TableFileEventDetails{TableFileCount: len(tblFiles)}

	// Write tables in reverse order so that on a partial success, it will still be true that if a db has a chunk, it
	// also has all of that chunks references.
	for i := len(tblFiles) - 1; i >= 0; i-- {
		tmpTblFile := tblFiles[i]

		fi, err := os.Stat(tmpTblFile.path)

		if ae.SetIfError(err) {
			return
		}

		f, err := os.Open(tmpTblFile.path)

		if ae.SetIfError(err) {
			return
		}

		details.CurrentFileSize = fi.Size()
		p.eventCh <- NewTFPullerEvent(StartUploadTableFile, details)

		fWithSize := FileReaderWithSize{f, fi.Size()}
		err = p.sinkDB.chunkStore().(nbs.TableFileStore).WriteTableFile(ctx, tmpTblFile.id, tmpTblFile.numChunks, fWithSize)

		go func() {
			_ = os.Remove(tmpTblFile.path)
		}()

		if ae.SetIfError(err) {
			return
		}

		details.TableFilesUploaded++
		p.eventCh <- NewTFPullerEvent(EndUpdateTableFile, details)
	}
}

// Pull executes the sync operation
func (p *Puller) Pull(ctx context.Context) error {
	twDetails := &TreeWalkEventDetails{TreeLevel: -1}

	leaves := make(hash.HashSet)
	absent := make(hash.HashSet)
	absent.Insert(p.rootChunkHash)

	ae := atomicerr.New()
	wg := &sync.WaitGroup{}
	completedTables := make(chan FilledWriters, 8)

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.processCompletedTables(ctx, ae, completedTables)
	}()

	for len(absent) > 0 {
		limitToNewChunks(absent, p.downloaded)

		chunksInLevel := len(absent)
		twDetails.ChunksInLevel = chunksInLevel
		p.eventCh <- NewTWPullerEvent(NewLevelTWEvent, twDetails)

		var err error
		absent, err = p.sinkDB.chunkStore().HasMany(ctx, absent)

		twDetails.ChunksAlreadyHad = chunksInLevel - len(absent)
		p.eventCh <- NewTWPullerEvent(DestDBHasTWEvent, twDetails)

		if len(absent) > 0 {
			leaves, absent, err = p.getCmp(ctx, twDetails, leaves, absent, completedTables)

			if err != nil {
				return err
			}
		}
	}

	if p.wr.Size() > 0 {
		completedTables <- FilledWriters{p.wr}
	}

	close(completedTables)

	wg.Wait()
	return ae.Get()
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
	found := make(chan chunks.Chunkable, 4096)
	processed := make(chan CmpChnkAndRefs, 4096)

	ae := atomicerr.New()
	go func() {
		defer close(found)
		err := p.srcDB.chunkStore().GetManyCompressed(ctx, batch, found)
		ae.SetIfError(err)
	}()

	batchSize := len(batch)
	numChunkWorkers := (batchSize / 1024) + 1
	if numChunkWorkers > maxChunkWorkers {
		numChunkWorkers = maxChunkWorkers
	}

	go func() {
		defer close(processed)
		for chable := range found {
			if ae.IsSet() {
				break
			}

			cmpChnk, ok := chable.(nbs.CompressedChunk)

			if !ok {
				ae.SetIfError(errors.New("requires an nbs.CompressedChunk"))
				break
			}

			p.downloaded.Insert(cmpChnk.H)

			if leaves.Has(cmpChnk.H) {
				processed <- CmpChnkAndRefs{cmpChnk: cmpChnk}
			} else {
				chnk, err := chable.ToChunk()

				if ae.SetIfError(err) {
					return
				}

				refs := make(map[hash.Hash]int)
				err = types.WalkRefs(chnk, p.fmt, func(r types.Ref) error {
					refs[r.TargetHash()] = int(r.Height())
					return nil
				})

				processed <- CmpChnkAndRefs{cmpChnk: cmpChnk, refs: refs}
			}
		}
	}()

	var err error
	var maxHeight int
	nextLeaves := make(hash.HashSet, batchSize)
	nextLevel := make(hash.HashSet, batchSize)

	twDetails.ChunksBuffered = 0
	for cmpAndRef := range processed {
		if err != nil {
			// drain to prevent deadlock
			continue
		}

		twDetails.ChunksBuffered++

		if twDetails.ChunksBuffered%1000 == 0 {
			p.eventCh <- NewTWPullerEvent(LevelUpdateTWEvent, twDetails)
		}

		err = p.wr.AddCmpChunk(cmpAndRef.cmpChnk)

		if p.wr.Size() >= p.chunksPerTF {
			completedTables <- FilledWriters{p.wr}
			p.wr, err = nbs.NewCmpChunkTableWriter()

			if ae.SetIfError(err) {
				continue
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
	}

	if err := ae.Get(); err != nil {
		return nil, nil, err
	}

	p.eventCh <- NewTWPullerEvent(LevelDoneTWEvent, twDetails)

	twDetails.TreeLevel = maxHeight
	return nextLeaves, nextLevel, nil
}
