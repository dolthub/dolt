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
	refs    map[hash.Hash]bool
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
}

// NewPuller creates a new Puller instance to do the syncing.  If a nil puller is returned without error that means
// that there is nothing to pull and the sinkDB is already up to date.
func NewPuller(ctx context.Context, tempDir string, chunksPerTF int, srcDB, sinkDB Database, rootChunkHash hash.Hash) (*Puller, error) {
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
		return nil, nil // already up to date
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

	for i := len(tblFiles) - 1; i >= 0; i-- {
		tmpTblFile := tblFiles[i]

		f, err := os.Open(tmpTblFile.path)

		if ae.SetIfError(err) {
			return
		}

		err = p.sinkDB.chunkStore().(nbs.TableFileStore).WriteTableFile(ctx, tmpTblFile.id, tmpTblFile.numChunks, f)

		if ae.SetIfError(err) {
			return
		}
	}
}

// Pull executes the sync operation
func (p *Puller) Pull(ctx context.Context) error {
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
		var err error
		absent, err = p.sinkDB.chunkStore().HasMany(ctx, absent)

		if len(absent) > 0 {
			leaves, absent, err = p.getCmp(ctx, leaves, absent, completedTables)

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

func (p *Puller) getCmp(ctx context.Context, leaves, batch hash.HashSet, completedTables chan FilledWriters) (hash.HashSet, hash.HashSet, error) {
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
		defer func() {
			close(processed)
		}()

		for chable := range found {
			if ae.IsSet() {
				break
			}

			cmpChnk, ok := chable.(nbs.CompressedChunk)

			if !ok {
				ae.SetIfError(errors.New("requires an nbs.CompressedChunk"))
				break
			}

			if p.downloaded.Has(cmpChnk.H) {
				continue
			} else {
				p.downloaded.Insert(cmpChnk.H)
			}

			if leaves.Has(cmpChnk.H) {
				processed <- CmpChnkAndRefs{cmpChnk: cmpChnk}
			} else {
				chnk, err := chable.ToChunk()

				if ae.SetIfError(err) {
					return
				}

				refs := make(map[hash.Hash]bool)
				err = types.WalkRefs(chnk, p.fmt, func(r types.Ref) error {
					refs[r.TargetHash()] = r.Height() == 1
					return nil
				})

				processed <- CmpChnkAndRefs{cmpChnk: cmpChnk, refs: refs}
			}
		}
	}()

	var err error
	nextLeaves := make(hash.HashSet, batchSize)
	nextLevel := make(hash.HashSet, batchSize)
	for cmpAndRef := range processed {
		if err != nil {
			// drain to prevent deadlock
			continue
		}

		err = p.wr.AddCmpChunk(cmpAndRef.cmpChnk)

		if p.wr.Size() >= p.chunksPerTF {
			completedTables <- FilledWriters{p.wr}
			p.wr, err = nbs.NewCmpChunkTableWriter()

			if ae.SetIfError(err) {
				continue
			}
		}

		for h, isLeaf := range cmpAndRef.refs {
			nextLevel.Insert(h)

			if isLeaf {
				nextLeaves.Insert(h)
			}
		}
	}

	if err := ae.Get(); err != nil {
		return nil, nil, err
	}

	return nextLeaves, nextLevel, nil
}
