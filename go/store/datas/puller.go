package datas

import (
	"context"
	"errors"
	"fmt"
	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/nbs"
	"github.com/liquidata-inc/dolt/go/store/types"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	maxChunkWorkers = 2
)

type FilledWriters struct {
	wr *nbs.CmpChunkTableWriter
}

type CmpChnkAndRefs struct {
	cmpChnk nbs.CompressedChunk
	refs map[hash.Hash]bool
}

type Puller struct {
	fmt *types.NomsBinFormat

	srcDB         Database
	sinkDB        Database
	rootChunkHash hash.Hash

	wr	    	*nbs.CmpChunkTableWriter
	tempDir 	string
	chunksPerTF int
}

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

	return &Puller {
		fmt: srcDB.Format(),
		srcDB: srcDB,
		sinkDB: sinkDB,
		rootChunkHash: rootChunkHash,
		tempDir: tempDir,
		wr: wr,
		chunksPerTF: chunksPerTF,
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
		chSink, err := p.sinkDB.chunkStore().(*nbs.NomsBlockStore).NewSink(ctx, tmpTblFile.id, tmpTblFile.numChunks)

		if ae.SetIfError(err) {
			return
		}

		f, err := os.Open(tmpTblFile.path)

		if ae.SetIfError(err) {
			return
		}

		_, err = io.Copy(chSink, f)

		if ae.SetIfError(err) {
			return
		}

		err = chSink.Close(ctx)

		if ae.SetIfError(err) {
			return
		}
	}
}

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
		leaves, absent, err = p.getCmp(ctx, leaves, absent, completedTables)

		if err != nil {
			return err
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

	wg := &sync.WaitGroup{}
	once := &sync.Once{}
	for i := 0; i < numChunkWorkers; i++ {
		wg.Add(1)
		go func() {
			defer func(){
				wg.Done()
				wg.Wait()
				once.Do(func() {
					close(processed)
				})
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

				if leaves.Has(cmpChnk.H) {
					processed <- CmpChnkAndRefs{cmpChnk:cmpChnk}
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

					processed <- CmpChnkAndRefs{cmpChnk:cmpChnk, refs:refs}
				}
			}
		}()
	}

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