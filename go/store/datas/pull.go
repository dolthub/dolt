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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sync"

	"github.com/cenkalti/backoff"
	"github.com/golang/snappy"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

type PullProgress struct {
	DoneCount, KnownCount, ApproxWrittenBytes uint64
}

const (
	bytesWrittenSampleRate = .10
	defaultBatchSize       = 1 << 12 // 4096 chunks
)

var ErrNoData = errors.New("no data")

func makeProgTrack(progressCh chan PullProgress) func(moreDone, moreKnown, moreApproxBytesWritten uint64) {
	var doneCount, knownCount, approxBytesWritten uint64
	return func(moreDone, moreKnown, moreApproxBytesWritten uint64) {
		if progressCh == nil {
			return
		}
		doneCount, knownCount, approxBytesWritten = doneCount+moreDone, knownCount+moreKnown, approxBytesWritten+moreApproxBytesWritten
		progressCh <- PullProgress{doneCount, knownCount, approxBytesWritten}
	}
}

func Clone(ctx context.Context, srcDB, sinkDB Database, eventCh chan<- TableFileEvent) error {

	srcCS := srcDB.chunkStore().(interface{})
	sinkCS := sinkDB.chunkStore().(interface{})

	srcTS, srcOK := srcCS.(nbs.TableFileStore)

	if !srcOK {
		return errors.New("src db is not a Table File Store")
	}

	size, err := srcTS.Size(ctx)

	if err != nil {
		return err
	}

	if size == 0 {
		return ErrNoData
	}

	sinkTS, sinkOK := sinkCS.(nbs.TableFileStore)

	if !sinkOK {
		return errors.New("sink db is not a Table File Store")
	}

	return clone(ctx, srcTS, sinkTS, eventCh)
}

type CloneTableFileEvent int

const (
	Listed = iota
	DownloadStart
	DownloadSuccess
	DownloadFailed
)

type TableFileEvent struct {
	EventType  CloneTableFileEvent
	TableFiles []nbs.TableFile
}

// mapTableFiles returns the list of all fileIDs for the table files, and a map from fileID to nbs.TableFile
func mapTableFiles(tblFiles []nbs.TableFile) ([]string, map[string]nbs.TableFile) {
	fileIds := make([]string, len(tblFiles))
	fileIDtoTblFile := make(map[string]nbs.TableFile)

	for i, tblFile := range tblFiles {
		fileIDtoTblFile[tblFile.FileID()] = tblFile
		fileIds[i] = tblFile.FileID()
	}

	return fileIds, fileIDtoTblFile
}

func CloseWithErr(c io.Closer, err *error) {
	e := c.Close()
	if *err == nil && e != nil {
		*err = e
	}
}

const concurrentTableFileDownloads = 3

func clone(ctx context.Context, srcTS, sinkTS nbs.TableFileStore, eventCh chan<- TableFileEvent) error {
	root, tblFiles, err := srcTS.Sources(ctx)
	if err != nil {
		return err
	}

	report := func(e TableFileEvent) {
		if eventCh != nil {
			eventCh <- e
		}
	}

	// Initializes the list of fileIDs we are going to download, and the map of fileIDToTF.  If this clone takes a long
	// time some of the urls within the nbs.TableFiles will expire and fail to download.  At that point we will retrieve
	// the sources again, and update the fileIDToTF map with updated info, but not change the files we are downloading.
	desiredFiles, fileIDToTF := mapTableFiles(tblFiles)
	completed := make([]bool, len(desiredFiles))

	report(TableFileEvent{Listed, tblFiles})

	download := func(ctx context.Context) error {
		sem := semaphore.NewWeighted(concurrentTableFileDownloads)
		eg, ctx := errgroup.WithContext(ctx)
		for i := 0; i < len(desiredFiles); i++ {
			if completed[i] {
				continue
			}
			if err := sem.Acquire(ctx, 1); err != nil {
				// The errgroup ctx has been canceled. We will
				// return the error from wg.Wait() below.
				break
			}
			idx := i
			eg.Go(func() (err error) {
				defer sem.Release(1)

				fileID := desiredFiles[idx]
				tblFile, ok := fileIDToTF[fileID]
				if !ok {
					// conjoin happened during clone
					return backoff.Permanent(errors.New("table file not found. please try again"))
				}

				var rd io.ReadCloser
				if rd, err = tblFile.Open(ctx); err != nil {
					return err
				}
				defer CloseWithErr(rd, &err)

				report(TableFileEvent{DownloadStart, []nbs.TableFile{tblFile}})
				err = sinkTS.WriteTableFile(ctx, tblFile.FileID(), tblFile.NumChunks(), rd, 0, nil)
				if err != nil {
					report(TableFileEvent{DownloadFailed, []nbs.TableFile{tblFile}})
					return err
				}

				report(TableFileEvent{DownloadSuccess, []nbs.TableFile{tblFile}})
				completed[idx] = true
				return nil
			})
		}

		return eg.Wait()
	}

	const maxAttempts = 3
	previousCompletedCnt := 0
	failureCount := 0

	madeProgress := func() bool {
		currentCompletedCnt := 0
		for _, b := range completed {
			if b {
				currentCompletedCnt++
			}
		}
		if currentCompletedCnt == previousCompletedCnt {
			return false
		} else {
			previousCompletedCnt = currentCompletedCnt
			return true
		}
	}

	// keep going as long as progress is being made.  If progress is not made retry up to maxAttempts times.
	for {
		err = download(ctx)
		if err == nil {
			break
		}
		if permanent, ok := err.(*backoff.PermanentError); ok {
			return permanent.Err
		} else if madeProgress() {
			failureCount = 0
		} else {
			failureCount++
		}
		if failureCount >= maxAttempts {
			return err
		}
		if _, tblFiles, err = srcTS.Sources(ctx); err != nil {
			return err
		} else {
			_, fileIDToTF = mapTableFiles(tblFiles)
		}
	}

	return sinkTS.SetRootChunk(ctx, root, hash.Hash{})
}

// Pull objects that descend from sourceRef from srcDB to sinkDB.
func Pull(ctx context.Context, srcDB, sinkDB Database, sourceRef types.Ref, progressCh chan PullProgress) error {
	return pull(ctx, srcDB, sinkDB, sourceRef, progressCh, defaultBatchSize)
}

func pull(ctx context.Context, srcDB, sinkDB Database, sourceRef types.Ref, progressCh chan PullProgress, batchSize int) error {
	// Sanity Check
	exists, err := srcDB.chunkStore().Has(ctx, sourceRef.TargetHash())

	if err != nil {
		return err
	}

	if !exists {
		return errors.New("not found")
	}

	exists, err = sinkDB.chunkStore().Has(ctx, sourceRef.TargetHash())

	if err != nil {
		return err
	}

	if exists {
		return nil // already up to date
	}

	if srcDB.chunkStore().Version() != sinkDB.chunkStore().Version() {
		return fmt.Errorf("cannot pull from src to sink; src version is %v and sink version is %v", srcDB.chunkStore().Version(), sinkDB.chunkStore().Version())
	}

	var sampleSize, sampleCount uint64
	updateProgress := makeProgTrack(progressCh)

	// TODO: This batches based on limiting the _number_ of chunks processed at the same time. We really want to batch based on the _amount_ of chunk data being processed simultaneously. We also want to consider the chunks in a particular order, however, and the current GetMany() interface doesn't provide any ordering guarantees. Once BUG 3750 is fixed, we should be able to revisit this and do a better job.
	absent := hash.HashSlice{sourceRef.TargetHash()}
	for absentCount := len(absent); absentCount != 0; absentCount = len(absent) {
		updateProgress(0, uint64(absentCount), 0)

		// For gathering up the hashes in the next level of the tree
		nextLevel := hash.HashSet{}
		uniqueOrdered := hash.HashSlice{}

		// Process all absent chunks in this level of the tree in quanta of at most |batchSize|
		for start, end := 0, batchSize; start < absentCount; start, end = end, end+batchSize {
			if end > absentCount {
				end = absentCount
			}
			batch := absent[start:end]

			neededChunks, err := getChunks(ctx, srcDB, batch, sampleSize, sampleCount, updateProgress)

			if err != nil {
				return err
			}

			uniqueOrdered, err = putChunks(ctx, sinkDB, batch, neededChunks, nextLevel, uniqueOrdered)

			if err != nil {
				return err
			}
		}

		absent, err = nextLevelMissingChunks(ctx, sinkDB, nextLevel, absent, uniqueOrdered)

		if err != nil {
			return err
		}
	}

	err = persistChunks(ctx, sinkDB.chunkStore())

	if err != nil {
		return err
	}

	return nil
}

func persistChunks(ctx context.Context, cs chunks.ChunkStore) error {
	// todo: there is no call to rebase on an unsuccessful Commit()
	// will  this loop forever?
	var success bool
	for !success {
		r, err := cs.Root(ctx)

		if err != nil {
			return err
		}

		success, err = cs.Commit(ctx, r, r)

		if err != nil {
			return err
		}
	}

	return nil
}

// PullWithoutBatching effectively removes the batching of chunk retrieval done on each level of the tree.  This means
// all chunks from one level of the tree will be retrieved from the underlying chunk store in one call, which pushes the
// optimization problem down to the chunk store which can make smarter decisions.
func PullWithoutBatching(ctx context.Context, srcDB, sinkDB Database, sourceRef types.Ref, progressCh chan PullProgress) error {
	// by increasing the batch size to MaxInt32 we effectively remove batching here.
	return pull(ctx, srcDB, sinkDB, sourceRef, progressCh, math.MaxInt32)
}

// concurrently pull all chunks from this batch that the sink is missing out of the source
func getChunks(ctx context.Context, srcDB Database, batch hash.HashSlice, sampleSize uint64, sampleCount uint64, updateProgress func(moreDone uint64, moreKnown uint64, moreApproxBytesWritten uint64)) (map[hash.Hash]*chunks.Chunk, error) {
	mu := &sync.Mutex{}
	neededChunks := map[hash.Hash]*chunks.Chunk{}
	err := srcDB.chunkStore().GetMany(ctx, batch.HashSet(), func(c *chunks.Chunk) {
		mu.Lock()
		defer mu.Unlock()
		neededChunks[c.Hash()] = c

		// Randomly sample amount of data written
		if rand.Float64() < bytesWrittenSampleRate {
			sampleSize += uint64(len(snappy.Encode(nil, c.Data())))
			sampleCount++
		}
		updateProgress(1, 0, sampleSize/uint64(math.Max(1, float64(sampleCount))))
	})
	if err != nil {
		return nil, err
	}
	return neededChunks, nil
}

// put the chunks that were downloaded into the sink IN ORDER and at the same time gather up an ordered, uniquified list
// of all the children of the chunks and add them to the list of the next level tree chunks.
func putChunks(ctx context.Context, sinkDB Database, hashes hash.HashSlice, neededChunks map[hash.Hash]*chunks.Chunk, nextLevel hash.HashSet, uniqueOrdered hash.HashSlice) (hash.HashSlice, error) {
	for _, h := range hashes {
		c := neededChunks[h]
		err := sinkDB.chunkStore().Put(ctx, *c)

		if err != nil {
			return hash.HashSlice{}, err
		}

		err = types.WalkRefs(*c, sinkDB.Format(), func(r types.Ref) error {
			if !nextLevel.Has(r.TargetHash()) {
				uniqueOrdered = append(uniqueOrdered, r.TargetHash())
				nextLevel.Insert(r.TargetHash())
			}

			return nil
		})

		if err != nil {
			return hash.HashSlice{}, err
		}
	}

	return uniqueOrdered, nil
}

// ask sinkDB which of the next level's hashes it doesn't have, and add those chunks to the absent list which will need
// to be retrieved.
func nextLevelMissingChunks(ctx context.Context, sinkDB Database, nextLevel hash.HashSet, absent hash.HashSlice, uniqueOrdered hash.HashSlice) (hash.HashSlice, error) {
	missingFromSink, err := sinkDB.chunkStore().HasMany(ctx, nextLevel)

	if err != nil {
		return hash.HashSlice{}, err
	}

	absent = absent[:0]
	for _, h := range uniqueOrdered {
		if missingFromSink.Has(h) {
			absent = append(absent, h)
		}
	}

	return absent, nil
}
