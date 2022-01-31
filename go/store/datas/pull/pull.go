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

package pull

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/golang/snappy"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
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

// Pull objects that descend from sourceHash from srcDB to sinkDB.
func Pull(ctx context.Context, srcCS, sinkCS chunks.ChunkStore, walkRefs WalkRefs, sourceHash hash.Hash, progressCh chan PullProgress) error {
	return pull(ctx, srcCS, sinkCS, walkRefs, sourceHash, progressCh, defaultBatchSize)
}

func pull(ctx context.Context, srcCS, sinkCS chunks.ChunkStore, walkRefs WalkRefs, sourceHash hash.Hash, progressCh chan PullProgress, batchSize int) error {
	// Sanity Check
	exists, err := srcCS.Has(ctx, sourceHash)

	if err != nil {
		return err
	}

	if !exists {
		return errors.New("not found")
	}

	exists, err = sinkCS.Has(ctx, sourceHash)

	if err != nil {
		return err
	}

	if exists {
		return nil // already up to date
	}

	if srcCS.Version() != sinkCS.Version() {
		return fmt.Errorf("cannot pull from src to sink; src version is %v and sink version is %v", srcCS.Version(), sinkCS.Version())
	}

	var sampleSize, sampleCount uint64
	updateProgress := makeProgTrack(progressCh)

	// TODO: This batches based on limiting the _number_ of chunks processed at the same time. We really want to batch based on the _amount_ of chunk data being processed simultaneously. We also want to consider the chunks in a particular order, however, and the current GetMany() interface doesn't provide any ordering guarantees. Once BUG 3750 is fixed, we should be able to revisit this and do a better job.
	absent := hash.HashSlice{sourceHash}
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

			neededChunks, err := getChunks(ctx, srcCS, batch, sampleSize, sampleCount, updateProgress)

			if err != nil {
				return err
			}

			uniqueOrdered, err = putChunks(ctx, walkRefs, sinkCS, batch, neededChunks, nextLevel, uniqueOrdered)

			if err != nil {
				return err
			}
		}

		absent, err = nextLevelMissingChunks(ctx, sinkCS, nextLevel, absent, uniqueOrdered)

		if err != nil {
			return err
		}
	}

	err = persistChunks(ctx, sinkCS)

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
func PullWithoutBatching(ctx context.Context, srcCS, sinkCS chunks.ChunkStore, walkRefs WalkRefs, sourceHash hash.Hash, progressCh chan PullProgress) error {
	// by increasing the batch size to MaxInt32 we effectively remove batching here.
	return pull(ctx, srcCS, sinkCS, walkRefs, sourceHash, progressCh, math.MaxInt32)
}

// concurrently pull all chunks from this batch that the sink is missing out of the source
func getChunks(ctx context.Context, srcCS chunks.ChunkStore, batch hash.HashSlice, sampleSize uint64, sampleCount uint64, updateProgress func(moreDone uint64, moreKnown uint64, moreApproxBytesWritten uint64)) (map[hash.Hash]*chunks.Chunk, error) {
	mu := &sync.Mutex{}
	neededChunks := map[hash.Hash]*chunks.Chunk{}
	err := srcCS.GetMany(ctx, batch.HashSet(), func(ctx context.Context, c *chunks.Chunk) {
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

type WalkRefs func(chunks.Chunk, func(hash.Hash, uint64) error) error

// put the chunks that were downloaded into the sink IN ORDER and at the same time gather up an ordered, uniquified list
// of all the children of the chunks and add them to the list of the next level tree chunks.
func putChunks(ctx context.Context, wrh WalkRefs, sinkCS chunks.ChunkStore, hashes hash.HashSlice, neededChunks map[hash.Hash]*chunks.Chunk, nextLevel hash.HashSet, uniqueOrdered hash.HashSlice) (hash.HashSlice, error) {
	for _, h := range hashes {
		c := neededChunks[h]
		err := sinkCS.Put(ctx, *c)

		if err != nil {
			return hash.HashSlice{}, err
		}

		err = wrh(*c, func(h hash.Hash, height uint64) error {
			if !nextLevel.Has(h) {
				uniqueOrdered = append(uniqueOrdered, h)
				nextLevel.Insert(h)
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
func nextLevelMissingChunks(ctx context.Context, sinkCS chunks.ChunkStore, nextLevel hash.HashSet, absent hash.HashSlice, uniqueOrdered hash.HashSlice) (hash.HashSlice, error) {
	missingFromSink, err := sinkCS.HasMany(ctx, nextLevel)

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
