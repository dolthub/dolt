// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"
	"math"
	"math/rand"

	"github.com/golang/snappy"
	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

type PullProgress struct {
	DoneCount, KnownCount, ApproxWrittenBytes uint64
}

const (
	bytesWrittenSampleRate = .10
	defaultBatchSize       = 1 << 12 // 4096 chunks
)

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

// Pull objects that descend from sourceRef from srcDB to sinkDB.
func Pull(ctx context.Context, srcDB, sinkDB Database, sourceRef types.Ref, progressCh chan PullProgress) {
	pull(ctx, srcDB, sinkDB, sourceRef, progressCh, defaultBatchSize)
}

func pull(ctx context.Context, srcDB, sinkDB Database, sourceRef types.Ref, progressCh chan PullProgress, batchSize int) {
	// Sanity Check
	exists, err := srcDB.chunkStore().Has(ctx, sourceRef.TargetHash())

	// TODO: fix panics
	d.PanicIfError(err)
	d.PanicIfFalse(exists)

	exists, err = sinkDB.chunkStore().Has(ctx, sourceRef.TargetHash())

	// TODO: fix panics
	d.PanicIfError(err)

	if exists {
		return // already up to date
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

			neededChunks := getChunks(ctx, srcDB, batch, sampleSize, sampleCount, updateProgress)
			uniqueOrdered = putChunks(ctx, sinkDB, batch, neededChunks, nextLevel, uniqueOrdered)
		}

		absent = nextLevelMissingChunks(ctx, sinkDB, nextLevel, absent, uniqueOrdered)
	}

	persistChunks(ctx, sinkDB.chunkStore())
}

func persistChunks(ctx context.Context, cs chunks.ChunkStore) {
	var success bool
	for !success {
		r, err := cs.Root(ctx)

		//TODO: fix panics
		d.PanicIfError(err)

		success, err = cs.Commit(ctx, r, r)

		// TODO: fix panics
		d.PanicIfError(err)
	}
}

// PullWithoutBatching effectively removes the batching of chunk retrieval done on each level of the tree.  This means
// all chunks from one level of the tree will be retrieved from the underlying chunk store in one call, which pushes the
// optimization problem down to the chunk store which can make smarter decisions.
func PullWithoutBatching(ctx context.Context, srcDB, sinkDB Database, sourceRef types.Ref, progressCh chan PullProgress) {
	// by increasing the batch size to MaxInt32 we effectively remove batching here.
	pull(ctx, srcDB, sinkDB, sourceRef, progressCh, math.MaxInt32)
}

// concurrently pull all chunks from this batch that the sink is missing out of the source
func getChunks(ctx context.Context, srcDB Database, batch hash.HashSlice, sampleSize uint64, sampleCount uint64, updateProgress func(moreDone uint64, moreKnown uint64, moreApproxBytesWritten uint64)) map[hash.Hash]*chunks.Chunk {
	neededChunks := map[hash.Hash]*chunks.Chunk{}
	found := make(chan *chunks.Chunk)

	go func() {
		defer close(found)
		err := srcDB.chunkStore().GetMany(ctx, batch.HashSet(), found)

		// TODO: fix panics
		d.PanicIfError(err)
	}()

	for c := range found {
		neededChunks[c.Hash()] = c

		// Randomly sample amount of data written
		if rand.Float64() < bytesWrittenSampleRate {
			sampleSize += uint64(len(snappy.Encode(nil, c.Data())))
			sampleCount++
		}
		updateProgress(1, 0, sampleSize/uint64(math.Max(1, float64(sampleCount))))
	}
	return neededChunks
}

// put the chunks that were downloaded into the sink IN ORDER and at the same time gather up an ordered, uniquified list
// of all the children of the chunks and add them to the list of the next level tree chunks.
func putChunks(ctx context.Context, sinkDB Database, hashes hash.HashSlice, neededChunks map[hash.Hash]*chunks.Chunk, nextLevel hash.HashSet, uniqueOrdered hash.HashSlice) hash.HashSlice {
	for _, h := range hashes {
		c := neededChunks[h]
		err := sinkDB.chunkStore().Put(ctx, *c)

		// TODO: fix panics
		d.PanicIfError(err)

		// TODO(binformat)
		types.WalkRefs(*c, types.Format_7_18, func(r types.Ref) {
			if !nextLevel.Has(r.TargetHash()) {
				uniqueOrdered = append(uniqueOrdered, r.TargetHash())
				nextLevel.Insert(r.TargetHash())
			}
		})
	}

	return uniqueOrdered
}

// ask sinkDB which of the next level's hashes it doesn't have, and add those chunks to the absent list which will need
// to be retrieved.
func nextLevelMissingChunks(ctx context.Context, sinkDB Database, nextLevel hash.HashSet, absent hash.HashSlice, uniqueOrdered hash.HashSlice) hash.HashSlice {
	missingFromSink, err := sinkDB.chunkStore().HasMany(ctx, nextLevel)

	// TODO: fix panics
	d.PanicIfError(err)

	absent = absent[:0]
	for _, h := range uniqueOrdered {
		if missingFromSink.Has(h) {
			absent = append(absent, h)
		}
	}

	return absent
}
