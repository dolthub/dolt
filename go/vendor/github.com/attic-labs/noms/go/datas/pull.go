// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"math"
	"math/rand"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/golang/snappy"
)

type PullProgress struct {
	DoneCount, KnownCount, ApproxWrittenBytes uint64
}

const (
	bytesWrittenSampleRate = .10
	batchSize              = 1 << 12 // 4096 chunks
)

// Pull objects that descend from sourceRef from srcDB to sinkDB.
func Pull(srcDB, sinkDB Database, sourceRef types.Ref, progressCh chan PullProgress) {
	// Sanity Check
	d.PanicIfFalse(srcDB.chunkStore().Has(sourceRef.TargetHash()))

	if sinkDB.chunkStore().Has(sourceRef.TargetHash()) {
		return // already up to date
	}

	var doneCount, knownCount, approxBytesWritten uint64
	updateProgress := func(moreDone, moreKnown, moreApproxBytesWritten uint64) {
		if progressCh == nil {
			return
		}
		doneCount, knownCount, approxBytesWritten = doneCount+moreDone, knownCount+moreKnown, approxBytesWritten+moreApproxBytesWritten
		progressCh <- PullProgress{doneCount, knownCount, approxBytesWritten}
	}
	var sampleSize, sampleCount uint64

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

			// Concurrently pull all chunks from this batch that the sink is missing out of the source
			neededChunks := map[hash.Hash]*chunks.Chunk{}
			found := make(chan *chunks.Chunk)
			go func() { defer close(found); srcDB.chunkStore().GetMany(batch.HashSet(), found) }()
			for c := range found {
				neededChunks[c.Hash()] = c

				// Randomly sample amount of data written
				if rand.Float64() < bytesWrittenSampleRate {
					sampleSize += uint64(len(snappy.Encode(nil, c.Data())))
					sampleCount++
				}
				updateProgress(1, 0, sampleSize/uint64(math.Max(1, float64(sampleCount))))
			}

			// Now, put the absent chunks into the sink IN ORDER.
			// At the same time, gather up an ordered, uniquified list of all the children of the chunks in |batch| and add them to those in previous batches. This list is what we'll use to descend to the next level of the tree.
			for _, h := range batch {
				c := neededChunks[h]
				sinkDB.chunkStore().Put(*c)
				types.WalkRefs(*c, func(r types.Ref) {
					if !nextLevel.Has(r.TargetHash()) {
						uniqueOrdered = append(uniqueOrdered, r.TargetHash())
						nextLevel.Insert(r.TargetHash())
					}
				})
			}
		}

		// Ask sinkDB which of the next level's hashes it doesn't have.
		absentSet := sinkDB.chunkStore().HasMany(nextLevel)
		absent = absent[:0]
		for _, h := range uniqueOrdered {
			if absentSet.Has(h) {
				absent = append(absent, h)
			}
		}
	}

	persistChunks(sinkDB.chunkStore())
}
