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

const bytesWrittenSampleRate = .10

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

	absent := hash.HashSlice{sourceRef.TargetHash()}
	for len(absent) != 0 {
		updateProgress(0, uint64(len(absent)), 0)

		// Concurrently pull all the chunks the sink is missing out of the source
		neededChunks := map[hash.Hash]*chunks.Chunk{}
		found := make(chan *chunks.Chunk)
		go func() { defer close(found); srcDB.chunkStore().GetMany(absent.HashSet(), found) }()
		for c := range found {
			neededChunks[c.Hash()] = c

			// Randomly sample amount of data written
			if rand.Float64() < bytesWrittenSampleRate {
				sampleSize += uint64(len(snappy.Encode(nil, c.Data())))
				sampleCount++
			}
			updateProgress(1, 0, sampleSize/uint64(math.Max(1, float64(sampleCount))))
		}

		// Now, put the absent chunks into the sink IN ORDER, meanwhile decoding each into a value so we can iterate all its refs.
		// Descend to the next level of the tree by gathering up an ordered, uniquified list of all the children of the chunks in |absent|.
		nextLevel := hash.HashSet{}
		uniqueOrdered := hash.HashSlice{}
		for _, h := range absent {
			c := neededChunks[h]
			sinkDB.chunkStore().Put(*c)
			types.DecodeValue(*c, srcDB).WalkRefs(func(r types.Ref) {
				if !nextLevel.Has(r.TargetHash()) {
					uniqueOrdered = append(uniqueOrdered, r.TargetHash())
					nextLevel.Insert(r.TargetHash())
				}
			})
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
