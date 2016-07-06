// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
)

const batchSize = 100

type ValidatingBatchingSink struct {
	vs    *ValueStore
	cs    chunks.ChunkStore
	batch [batchSize]chunks.Chunk
	count int
	tc    *TypeCache
}

func NewValidatingBatchingSink(cs chunks.ChunkStore, tc *TypeCache) *ValidatingBatchingSink {
	return &ValidatingBatchingSink{vs: newLocalValueStore(cs), cs: cs, tc: tc}
}

// Prepare primes the type info cache used to validate Enqueued Chunks by reading the Chunks referenced by the provided hints.
func (vbs *ValidatingBatchingSink) Prepare(hints Hints) {
	for hint := range hints {
		vbs.vs.ReadValue(hint)
	}
}

// Enequeue adds a Chunk to the queue of Chunks waiting to be Put into vbs' backing ChunkStore. The instance keeps an internal buffer of Chunks, spilling to the ChunkStore when the buffer is full. If an attempt to Put Chunks fails, this method returns the BackpressureError from the underlying ChunkStore.
func (vbs *ValidatingBatchingSink) Enqueue(c chunks.Chunk) chunks.BackpressureError {
	h := c.Hash()
	if vbs.vs.isPresent(h) {
		return nil
	}
	v := DecodeFromBytes(c.Data(), vbs.vs, vbs.tc)
	d.PanicIfTrue(getHash(v) != h, "Invalid hash found")
	vbs.vs.ensureChunksInCache(v)
	vbs.vs.set(h, hintedChunk{v.Type(), h})

	vbs.batch[vbs.count] = c
	vbs.count++
	if vbs.count == batchSize {
		return vbs.Flush()
	}
	return nil
}

// Flush Puts any Chunks buffered by Enqueue calls into the backing ChunkStore. If the attempt to Put fails, this method returns the BackpressureError returned by the underlying ChunkStore.
func (vbs *ValidatingBatchingSink) Flush() (err chunks.BackpressureError) {
	err = vbs.cs.PutMany(vbs.batch[:vbs.count])
	vbs.count = 0
	return
}
