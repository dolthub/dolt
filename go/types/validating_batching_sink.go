// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

const batchSize = 100

type ValidatingBatchingSink struct {
	vs    *ValueStore
	cs    chunks.ChunkStore
	batch [batchSize]chunks.Chunk
	count int
	pool  sync.Pool
}

func NewValidatingBatchingSink(cs chunks.ChunkStore) *ValidatingBatchingSink {
	return &ValidatingBatchingSink{
		vs:   newLocalValueStore(cs),
		cs:   cs,
		pool: sync.Pool{New: func() interface{} { return NewTypeCache() }},
	}
}

// Prepare primes the type info cache used to validate Enqueued Chunks by reading the Chunks referenced by the provided hints.
func (vbs *ValidatingBatchingSink) Prepare(hints Hints) {
	rl := make(chan struct{}, batchSize)
	wg := sync.WaitGroup{}
	for hint := range hints {
		wg.Add(1)
		rl <- struct{}{}
		go func(hint hash.Hash) {
			vbs.vs.ReadValue(hint)
			<-rl
			wg.Done()
		}(hint)
	}
	wg.Wait()
	close(rl)
}

// DecodedChunk holds a pointer to a Chunk and the Value that results from
// calling DecodeFromBytes(c.Data()).
type DecodedChunk struct {
	Chunk *chunks.Chunk
	Value *Value
}

// DecodeUnqueued decodes c and checks that the hash of the resulting value
// matches c.Hash(). It returns a DecodedChunk holding both c and a pointer to
// the decoded Value. However, if c has already been Enqueued, DecodeUnqueued
// returns an empty DecodedChunk.
func (vbs *ValidatingBatchingSink) DecodeUnqueued(c *chunks.Chunk) DecodedChunk {
	h := c.Hash()
	if vbs.vs.isPresent(h) {
		return DecodedChunk{}
	}
	tc := vbs.pool.Get()
	defer vbs.pool.Put(tc)
	v := DecodeFromBytes(c.Data(), vbs.vs, tc.(*TypeCache))
	if getHash(v) != h {
		d.Panic("Invalid hash found")
	}
	return DecodedChunk{c, &v}
}

// Enequeue adds c to the queue of Chunks waiting to be Put into vbs' backing
// ChunkStore. It is assumed that v is the Value decoded from c, and so v can
// be used to validate the ref-completeness of c.  The instance keeps an
// internal buffer of Chunks, spilling to the ChunkStore when the buffer is
// full. If an attempt to Put Chunks fails, this method returns the
// BackpressureError from the underlying ChunkStore.
func (vbs *ValidatingBatchingSink) Enqueue(c chunks.Chunk, v Value) chunks.BackpressureError {
	h := c.Hash()
	vbs.vs.ensureChunksInCache(v)
	vbs.vs.set(h, hintedChunk{v.Type(), h})

	vbs.batch[vbs.count] = c
	vbs.count++
	if vbs.count == batchSize {
		return vbs.Flush()
	}
	return nil
}

// Flush Puts any Chunks buffered by Enqueue calls into the backing
// ChunkStore. If the attempt to Put fails, this method returns the
// BackpressureError returned by the underlying ChunkStore.
func (vbs *ValidatingBatchingSink) Flush() (err chunks.BackpressureError) {
	err = vbs.cs.PutMany(vbs.batch[:vbs.count])
	vbs.count = 0
	return
}
