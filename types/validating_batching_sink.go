package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
)

const batchSize = 16

type ValidatingBatchingSink struct {
	vs    *ValueStore
	cs    chunks.ChunkStore
	batch [batchSize]chunks.Chunk
	count int
}

func NewValidatingBatchingSink(cs chunks.ChunkStore) *ValidatingBatchingSink {
	return &ValidatingBatchingSink{vs: newLocalValueStore(cs), cs: cs}
}

// Prepare primes the type info cache used to validate Enqueued Chunks by reading the Chunks referenced by the provided hints.
func (vbs *ValidatingBatchingSink) Prepare(hints Hints) {
	for hint := range hints {
		vbs.vs.ReadValue(hint)
	}
}

// Enequeue adds a Chunk to the queue of Chunks waiting to be Put into vbs' backing ChunkStore. The instance keeps an internal buffer of Chunks, spilling to the ChunkStore when the buffer is full. If an attempt to Put Chunks fails, this method returns the BackpressureError from the underlying ChunkStore.
func (vbs *ValidatingBatchingSink) Enqueue(c chunks.Chunk) chunks.BackpressureError {
	r := c.Ref()
	if vbs.vs.isPresent(r) {
		return nil
	}
	v := DecodeChunk(c, vbs.vs)
	d.Exp.NotNil(v, "Chunk with hash %s failed to decode", r)
	vbs.vs.checkChunksInCache(v)
	vbs.vs.set(r, hintedChunk{v.Type(), r})

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
