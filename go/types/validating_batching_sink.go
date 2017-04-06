// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

const batchSize = 100

type ValidatingBatchingSink struct {
	vs         *ValueStore
	cs         chunks.ChunkStore
	validate   bool
	unresolved hash.HashSet
}

func NewValidatingBatchingSink(cs chunks.ChunkStore) *ValidatingBatchingSink {
	return &ValidatingBatchingSink{
		vs:         newLocalValueStore(cs),
		cs:         cs,
		validate:   true,
		unresolved: hash.HashSet{},
	}
}

func NewCompletenessCheckingBatchingSink(cs chunks.ChunkStore) *ValidatingBatchingSink {
	return &ValidatingBatchingSink{
		vs:         newLocalValueStore(cs),
		cs:         cs,
		validate:   false,
		unresolved: hash.HashSet{},
	}
}

// DecodedChunk holds a pointer to a Chunk and the Value that results from
// calling DecodeFromBytes(c.Data()).
type DecodedChunk struct {
	Chunk *chunks.Chunk
	Value *Value
}

// DecodeUnqueued decodes c and checks that the hash of the resulting value
// matches c.Hash(). It returns a DecodedChunk holding both c and a pointer to
// the decoded Value.
func (vbs *ValidatingBatchingSink) DecodeUnqueued(c *chunks.Chunk) DecodedChunk {
	h := c.Hash()
	var v Value
	if vbs.validate {
		v = decodeFromBytesWithValidation(c.Data(), vbs.vs)
	} else {
		v = DecodeFromBytes(c.Data(), vbs.vs)
	}

	if getHash(v) != h {
		d.Panic("Invalid hash found")
	}
	return DecodedChunk{c, &v}
}

// Put Puts c into vbs' backing ChunkStore. It is assumed that v is the Value
// decoded from c, and so v can be used to validate the ref-completeness of c.
func (vbs *ValidatingBatchingSink) Put(c chunks.Chunk, v Value) {
	h := c.Hash()
	vbs.unresolved.Remove(h)
	v.WalkRefs(func(ref Ref) {
		vbs.unresolved.Insert(ref.TargetHash())
	})
	vbs.cs.Put(c)
}

// Flush makes durable all enqueued Chunks.
func (vbs *ValidatingBatchingSink) Flush() {
	vbs.cs.Flush()
}

// PanicIfDangling does a Has check on all the references encountered
// while enqueuing novel chunks. It panics if any of these refs point
// to Chunks that don't exist in the backing ChunkStore.
func (vbs *ValidatingBatchingSink) PanicIfDangling() {
	present := vbs.cs.HasMany(vbs.unresolved)
	absent := hash.HashSlice{}
	for h := range vbs.unresolved {
		if !present.Has(h) {
			absent = append(absent, h)
		}
	}
	if len(absent) != 0 {
		d.Panic("Found dangling references to %v", absent)
	}
}
