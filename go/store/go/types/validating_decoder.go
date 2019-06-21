// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
)

type ValidatingDecoder struct {
	vs *ValueStore
}

func NewValidatingDecoder(cs chunks.ChunkStore) *ValidatingDecoder {
	return &ValidatingDecoder{NewValueStore(cs)}
}

// DecodedChunk holds a pointer to a Chunk and the Value that results from
// calling DecodeFromBytes(c.Data()).
type DecodedChunk struct {
	Chunk *chunks.Chunk
	Value *Value
}

// Decode decodes c and checks that the hash of the resulting value
// matches c.Hash(). It returns a DecodedChunk holding both c and a pointer to
// the decoded Value.
func (vbs *ValidatingDecoder) Decode(c *chunks.Chunk) DecodedChunk {
	h := c.Hash()
	v := decodeFromBytesWithValidation(c.Data(), vbs.vs)

	if getHash(v) != h {
		d.Panic("Invalid hash found")
	}
	return DecodedChunk{c, &v}
}
