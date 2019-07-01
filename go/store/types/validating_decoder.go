// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
)

type ValidatingDecoder struct {
	vs     *ValueStore
	format *Format
}

func NewValidatingDecoder(cs chunks.ChunkStore) *ValidatingDecoder {
	return &ValidatingDecoder{NewValueStore(cs), getFormatForVersionString(cs.Version())}
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
	// TODO(binformat)
	v := decodeFromBytesWithValidation(c.Data(), vbs.vs, vbs.format)

	// TODO(binformat)
	if getHash(v, Format_7_18) != h {
		d.Panic("Invalid hash found")
	}
	return DecodedChunk{c, &v}
}
