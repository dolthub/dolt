// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"bytes"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestSerializeRoundTrip(t *testing.T) {
	assert := assert.New(t)
	inputs := [][]byte{[]byte("abc"), []byte("def")}
	chnx := make([]Chunk, len(inputs))
	for i, data := range inputs {
		chnx[i] = NewChunk(data)
	}

	buf := &bytes.Buffer{}
	Serialize(chnx[0], buf)
	Serialize(chnx[1], buf)

	chunkChan := make(chan *Chunk)
	go func() {
		defer close(chunkChan)
		err := Deserialize(bytes.NewReader(buf.Bytes()), chunkChan)
		assert.NoError(err)
	}()

	for c := range chunkChan {
		assert.Equal(chnx[0].Hash(), c.Hash())
		chnx = chnx[1:]
	}
	assert.Len(chnx, 0)
}

func TestBadSerialization(t *testing.T) {
	bad := []byte{0, 1} // Not enough bytes to read first length
	ch := make(chan *Chunk)
	defer close(ch)
	assert.Error(t, Deserialize(bytes.NewReader(bad), ch))
}
