// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestChunk(t *testing.T) {
	c := NewChunk([]byte("abc"))
	h := c.Hash()
	// See http://www.di-mgt.com.au/sha_testvectors.html
	assert.Equal(t, "rmnjb8cjc5tblj21ed4qs821649eduie", h.String())
}

func TestChunkWriteAfterCloseFails(t *testing.T) {
	assert := assert.New(t)
	input := "abc"
	w := NewChunkWriter()
	_, err := w.Write([]byte(input))
	assert.NoError(err)

	assert.NoError(w.Close())
	assert.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}

func TestChunkWriteAfterChunkFails(t *testing.T) {
	assert := assert.New(t)
	input := "abc"
	w := NewChunkWriter()
	_, err := w.Write([]byte(input))
	assert.NoError(err)

	_ = w.Chunk()
	assert.Panics(func() { w.Write([]byte(input)) }, "Write() after Chunk() should barf!")
}

func TestChunkChunkCloses(t *testing.T) {
	assert := assert.New(t)
	input := "abc"
	w := NewChunkWriter()
	_, err := w.Write([]byte(input))
	assert.NoError(err)

	w.Chunk()
	assert.Panics(func() { w.Write([]byte(input)) }, "Write() after Close() should barf!")
}
