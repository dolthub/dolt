// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package chunks provides facilities for representing, storing, and fetching content-addressed chunks of Noms data.
package chunks

import (
	"bytes"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

// Chunk is a unit of stored data in noms
type Chunk struct {
	r    hash.Hash
	data []byte
}

var EmptyChunk = NewChunk([]byte{})

func (c Chunk) Hash() hash.Hash {
	return c.r
}

func (c Chunk) Data() []byte {
	return c.data
}

func (c Chunk) IsEmpty() bool {
	return len(c.data) == 0
}

// NewChunk creates a new Chunk backed by data. This means that the returned Chunk has ownership of this slice of memory.
func NewChunk(data []byte) Chunk {
	r := hash.Of(data)
	return Chunk{r, data}
}

// NewChunkWithHash creates a new chunk with a known hash. The hash is not re-calculated or verified. This should obviously only be used in cases where the caller already knows the specified hash is correct.
func NewChunkWithHash(r hash.Hash, data []byte) Chunk {
	return Chunk{r, data}
}

// ChunkWriter wraps an io.WriteCloser, additionally providing the ability to grab the resulting Chunk for all data written through the interface. Calling Chunk() or Close() on an instance disallows further writing.
type ChunkWriter struct {
	buffer *bytes.Buffer
	c      Chunk
}

func NewChunkWriter() *ChunkWriter {
	b := &bytes.Buffer{}
	return &ChunkWriter{
		buffer: b,
	}
}

func (w *ChunkWriter) Write(data []byte) (int, error) {
	if w.buffer == nil {
		d.Panic("Write() cannot be called after Hash() or Close().")
	}
	size, err := w.buffer.Write(data)
	d.Chk.NoError(err)
	return size, nil
}

// Chunk() closes the writer and returns the resulting Chunk.
func (w *ChunkWriter) Chunk() Chunk {
	d.Chk.NoError(w.Close())
	return w.c
}

// Close() closes computes the hash and Puts it into the ChunkSink Note: The Write() method never returns an error. Instead, like other noms interfaces, errors are reported via panic.
func (w *ChunkWriter) Close() error {
	if w.buffer == nil {
		return nil
	}

	w.c = NewChunk(w.buffer.Bytes())
	w.buffer = nil
	return nil
}
