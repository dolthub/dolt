package chunks

import (
	"bytes"
	"hash"
	"io"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// Chunk is a unit of stored data in noms
type Chunk struct {
	r    ref.Ref
	data []byte
}

var EmptyChunk = Chunk{}

func (c Chunk) Ref() ref.Ref {
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
	r := ref.FromData(data)
	return Chunk{r, data}
}

// NewChunkWithRef creates a new chunk with a known ref. The ref is not re-calculated or verified. This should obviously only be used in cases where the caller already knows the specified ref is correct.
func NewChunkWithRef(r ref.Ref, data []byte) Chunk {
	return Chunk{r, data}
}

// ChunkWriter wraps an io.WriteCloser, additionally providing the ability to grab the resulting Chunk for all data written through the interface. Calling Chunk() or Close() on an instance disallows further writing.
type ChunkWriter struct {
	buffer *bytes.Buffer
	writer io.Writer
	hash   hash.Hash
	c      Chunk
}

func NewChunkWriter() *ChunkWriter {
	b := &bytes.Buffer{}
	h := ref.NewHash()
	return &ChunkWriter{
		buffer: b,
		writer: io.MultiWriter(b, h),
		hash:   h,
	}
}

func (w *ChunkWriter) Write(data []byte) (int, error) {
	d.Chk.NotNil(w.buffer, "Write() cannot be called after Ref() or Close().")
	size, err := w.writer.Write(data)
	d.Chk.NoError(err)
	return size, nil
}

// Chunk() closes the writer and returns the resulting Chunk.
func (w *ChunkWriter) Chunk() Chunk {
	d.Chk.NoError(w.Close())
	return w.c
}

// Close() closes computes the ref and Puts it into the ChunkSink Note: The Write() method never returns an error. Instead, like other noms interfaces, errors are reported via panic.
func (w *ChunkWriter) Close() error {
	if w.buffer == nil {
		return nil
	}

	w.c = NewChunk(w.buffer.Bytes())
	w.buffer = nil
	return nil
}
