package chunks

import (
	"bytes"
	"hash"
	"io"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// ChunkWriter wraps an io.WriteCloser, additionally providing the ability to grab a Ref for all data written through the interface. Calling Ref() or Close() on an instance disallows further writing.
type ChunkWriter interface {
	// Note: The Write() method never returns an error. Instead, like other noms interfaces, errors are reported via panic.
	io.WriteCloser
	// Ref returns the ref.Ref for all data written at the time of call.
	Ref() ref.Ref
}

// ChunkWriter wraps an io.WriteCloser, additionally providing the ability to grab a Ref for all data written through the interface. Calling Ref() or Close() on an instance disallows further writing.
type writeFn func(ref ref.Ref, data []byte)

type chunkWriter struct {
	write  writeFn
	buffer *bytes.Buffer
	writer io.Writer
	hash   hash.Hash
	ref    ref.Ref
}

func NewChunkWriter(write writeFn) ChunkWriter {
	b := &bytes.Buffer{}
	h := ref.NewHash()
	return &chunkWriter{
		write:  write,
		buffer: b,
		writer: io.MultiWriter(b, h),
		hash:   h,
	}
}

func (w *chunkWriter) Write(data []byte) (int, error) {
	d.Chk.NotNil(w.buffer, "Write() cannot be called after Ref() or Close().")
	size, err := w.writer.Write(data)
	d.Chk.NoError(err)
	return size, nil
}

func (w *chunkWriter) Ref() ref.Ref {
	d.Chk.NoError(w.Close())
	return w.ref
}

func (w *chunkWriter) Close() error {
	if w.buffer == nil {
		return nil
	}

	w.ref = ref.FromHash(w.hash)
	w.write(w.ref, w.buffer.Bytes())
	w.buffer = nil
	return nil
}
