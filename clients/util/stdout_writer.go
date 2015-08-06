package util

import (
	"hash"
	"io"
	"io/ioutil"
	"os"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

// StdoutChunkSink implements chunks.ChunkSink by writing data to stdout.
type StdoutChunkSink struct {
	chunks.ChunkSink
}

// Put returns an implementation of chunks.ChunkWriter backed by stdout.
func (f StdoutChunkSink) Put() chunks.ChunkWriter {
	h := ref.NewHash()
	// Note that we never want to close stdout, so we put a NopCloser into
	// stdoutChunkWriter to satisfy the Closer part of the ChunkWriter interface.
	return &stdoutChunkWriter{
		ioutil.NopCloser(nil),
		os.Stdout,
		io.MultiWriter(os.Stdout, h),
		h,
	}
}

type stdoutChunkWriter struct {
	io.Closer
	file   *os.File
	writer io.Writer
	hash   hash.Hash
}

func (w *stdoutChunkWriter) Write(data []byte) (int, error) {
	dbg.Chk.NotNil(w.file, "Write() cannot be called after Ref() or Close().")
	return w.writer.Write(data)
}

func (w *stdoutChunkWriter) Ref() (ref.Ref, error) {
	w.file = nil
	return ref.FromHash(w.hash), nil
}
