package store

import (
	"io"

	"github.com/attic-labs/noms/ref"
)

type ChunkSink interface {
	Put() ChunkWriter
}

type ChunkWriter interface {
	io.WriteCloser
	Ref() (ref.Ref, error)
}
