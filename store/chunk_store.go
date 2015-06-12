package store

import (
	"io"

	"github.com/attic-labs/noms/ref"
)

type ChunkSource interface {
	Root() ref.Ref
	Get(ref ref.Ref) (io.ReadCloser, error)
}

type ChunkSink interface {
	UpdateRoot(current, last ref.Ref) bool
	Put() ChunkWriter
}

type ChunkWriter interface {
	io.WriteCloser
	Ref() (ref.Ref, error)
}
