package store

import (
	"io"

	"github.com/attic-labs/noms/ref"
)

type RootStore interface {
	Root() ref.Ref
	UpdateRoot(current, last ref.Ref) bool
}

type ChunkSource interface {
	Get(ref ref.Ref) (io.ReadCloser, error)
}

type ChunkSink interface {
	Put() ChunkWriter
}

type ChunkWriter interface {
	io.WriteCloser
	Ref() (ref.Ref, error)
}
