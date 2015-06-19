package chunks

import (
	"io"

	"github.com/attic-labs/noms/ref"
)

type ChunkStore interface {
	ChunkSource
	ChunkSink
}

type RootTracker interface {
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

type Flags struct {
	file   fileStoreFlags
	memory memoryStoreFlags
	s3     s3StoreFlags
}

func NewFlags() Flags {
	return Flags{
		fileFlags(),
		memoryFlags(),
		s3Flags(),
	}
}

func (f Flags) CreateStore() (cs ChunkStore) {
	if cs = f.file.createStore(); cs != nil {
	} else if f.memory.createStore(); cs != nil {
	} else if f.s3.createStore(); cs != nil {
	}
	return cs
}
