package chunks

import (
	"io"

	"github.com/attic-labs/noms/ref"
)

type ChunkStore interface {
	ChunkSource
	ChunkSink
	RootTracker
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
	aws    awsStoreFlags
}

func NewFlags() Flags {
	return Flags{
		fileFlags(),
		memoryFlags(),
		awsFlags(),
	}
}

func (f Flags) CreateStore() (cs ChunkStore) {
	if cs = f.file.createStore(); cs != nil {
	} else if cs = f.memory.createStore(); cs != nil {
	} else if cs = f.aws.createStore(); cs != nil {
	}
	return cs
}
