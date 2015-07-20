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
	aws    awsStoreFlags
	file   fileStoreFlags
	memory memoryStoreFlags
	nop    nopStoreFlags
}

func NewFlags() Flags {
	return Flags{
		awsFlags(),
		fileFlags(),
		memoryFlags(),
		nopFlags(),
	}
}

func (f Flags) CreateStore() (cs ChunkStore) {
	if cs = f.aws.createStore(); cs != nil {
	} else if cs = f.file.createStore(); cs != nil {
	} else if cs = f.memory.createStore(); cs != nil {
	} else if cs = f.nop.createStore(); cs != nil {
	}
	return cs
}
