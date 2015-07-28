package chunks

import (
	"io"

	"github.com/attic-labs/noms/ref"
)

// ChunkStore provides the ability to read and write chunks of data, addressable by a ref.Ref.
type ChunkStore interface {
	ChunkSource
	ChunkSink
	RootTracker
}

// RootTracker allows querying and management of the root of an entire tree of references.
type RootTracker interface {
	Root() ref.Ref
	UpdateRoot(current, last ref.Ref) bool
}

// ChunkSource provides an interface for fetching chunks by Ref.
type ChunkSource interface {
	// Get gets a reader for the value of the Ref in the store. If the ref is absent from the store nil and no error is returned.
	Get(ref ref.Ref) (io.ReadCloser, error)
}

// ChunkSink is a place to put chunks.
type ChunkSink interface {
	Put() ChunkWriter
}

// ChunkWriter wraps an io.WriteCloser, additionally providing the ability to grab a Ref for all data written through the interface. Calling Ref() or Close() on an instance disallows further writing.
type ChunkWriter interface {
	io.WriteCloser
	// Ref returns the ref.Ref for all data written at the time of call.
	Ref() (ref.Ref, error)
}

// NewFlags creates a new instance of Flags.
func NewFlags() Flags {
	return NewFlagsWithPrefix("")
}

// NewFlagsWithPrefix creates a new instance of Flags with the names of all flags declared therein prefixed by the given string.
func NewFlagsWithPrefix(prefix string) Flags {
	return Flags{
		awsFlags(prefix),
		fileFlags(prefix),
		memoryFlags(prefix),
		nopFlags(prefix),
	}
}

// Flags wraps up the command-line flags for all ChunkStore implementations.
type Flags struct {
	aws    awsStoreFlags
	file   fileStoreFlags
	memory memoryStoreFlags
	nop    nopStoreFlags
}

// CreateStore consults f and returns an instance of the appropriate ChunkStore implementation.
func (f Flags) CreateStore() (cs ChunkStore) {
	if cs = f.aws.createStore(); cs != nil {
	} else if cs = f.file.createStore(); cs != nil {
	} else if cs = f.memory.createStore(); cs != nil {
	} else if cs = f.nop.createStore(); cs != nil {
	}
	return cs
}
