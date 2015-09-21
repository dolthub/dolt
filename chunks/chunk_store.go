package chunks

import (
	"io"

	"github.com/attic-labs/noms/ref"
)

// ChunkStore is the core storage abstraction in noms. We can put data anyplace we have a ChunkStore implementation for.
type ChunkStore interface {
	ChunkSource
	ChunkSink
	RootTracker
}

// RootTracker allows querying and management of the root of an entire tree of references. The "root" is the single mutable variable in a ChunkStore. It can store any ref, but it is typically used by higher layers (such as DataStore) to store a ref to a value that represents the current state and entire history of a datastore.
type RootTracker interface {
	Root() ref.Ref
	UpdateRoot(current, last ref.Ref) bool
}

// ChunkSource is a place to get chunks from.
type ChunkSource interface {
	// Get gets a reader for the value of the Ref in the store. If the ref is absent from the store nil is returned.
	Get(ref ref.Ref) Chunk

	// Returns true iff the value at the address |ref| is contained in the source
	Has(ref ref.Ref) bool
}

// ChunkSink is a place to put chunks.
type ChunkSink interface {
	Put(c Chunk)
	io.Closer
}

// NewFlags creates a new instance of Flags, which declares a number of ChunkStore-related command-line flags using the golang flag package. Call this before flag.Parse().
func NewFlags() Flags {
	return NewFlagsWithPrefix("")
}

// NewFlagsWithPrefix creates a new instance of Flags with the names of all flags declared therein prefixed by the given string.
func NewFlagsWithPrefix(prefix string) Flags {
	return Flags{
		levelDBFlags(prefix),
		memoryFlags(prefix),
		nopFlags(prefix),
	}
}

// Flags abstracts away definitions for and handling of command-line flags for all ChunkStore implementations.
type Flags struct {
	ldb    ldbStoreFlags
	memory memoryStoreFlags
	nop    nopStoreFlags
}

// CreateStore creates a ChunkStore implementation based on the values of command-line flags.
func (f Flags) CreateStore() (cs ChunkStore) {
	if cs = f.ldb.createStore(); cs != nil {
	} else if cs = f.memory.createStore(); cs != nil {
	} else if cs = f.nop.createStore(); cs != nil {
	}
	return cs
}
