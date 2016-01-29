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

// Factory allows the creation of namespaced ChunkStore instances. The details of how namespaces are separated is left up to the particular implementation of Factory and ChunkStore.
type Factory interface {
	CreateNamespacedStore(ns string) ChunkStore

	// Shutter shuts down the factory. Subsequent calls to CreateNamespacedStore() will fail.
	Shutter()
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
