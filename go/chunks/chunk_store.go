// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"io"

	"github.com/attic-labs/noms/go/hash"
)

// ChunkStore is the core storage abstraction in noms. We can put data anyplace we have a
// ChunkStore implementation for.
type ChunkStore interface {
	ChunkSource
	ChunkSink
	RootTracker
}

// Factory allows the creation of namespaced ChunkStore instances. The details
// of how namespaces are separated is left up to the particular implementation
// of Factory and ChunkStore.
type Factory interface {
	CreateStore(ns string) ChunkStore

	// Shutter shuts down the factory. Subsequent calls to CreateStore() will fail.
	Shutter()
}

// RootTracker allows querying and management of the root of an entire tree of
// references. The "root" is the single mutable variable in a ChunkStore. It
// can store any hash, but it is typically used by higher layers (such as
// Database) to store a hash to a value that represents the current state and
// entire history of a database.
type RootTracker interface {
	Root() hash.Hash
	UpdateRoot(current, last hash.Hash) bool
}

// ChunkSource is a place to get chunks from.
type ChunkSource interface {
	// Get the Chunk for the value of the hash in the store. If the hash is
	// absent from the store nil is returned.
	Get(h hash.Hash) Chunk

	// GetMany gets the Chunks with |hashes| from the store. On return,
	// |foundChunks| will have been fully sent all chunks which have been
	// found. Any non-present chunks will silently be ignored.
	GetMany(hashes hash.HashSet, foundChunks chan *Chunk)

	// Returns true iff the value at the address |h| is contained in the source
	Has(h hash.Hash) bool

	// Returns the NomsVersion with which this ChunkSource is compatible.
	Version() string
}

// ChunkSink is a place to put chunks.
type ChunkSink interface {
	// Put writes c into the ChunkSink, blocking until the operation is complete.
	Put(c Chunk)

	// PutMany writes chunks into the sink, blocking until the operation is complete.
	PutMany(chunks []Chunk)

	// On return, any previously Put chunks should be durable
	Flush()

	io.Closer
}
