// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"io"
	"sync"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

// BatchStore provides an interface similar to chunks.ChunkStore, but batch-
// oriented. Instead of Put(), it provides SchedulePut(), which enqueues a
// Chunk to be sent at a possibly later time.
type BatchStore interface {
	// Get returns the Chunk with the hash h from the store. If h is absent
	// from the store, chunks.EmptyChunk is returned.
	Get(h hash.Hash) chunks.Chunk

	// GetMany gets the Chunks with |hashes| from the store. On return,
	// |foundChunks| will have been fully sent all chunks which have been
	// found. Any non-present chunks will silently be ignored.
	GetMany(hashes hash.HashSet, foundChunks chan *chunks.Chunk)

	// SchedulePut enqueues a write for the Chunk c with the given refHeight.
	// c must be visible to subsequent Get() calls upon return. Typically, the
	// Value which was encoded to provide c can also be queried for its
	// refHeight. The call may or may not block until c is persisted, but c is
	// guaranteed to be persistent after a call to Flush() or UpdateRoot().
	SchedulePut(c chunks.Chunk)

	// Flush causes enqueued Puts to be persisted.
	Flush()
	chunks.RootTracker
	io.Closer
}

// BatchStoreAdaptor provides a naive implementation of BatchStore should only
// be used with ChunkStores that can Put relatively quickly. It provides no
// actual batching or validation. Its intended use is for adapting a
// ChunkStore for use in something that requires a BatchStore.
type BatchStoreAdaptor struct {
	cs   chunks.ChunkStore
	once sync.Once
}

// NewBatchStoreAdaptor returns a BatchStore instance backed by a ChunkStore.
// Takes ownership of cs and manages its lifetime; calling Close on the
// returned BatchStore will Close cs.
func NewBatchStoreAdaptor(cs chunks.ChunkStore) BatchStore {
	return &BatchStoreAdaptor{cs: cs}
}

// Get simply proxies to the backing ChunkStore
func (bsa *BatchStoreAdaptor) Get(h hash.Hash) chunks.Chunk {
	bsa.once.Do(bsa.expectVersion)
	return bsa.cs.Get(h)
}

// GetMany simply proxies to the backing ChunkStore
func (bsa *BatchStoreAdaptor) GetMany(hashes hash.HashSet, foundChunks chan *chunks.Chunk) {
	bsa.once.Do(bsa.expectVersion)
	bsa.cs.GetMany(hashes, foundChunks)
}

// SchedulePut simply calls Put on the underlying ChunkStore.
func (bsa *BatchStoreAdaptor) SchedulePut(c chunks.Chunk) {
	bsa.once.Do(bsa.expectVersion)
	bsa.cs.Put(c)
}

func (bsa *BatchStoreAdaptor) expectVersion() {
	dataVersion := bsa.cs.Version()
	if constants.NomsVersion != dataVersion {
		d.Panic("SDK version %s incompatible with data of version %s", constants.NomsVersion, dataVersion)
	}
}

func (bsa *BatchStoreAdaptor) Root() hash.Hash {
	return bsa.cs.Root()
}

func (bsa *BatchStoreAdaptor) UpdateRoot(current, last hash.Hash) bool {
	bsa.once.Do(bsa.expectVersion)
	return bsa.cs.UpdateRoot(current, last)
}

func (bsa *BatchStoreAdaptor) Flush() {
	bsa.once.Do(bsa.expectVersion)
	bsa.cs.Flush()
}

// Close closes the underlying ChunkStore
func (bsa *BatchStoreAdaptor) Close() error {
	bsa.Flush()
	return bsa.cs.Close()
}
