// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io/ioutil"
	"os"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

const (
	defaultCacheMemTableSize uint64 = 1 << 27 // 128MiB
)

func NewCache() *NomsBlockCache {
	dir, err := ioutil.TempDir("", "")
	d.PanicIfError(err)
	store := NewLocalStore(dir, defaultCacheMemTableSize)
	d.Chk.NoError(err, "opening put cache in %s", dir)
	return &NomsBlockCache{store, dir}
}

// NomsBlockCache holds Chunks, allowing them to be retrieved by hash or enumerated in hash order.
type NomsBlockCache struct {
	chunks *NomsBlockStore
	dbDir  string
}

// Insert stores c in the cache.
func (nbc *NomsBlockCache) Insert(c chunks.Chunk) {
	d.PanicIfFalse(nbc.chunks.addChunk(addr(c.Hash()), c.Data()))
}

// Has checks if the chunk referenced by hash is in the cache.
func (nbc *NomsBlockCache) Has(hash hash.Hash) bool {
	return nbc.chunks.Has(hash)
}

// Get retrieves the chunk referenced by hash. If the chunk is not present,
// Get returns the empty Chunk.
func (nbc *NomsBlockCache) Get(hash hash.Hash) chunks.Chunk {
	return nbc.chunks.Get(hash)
}

// ExtractChunks writes the entire contents of the cache to chunkChan. The
// chunks are extracted in insertion order.
func (nbc *NomsBlockCache) ExtractChunks(order EnumerationOrder, chunkChan chan *chunks.Chunk) {
	nbc.chunks.extractChunks(order, chunkChan)
}

// Count returns the number of items in the cache.
func (nbc *NomsBlockCache) Count() uint32 {
	return nbc.chunks.Count()
}

// Destroy drops the cache and deletes any backing storage.
func (nbc *NomsBlockCache) Destroy() error {
	d.Chk.NoError(nbc.chunks.Close())
	return os.RemoveAll(nbc.dbDir)
}
