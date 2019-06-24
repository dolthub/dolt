// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"io/ioutil"
	"os"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

const (
	defaultCacheMemTableSize uint64 = 1 << 27 // 128MiB
)

func NewCache(ctx context.Context) *NomsBlockCache {
	dir, err := ioutil.TempDir("", "")
	d.PanicIfError(err)
	store := NewLocalStore(ctx, dir, defaultCacheMemTableSize)
	d.Chk.NoError(err, "opening put cache in %s", dir)
	return &NomsBlockCache{store, dir}
}

// NomsBlockCache holds Chunks, allowing them to be retrieved by hash or enumerated in hash order.
type NomsBlockCache struct {
	chunks *NomsBlockStore
	dbDir  string
}

// Insert stores c in the cache.
func (nbc *NomsBlockCache) Insert(ctx context.Context, c chunks.Chunk) {
	d.PanicIfFalse(nbc.chunks.addChunk(ctx, addr(c.Hash()), c.Data()))
}

// Has checks if the chunk referenced by hash is in the cache.
func (nbc *NomsBlockCache) Has(ctx context.Context, hash hash.Hash) bool {
	return nbc.chunks.Has(ctx, hash)
}

// HasMany returns a set containing the members of hashes present in the
// cache.
func (nbc *NomsBlockCache) HasMany(ctx context.Context, hashes hash.HashSet) hash.HashSet {
	return nbc.chunks.HasMany(ctx, hashes)
}

// Get retrieves the chunk referenced by hash. If the chunk is not present,
// Get returns the empty Chunk.
func (nbc *NomsBlockCache) Get(ctx context.Context, hash hash.Hash) chunks.Chunk {
	return nbc.chunks.Get(ctx, hash)
}

// GetMany gets the Chunks with |hashes| from the store. On return,
// |foundChunks| will have been fully sent all chunks which have been
// found. Any non-present chunks will silently be ignored.
func (nbc *NomsBlockCache) GetMany(ctx context.Context, hashes hash.HashSet, foundChunks chan *chunks.Chunk) {
	nbc.chunks.GetMany(ctx, hashes, foundChunks)
}

// ExtractChunks writes the entire contents of the cache to chunkChan. The
// chunks are extracted in insertion order.
func (nbc *NomsBlockCache) ExtractChunks(ctx context.Context, chunkChan chan *chunks.Chunk) {
	nbc.chunks.extractChunks(ctx, chunkChan)
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
