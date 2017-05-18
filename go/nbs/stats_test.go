// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

func TestStats(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	store := NewLocalStore(dir, testMemTableSize)

	i1, i2, i3, i4, i5 := []byte("abc"), []byte("def"), []byte("ghi"), []byte("jkl"), []byte("mno")

	c1, c2, c3, c4, c5 := chunks.NewChunk(i1), chunks.NewChunk(i2), chunks.NewChunk(i3), chunks.NewChunk(i4), chunks.NewChunk(i5)

	// These just go to mem table, only operation stats
	store.Put(c1)
	store.Put(c2)
	store.Put(c3)
	assert.Equal(uint64(3), store.Stats().PutLatency.Samples())
	assert.Equal(uint64(0), store.Stats().PersistLatency.Samples())

	assert.True(store.Has(c1.Hash()))
	assert.True(store.Has(c2.Hash()))
	assert.True(store.Has(c3.Hash()))
	assert.Equal(uint64(3), store.Stats().HasLatency.Samples())
	assert.Equal(uint64(4), store.Stats().AddressesPerHas.Sum())

	assert.False(store.Get(c1.Hash()).IsEmpty())
	assert.False(store.Get(c2.Hash()).IsEmpty())
	assert.False(store.Get(c3.Hash()).IsEmpty())
	assert.Equal(uint64(3), store.Stats().GetLatency.Samples())
	assert.Equal(uint64(0), store.Stats().ReadLatency.Samples())
	assert.Equal(uint64(4), store.Stats().ChunksPerGet.Sum())

	store.Commit(store.Root(), store.Root())

	// No we have write IO
	assert.Equal(uint64(1), store.Stats().PersistLatency.Samples())
	assert.Equal(uint64(3), store.Stats().ChunksPerPersist.Sum())
	assert.Equal(uint64(192), store.Stats().BytesPerPersist.Sum())

	// Now some gets that will incure read IO
	store.Get(c1.Hash())
	store.Get(c2.Hash())
	store.Get(c3.Hash())
	assert.Equal(uint64(3), store.Stats().ReadLatency.Samples())
	assert.Equal(uint64(36), store.Stats().BytesPerRead.Sum())
	assert.Equal(uint64(4), store.Stats().ChunksPerRead.Sum())

	// Try A GetMany
	chnx := make([]chunks.Chunk, 3)
	chnx[0] = c1
	chnx[1] = c2
	chnx[2] = c3
	hashes := make(hash.HashSlice, len(chnx))
	for i, c := range chnx {
		hashes[i] = c.Hash()
	}
	chunkChan := make(chan *chunks.Chunk, 3)
	store.GetMany(hashes.HashSet(), chunkChan)
	assert.Equal(uint64(4), store.Stats().ReadLatency.Samples())
	assert.Equal(uint64(60), store.Stats().BytesPerRead.Sum())
	assert.Equal(uint64(7), store.Stats().ChunksPerRead.Sum())

	// Force a conjoin
	store.c = newAsyncConjoiner(2)
	store.Put(c4)
	store.Commit(store.Root(), store.Root())
	store.Put(c5)
	store.Commit(store.Root(), store.Root())

	assert.Equal(uint64(1), store.Stats().ConjoinLatency.Samples())
	// TODO: Once random conjoin hack is out, test other conjoin stats

	defer store.Close()
	defer os.RemoveAll(dir)
}
