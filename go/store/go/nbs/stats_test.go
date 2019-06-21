// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/stretchr/testify/assert"
)

func TestStats(t *testing.T) {
	assert := assert.New(t)

	stats := func(store *NomsBlockStore) Stats {
		return store.Stats().(Stats)
	}

	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	store := NewLocalStore(context.Background(), dir, testMemTableSize)

	assert.EqualValues(1, stats(store).OpenLatency.Samples())

	// Opening a new store will still incur some read IO, to discover that the manifest doesn't exist
	assert.EqualValues(1, stats(store).ReadManifestLatency.Samples())

	i1, i2, i3, i4, i5 := []byte("abc"), []byte("def"), []byte("ghi"), []byte("jkl"), []byte("mno")

	c1, c2, c3, c4, c5 := chunks.NewChunk(i1), chunks.NewChunk(i2), chunks.NewChunk(i3), chunks.NewChunk(i4), chunks.NewChunk(i5)

	// These just go to mem table, only operation stats
	store.Put(context.Background(), c1)
	store.Put(context.Background(), c2)
	store.Put(context.Background(), c3)
	assert.Equal(uint64(3), stats(store).PutLatency.Samples())
	assert.Equal(uint64(0), stats(store).PersistLatency.Samples())

	assert.True(store.Has(context.Background(), c1.Hash()))
	assert.True(store.Has(context.Background(), c2.Hash()))
	assert.True(store.Has(context.Background(), c3.Hash()))
	assert.Equal(uint64(3), stats(store).HasLatency.Samples())
	assert.Equal(uint64(3), stats(store).AddressesPerHas.Sum())

	assert.False(store.Get(context.Background(), c1.Hash()).IsEmpty())
	assert.False(store.Get(context.Background(), c2.Hash()).IsEmpty())
	assert.False(store.Get(context.Background(), c3.Hash()).IsEmpty())
	assert.Equal(uint64(3), stats(store).GetLatency.Samples())
	assert.Equal(uint64(0), stats(store).FileReadLatency.Samples())
	assert.Equal(uint64(3), stats(store).ChunksPerGet.Sum())

	store.Commit(context.Background(), store.Root(context.Background()), store.Root(context.Background()))

	// Commit will update the manifest
	assert.EqualValues(1, stats(store).WriteManifestLatency.Samples())
	assert.EqualValues(1, stats(store).CommitLatency.Samples())

	// Now we have write IO
	assert.Equal(uint64(1), stats(store).PersistLatency.Samples())
	assert.Equal(uint64(3), stats(store).ChunksPerPersist.Sum())
	assert.Equal(uint64(131), stats(store).BytesPerPersist.Sum())

	// Now some gets that will incur read IO
	store.Get(context.Background(), c1.Hash())
	store.Get(context.Background(), c2.Hash())
	store.Get(context.Background(), c3.Hash())
	assert.Equal(uint64(3), stats(store).FileReadLatency.Samples())
	assert.Equal(uint64(27), stats(store).FileBytesPerRead.Sum())

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
	store.GetMany(context.Background(), hashes.HashSet(), chunkChan)
	assert.Equal(uint64(4), stats(store).FileReadLatency.Samples())
	assert.Equal(uint64(54), stats(store).FileBytesPerRead.Sum())

	// Force a conjoin
	store.c = inlineConjoiner{2}
	store.Put(context.Background(), c4)
	store.Commit(context.Background(), store.Root(context.Background()), store.Root(context.Background()))
	store.Put(context.Background(), c5)
	store.Commit(context.Background(), store.Root(context.Background()), store.Root(context.Background()))

	assert.Equal(uint64(1), stats(store).ConjoinLatency.Samples())
	// TODO: Once random conjoin hack is out, test other conjoin stats

	defer store.Close()
	defer os.RemoveAll(dir)
}
