// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestStats(t *testing.T) {
	assert := assert.New(t)

	stats := func(store *NomsBlockStore) Stats {
		return store.Stats().(Stats)
	}

	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer file.RemoveAll(dir)
	store, err := NewLocalStore(context.Background(), constants.FormatDefaultString, dir, testMemTableSize, NewUnlimitedMemQuotaProvider())
	require.NoError(t, err)
	defer store.Close()

	assert.EqualValues(1, stats(store).OpenLatency.Samples())

	// Opening a new store will still incur some read IO, to discover that the manifest doesn't exist
	assert.EqualValues(1, stats(store).ReadManifestLatency.Samples())

	i1, i2, i3, i4, i5 := []byte("abc"), []byte("def"), []byte("ghi"), []byte("jkl"), []byte("mno")

	c1, c2, c3, c4, c5 := chunks.NewChunk(i1), chunks.NewChunk(i2), chunks.NewChunk(i3), chunks.NewChunk(i4), chunks.NewChunk(i5)

	// These just go to mem table, only operation stats
	err = store.Put(context.Background(), c1, noopGetAddrs)
	require.NoError(t, err)
	err = store.Put(context.Background(), c2, noopGetAddrs)
	require.NoError(t, err)
	err = store.Put(context.Background(), c3, noopGetAddrs)
	require.NoError(t, err)
	assert.Equal(uint64(3), stats(store).PutLatency.Samples())
	assert.Equal(uint64(0), stats(store).PersistLatency.Samples())

	assert.True(store.Has(context.Background(), c1.Hash()))
	assert.True(store.Has(context.Background(), c2.Hash()))
	assert.True(store.Has(context.Background(), c3.Hash()))
	assert.Equal(uint64(3), stats(store).HasLatency.Samples())
	assert.Equal(uint64(3), stats(store).AddressesPerHas.Sum())

	c, err := store.Get(context.Background(), c1.Hash())
	require.NoError(t, err)
	assert.False(c.IsEmpty())
	c, err = store.Get(context.Background(), c2.Hash())
	require.NoError(t, err)
	assert.False(c.IsEmpty())
	c, err = store.Get(context.Background(), c3.Hash())
	require.NoError(t, err)
	assert.False(c.IsEmpty())
	assert.Equal(uint64(3), stats(store).GetLatency.Samples())
	assert.Equal(uint64(0), stats(store).FileReadLatency.Samples())
	assert.Equal(uint64(3), stats(store).ChunksPerGet.Sum())

	h, err := store.Root(context.Background())
	require.NoError(t, err)
	_, err = store.Commit(context.Background(), h, h)
	require.NoError(t, err)

	// Commit will update the manifest
	assert.EqualValues(1, stats(store).WriteManifestLatency.Samples())
	assert.EqualValues(1, stats(store).CommitLatency.Samples())

	// Now we have write IO
	assert.Equal(uint64(1), stats(store).PersistLatency.Samples())
	assert.Equal(uint64(3), stats(store).ChunksPerPersist.Sum())
	assert.Equal(uint64(131), stats(store).BytesPerPersist.Sum())

	// Now some gets that will incur read IO
	_, err = store.Get(context.Background(), c1.Hash())
	require.NoError(t, err)
	_, err = store.Get(context.Background(), c2.Hash())
	require.NoError(t, err)
	_, err = store.Get(context.Background(), c3.Hash())
	require.NoError(t, err)
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
	err = store.GetMany(context.Background(), hashes.HashSet(), func(ctx context.Context, c *chunks.Chunk) {
		select {
		case chunkChan <- c:
		case <-ctx.Done():
		}
	})
	require.NoError(t, err)
	assert.Equal(uint64(4), stats(store).FileReadLatency.Samples())
	assert.Equal(uint64(54), stats(store).FileBytesPerRead.Sum())

	// Force a conjoin
	store.conjoiner = inlineConjoiner{2}
	err = store.Put(context.Background(), c4, noopGetAddrs)
	require.NoError(t, err)
	h, err = store.Root(context.Background())
	require.NoError(t, err)
	_, err = store.Commit(context.Background(), h, h)
	require.NoError(t, err)

	err = store.Put(context.Background(), c5, noopGetAddrs)
	require.NoError(t, err)
	h, err = store.Root(context.Background())
	require.NoError(t, err)
	_, err = store.Commit(context.Background(), h, h)
	require.NoError(t, err)

	assert.Equal(uint64(1), stats(store).ConjoinLatency.Samples())
	// TODO: Once random conjoin hack is out, test other conjoin stats
}
