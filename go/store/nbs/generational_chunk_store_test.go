// Copyright 2021 Dolthub, Inc.
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

package nbs

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

var randGen = rand.New(rand.NewSource(0))

func genChunks(t *testing.T, count int, max int) []chunks.Chunk {
	chnks := make([]chunks.Chunk, count)
	for i := 0; i < count; i++ {
		bytes := make([]byte, randGen.Int()%max)
		n, err := randGen.Read(bytes)
		require.NoError(t, err)
		chnks[i] = chunks.NewChunk(bytes[:n])
	}

	return chnks
}

func mergeMaps(m1, m2 map[int]bool) map[int]bool {
	m3 := make(map[int]bool)
	for k := range m1 {
		m3[k] = true
	}

	for k := range m2 {
		m3[k] = true
	}

	return m3
}

func hashesForChunks(chunks []chunks.Chunk, indexes map[int]bool) hash.HashSet {
	hashes := make(hash.HashSet)
	for idx := range indexes {
		hashes[chunks[idx].Hash()] = struct{}{}
	}

	return hashes
}

type foundHashes hash.HashSet

func (fh foundHashes) found(ctx context.Context, chk *chunks.Chunk) {
	fh[chk.Hash()] = struct{}{}
}

func requireChunks(t *testing.T, ctx context.Context, chunks []chunks.Chunk, genCS *GenerationalNBS, inOld, inNew map[int]bool) {
	// Has/Get Checks
	for i, chk := range chunks {
		has, err := genCS.oldGen.Has(ctx, chk.Hash())
		require.NoError(t, err)
		require.Equal(t, inOld[i], has, "error for index: %d", i)

		retrieved, err := genCS.oldGen.Get(ctx, chk.Hash())
		require.NoError(t, err)
		require.Equal(t, !inOld[i], retrieved.IsEmpty(), "error for index: %d", i)

		has, err = genCS.newGen.Has(ctx, chk.Hash())
		require.NoError(t, err)
		require.Equal(t, inNew[i], has, "error for index: %d", i)

		retrieved, err = genCS.newGen.Get(ctx, chk.Hash())
		require.NoError(t, err)
		require.Equal(t, !inNew[i], retrieved.IsEmpty(), "error for index: %d", i)

		has, err = genCS.Has(ctx, chk.Hash())
		require.NoError(t, err)
		require.Equal(t, inOld[i] || inNew[i], has, "error for index: %d", i)

		retrieved, err = genCS.Get(ctx, chk.Hash())
		require.NoError(t, err)
		require.Equal(t, !(inOld[i] || inNew[i]), retrieved.IsEmpty(), "error for index: %d", i)
	}

	// HasMany Checks
	absent, err := genCS.oldGen.HasMany(ctx, hashesForChunks(chunks, inOld))
	require.NoError(t, err)
	require.Len(t, absent, 0)

	absent, err = genCS.newGen.HasMany(ctx, hashesForChunks(chunks, inNew))
	require.NoError(t, err)
	require.Len(t, absent, 0)

	inUnion := mergeMaps(inOld, inNew)
	absent, err = genCS.HasMany(ctx, hashesForChunks(chunks, inUnion))
	require.NoError(t, err)
	require.Len(t, absent, 0)

	// GetMany Checks
	expected := hashesForChunks(chunks, inOld)
	received := foundHashes{}
	err = genCS.oldGen.GetMany(ctx, expected, received.found)
	require.NoError(t, err)
	require.Equal(t, expected, hash.HashSet(received))

	expected = hashesForChunks(chunks, inNew)
	received = foundHashes{}
	err = genCS.newGen.GetMany(ctx, expected, received.found)
	require.NoError(t, err)
	require.Equal(t, expected, hash.HashSet(received))

	expected = hashesForChunks(chunks, inUnion)
	received = foundHashes{}
	err = genCS.GetMany(ctx, expected, received.found)
	require.NoError(t, err)
	require.Equal(t, expected, hash.HashSet(received))
}

func putChunks(t *testing.T, ctx context.Context, chunks []chunks.Chunk, cs chunks.ChunkStore, indexesIn map[int]bool, chunkIndexes ...int) {
	for _, idx := range chunkIndexes {
		err := cs.Put(ctx, chunks[idx])
		require.NoError(t, err)
		indexesIn[idx] = true
	}
}

func TestGenerationalCS(t *testing.T) {
	ctx := context.Background()
	oldGen, _, _ := makeTestLocalStore(t, 64)
	newGen, _, _ := makeTestLocalStore(t, 64)
	inOld := make(map[int]bool)
	inNew := make(map[int]bool)
	chnks := genChunks(t, 100, 1000)

	putChunks(t, ctx, chnks, oldGen, inOld, 0, 1, 2, 3, 4)

	cs := NewGenerationalCS(oldGen, newGen)
	requireChunks(t, ctx, chnks, cs, inOld, inNew)

	putChunks(t, ctx, chnks, cs, inNew, 6, 7, 8, 9)
	requireChunks(t, ctx, chnks, cs, inOld, inNew)

	err := cs.copyToOldGen(ctx, hashesForChunks(chnks, inNew))
	require.NoError(t, err)

	inOld = mergeMaps(inOld, inNew)
	requireChunks(t, ctx, chnks, cs, inOld, inNew)

	putChunks(t, ctx, chnks, cs, inNew, 10, 11, 12, 13, 14)
	requireChunks(t, ctx, chnks, cs, inOld, inNew)

	err = cs.copyToOldGen(ctx, hashesForChunks(chnks, inNew))
	require.NoError(t, err)

	inOld = mergeMaps(inOld, inNew)
	requireChunks(t, ctx, chnks, cs, inOld, inNew)

	putChunks(t, ctx, chnks, cs, inNew, 15, 16, 17, 18, 19)
	requireChunks(t, ctx, chnks, cs, inOld, inNew)
}
