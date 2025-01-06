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

package remotestorage

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

func genRandomChunks(rng *rand.Rand, n int) (hash.HashSet, []nbs.ToChunker) {
	chks := make([]nbs.ToChunker, n)
	hashes := make(hash.HashSet)
	for i := 0; i < n; i++ {
		size := int(rng.Int31n(99) + 1)
		bytes := make([]byte, size)
		for j := 0; j < size; j++ {
			bytes[j] = byte(rng.Int31n(255))
		}

		chk := nbs.ChunkToCompressedChunk(chunks.NewChunk(bytes))
		chks[i] = chk

		hashes[chk.Hash()] = struct{}{}
	}

	return hashes, chks
}

func TestMapChunkCache(t *testing.T) {
	const chunkBatchSize = 10

	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	hashes, chks := genRandomChunks(rng, chunkBatchSize)

	mapChunkCache := newMapChunkCache()
	mapChunkCache.Put(chks)
	hashToChunk := mapChunkCache.Get(hashes)

	assert.Equal(t, len(hashToChunk), chunkBatchSize, "Did not read back all chunks (seed %d)", seed)

	absent := mapChunkCache.Has(hashes)

	assert.Equal(t, len(absent), 0, "Missing chunks that were added (seed %d)", seed)

	toFlush := mapChunkCache.GetAndClearChunksToFlush()

	assert.True(t, reflect.DeepEqual(toFlush, hashToChunk), "unexpected or missing chunks to flush (seed %d)", seed)

	moreHashes, moreChks := genRandomChunks(rng, chunkBatchSize)

	joinedHashes := make(hash.HashSet)

	for h := range hashes {
		joinedHashes[h] = struct{}{}
	}

	for h := range moreHashes {
		joinedHashes[h] = struct{}{}
	}

	absent = mapChunkCache.Has(joinedHashes)

	assert.True(t, reflect.DeepEqual(absent, moreHashes), "unexpected absent hashset (seed %d)", seed)

	mapChunkCache.PutChunk(chks[0])
	mapChunkCache.PutChunk(moreChks[0])

	toFlush = mapChunkCache.GetAndClearChunksToFlush()

	expected := map[hash.Hash]nbs.ToChunker{moreChks[0].Hash(): moreChks[0]}
	eq := reflect.DeepEqual(toFlush, expected)
	assert.True(t, eq, "Missing or unexpected chunks to flush (seed %d)", seed)
}
