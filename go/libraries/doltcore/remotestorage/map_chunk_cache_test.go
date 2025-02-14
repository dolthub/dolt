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
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

func TestMapChunkCache(t *testing.T) {
	t.Run("New", func(t *testing.T) {
		assert.NotNil(t, newMapChunkCache())
		assert.NotNil(t, NewMapChunkCacheWithCapacity(32, 32))
		assert.Panics(t, func() {
			assert.NotNil(t, NewMapChunkCacheWithCapacity(-1, 32))
		})
		assert.Panics(t, func() {
			assert.NotNil(t, NewMapChunkCacheWithCapacity(32, -1))
		})
	})
	t.Run("CachesChunks", func(t *testing.T) {
		var seed [32]byte
		rand := rand.NewChaCha8(seed)
		cache := NewMapChunkCacheWithCapacity(8, 8)
		inserted := make(hash.HashSet)
		// Insert some chunks.
		for i := 0; i < 8; i++ {
			bs := make([]byte, 512)
			rand.Read(bs)
			chk := chunks.NewChunk(bs)
			inserted.Insert(chk.Hash())
			cc := nbs.ChunkToCompressedChunk(chk)
			cache.InsertChunks([]nbs.ToChunker{cc})
		}

		// Query for those chunks, plus some that were not inserted.
		query := make(hash.HashSet)
		for h := range inserted {
			query.Insert(h)
		}
		for i := 0; i < 8; i++ {
			var bs [512]byte
			rand.Read(bs[:])
			query.Insert(hash.Of(bs[:]))
		}

		// Only got back the inserted chunks...
		cached := cache.GetCachedChunks(query)
		assert.Len(t, cached, 8)

		// If we insert more than our max size, and query
		// for everything inserted, we get back a result
		// set matching our max size.
		for i := 0; i < 64; i++ {
			bs := make([]byte, 512)
			rand.Read(bs)
			chk := chunks.NewChunk(bs)
			inserted.Insert(chk.Hash())
			cc := nbs.ChunkToCompressedChunk(chk)
			cache.InsertChunks([]nbs.ToChunker{cc})
		}
		cached = cache.GetCachedChunks(inserted)
		assert.Len(t, cached, 8)
	})
	t.Run("CachesHasRecords", func(t *testing.T) {
		var seed [32]byte
		rand := rand.NewChaCha8(seed)
		cache := NewMapChunkCacheWithCapacity(8, 8)
		query := make(hash.HashSet)
		for i := 0; i < 64; i++ {
			var bs [512]byte
			rand.Read(bs[:])
			query.Insert(hash.Of(bs[:]))
		}

		// Querying an empty cache returns all the hashes.
		res := cache.GetCachedHas(query)
		assert.NotSame(t, res, query)
		assert.Len(t, res, 64)
		for h := range query {
			_, ok := res[h]
			assert.True(t, ok, "everything in query is in res")
		}

		// Insert 8 of our query hashes into the cache.
		insert := make(hash.HashSet)
		insertTwo := make(hash.HashSet)
		i := 0
		for h := range query {
			if i < 8 {
				insert.Insert(h)
			} else {
				insertTwo.Insert(h)
			}
			i += 1
			if i == 16 {
				break
			}
		}
		cache.InsertHas(insert)

		// Querying our original query set returns expected results.
		res = cache.GetCachedHas(query)
		assert.Len(t, res, 64-8)
		for h := range query {
			if _, ok := insert[h]; !ok {
				_, ok = res[h]
				assert.True(t, ok, "everything in query that is not in insert is in res")
			}
		}

		// Inserting another 8 hashes hits max limit. Only 8 records cached.
		cache.InsertHas(insertTwo)
		res = cache.GetCachedHas(query)
		assert.Len(t, res, 64-8)
	})
}
