// Copyright 2025 Dolthub, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

func TestNoopWriteBuffer(t *testing.T) {
	cache := noopWriteBuffer{}
	err := cache.Put(nbs.CompressedChunk{})
	assert.NotNil(t, err)
	assert.Panics(t, func() {
		cache.GetAllForWrite()
	})
	assert.Panics(t, func() {
		cache.WriteCompleted(false)
	})
	cache.AddPendingChunks(make(hash.HashSet), make(map[hash.Hash]nbs.ToChunker))
	cache.RemovePresentChunks(make(hash.HashSet))
}

func TestMapWriteBuffer(t *testing.T) {
	t.Run("SmokeTest", func(t *testing.T) {
		// A bit of a typical usage...
		cache := newMapWriteBuffer()
		var seed [32]byte
		rand := rand.NewChaCha8(seed)
		query := make(hash.HashSet)
		for i := 0; i < 64; i++ {
			var bs [512]byte
			rand.Read(bs[:])
			query.Insert(hash.Of(bs[:]))
		}
		res := make(map[hash.Hash]nbs.ToChunker)
		cache.AddPendingChunks(query, res)
		assert.Len(t, res, 0)

		// Insert some chunks.
		inserted := make(hash.HashSet)
		for i := 0; i < 8; i++ {
			bs := make([]byte, 512)
			rand.Read(bs)
			chk := chunks.NewChunk(bs)
			cache.Put(nbs.ChunkToCompressedChunk(chk))
			inserted.Insert(chk.Hash())
		}
		cache.AddPendingChunks(query, res)
		assert.Len(t, res, 0)
		for h := range inserted {
			query.Insert(h)
		}
		cache.AddPendingChunks(query, res)
		assert.Len(t, res, 8)

		cache.RemovePresentChunks(query)
		assert.Len(t, query, 64)
		for h := range inserted {
			query.Insert(h)
		}

		// Cache continues working for reads during a pending write.
		toWrite := cache.GetAllForWrite()
		assert.Len(t, toWrite, 8)
		res = make(map[hash.Hash]nbs.ToChunker)
		cache.AddPendingChunks(query, res)
		assert.Len(t, res, 8)
		cache.RemovePresentChunks(query)
		assert.Len(t, query, 64)

		// After a failure, chunks are still present.
		cache.WriteCompleted(false)
		toWrite = cache.GetAllForWrite()
		assert.Len(t, toWrite, 8)
		// And after a success, they are cleared.
		cache.WriteCompleted(true)
		toWrite = cache.GetAllForWrite()
		assert.Len(t, toWrite, 0)
		cache.WriteCompleted(true)
	})
	t.Run("ConcurrentPuts", func(t *testing.T) {
		cache := newMapWriteBuffer()
		var seed [32]byte
		seedRand := rand.NewChaCha8(seed)
		const numThreads = 16
		var wg sync.WaitGroup
		// One thread is writing and failing...
		wg.Add(1)
		var writes int
		go func() {
			defer wg.Done()
			for {
				cached := cache.GetAllForWrite()
				writes += 1
				time.Sleep(5 * time.Millisecond)
				cache.WriteCompleted(false)
				if len(cached) == numThreads*32 {
					return
				}
				time.Sleep(100 * time.Microsecond)
			}
		}()
		wg.Add(numThreads)
		var inserted [numThreads][]hash.Hash
		for i := 0; i < numThreads; i++ {
			var seed [32]byte
			seedRand.Read(seed[:])
			randCha := rand.NewChaCha8(seed)
			go func() {
				for j := 0; j < 32; j++ {
					var bs [512]byte
					randCha.Read(bs[:])
					chk := chunks.NewChunk(bs[:])
					cache.Put(nbs.ChunkToCompressedChunk(chk))
					inserted[i] = append(inserted[i], chk.Hash())
				}
				defer wg.Done()
			}()
		}
		wg.Wait()
		// All writes failed. Let's make sure we have everything we expect.
		cached := cache.GetAllForWrite()
		assert.Len(t, cached, 32*numThreads)
		for i := range inserted {
			for _, h := range inserted[i] {
				_, ok := cached[h]
				assert.True(t, ok)
			}
		}
		cache.WriteCompleted(true)
	})
}
