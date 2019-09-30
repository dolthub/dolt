// Copyright 2019 Liquidata, Inc.
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
	"sync"

	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

// mapChunkCache is a ChunkCache implementation that stores everything in an in memory map.
type mapChunkCache struct {
	mu          *sync.Mutex
	hashToChunk map[hash.Hash]chunks.Chunkable
	toFlush     map[hash.Hash]chunks.Chunkable
}

func newMapChunkCache() *mapChunkCache {
	return &mapChunkCache{
		&sync.Mutex{},
		make(map[hash.Hash]chunks.Chunkable),
		make(map[hash.Hash]chunks.Chunkable),
	}
}

// Put puts a slice of chunks into the cache.
func (mcc *mapChunkCache) Put(chnks []chunks.Chunkable) {
	mcc.mu.Lock()
	defer mcc.mu.Unlock()

	for i := 0; i < len(chnks); i++ {
		c := chnks[i]
		h := c.Hash()

		if curr, ok := mcc.hashToChunk[h]; ok {
			if !curr.IsEmpty() {
				continue
			}
		}

		mcc.hashToChunk[h] = c

		if !c.IsEmpty() {
			mcc.toFlush[h] = c
		}
	}
}

// Get gets a map of hash to chunk for a set of hashes.  In the event that a chunk is not in the cache, chunks.Empty.
// is put in it's place
func (mcc *mapChunkCache) Get(hashes hash.HashSet) map[hash.Hash]chunks.Chunkable {
	hashToChunk := make(map[hash.Hash]chunks.Chunkable)

	mcc.mu.Lock()
	defer mcc.mu.Unlock()

	for h := range hashes {
		if c, ok := mcc.hashToChunk[h]; ok {
			hashToChunk[h] = c
		} else {
			hashToChunk[h] = chunks.EmptyChunk
		}
	}

	return hashToChunk
}

// Has takes a set of hashes and returns the set of hashes that the cache currently does not have in it.
func (mcc *mapChunkCache) Has(hashes hash.HashSet) (absent hash.HashSet) {
	absent = make(hash.HashSet)

	mcc.mu.Lock()
	defer mcc.mu.Unlock()

	for h := range hashes {
		if _, ok := mcc.hashToChunk[h]; !ok {
			absent[h] = struct{}{}
		}
	}

	return absent
}

// PutChunk puts a single chunk in the cache.  true returns in the event that the chunk was cached successfully
// and false is returned if that chunk is already is the cache.
func (mcc *mapChunkCache) PutChunk(ch chunks.Chunkable) bool {
	mcc.mu.Lock()
	defer mcc.mu.Unlock()

	h := ch.Hash()
	if existing, ok := mcc.hashToChunk[h]; !ok || existing.IsEmpty() {
		mcc.hashToChunk[h] = ch
		mcc.toFlush[h] = ch
		return true
	}

	return false
}

// GetAndClearChunksToFlush gets a map of hash to chunk which includes all the chunks that were put in the cache
// between the last time GetAndClearChunksToFlush was called and now.
func (mcc *mapChunkCache) GetAndClearChunksToFlush() map[hash.Hash]chunks.Chunkable {
	newToFlush := make(map[hash.Hash]chunks.Chunkable)

	mcc.mu.Lock()
	defer mcc.mu.Unlock()

	toFlush := mcc.toFlush
	mcc.toFlush = newToFlush

	return toFlush
}
