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
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// mapChunkCache is a simple ChunkCache implementation that stores
// cached chunks and has records in two separate lru caches.
type mapChunkCache struct {
	chunks *lru.TwoQueueCache[hash.Hash, nbs.ToChunker]
	has    *lru.TwoQueueCache[hash.Hash, struct{}]
}

const defaultCacheChunkCapacity = 32 * 1024
const defaultCacheHasCapacity = 1024 * 1024

func newMapChunkCache() *mapChunkCache {
	return NewMapChunkCacheWithCapacity(defaultCacheChunkCapacity, defaultCacheHasCapacity)
}

func NewMapChunkCacheWithCapacity(maxChunkCapacity, maxHasCapacity int) *mapChunkCache {
	chunks, err := lru.New2Q[hash.Hash, nbs.ToChunker](maxChunkCapacity)
	if err != nil {
		panic(err)
	}
	has, err := lru.New2Q[hash.Hash, struct{}](maxHasCapacity)
	if err != nil {
		panic(err)
	}
	return &mapChunkCache{
		chunks,
		has,
	}
}

func (cache *mapChunkCache) InsertChunks(cs []nbs.ToChunker) {
	for _, c := range cs {
		cache.chunks.Add(c.Hash(), c)
	}
}

func (cache *mapChunkCache) GetCachedChunks(hs hash.HashSet) map[hash.Hash]nbs.ToChunker {
	ret := make(map[hash.Hash]nbs.ToChunker)
	for h := range hs {
		c, ok := cache.chunks.Get(h)
		if ok {
			ret[h] = c
		}
	}
	return ret
}

func (cache *mapChunkCache) InsertHas(hs hash.HashSet) {
	for h := range hs {
		cache.has.Add(h, struct{}{})
	}
}

func (cache *mapChunkCache) GetCachedHas(hs hash.HashSet) (absent hash.HashSet) {
	ret := make(hash.HashSet)
	for h := range hs {
		if !cache.has.Contains(h) {
			ret.Insert(h)
		}
	}
	return ret
}
