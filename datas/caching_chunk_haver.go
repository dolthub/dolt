// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/hash"
)

type cachingChunkHaver struct {
	backing  chunks.ChunkSource
	hasCache map[hash.Hash]bool
	mu       *sync.Mutex
}

func newCachingChunkHaver(cs chunks.ChunkSource) *cachingChunkHaver {
	return &cachingChunkHaver{cs, map[hash.Hash]bool{}, &sync.Mutex{}}
}

func (ccs *cachingChunkHaver) Has(r hash.Hash) bool {
	if has, ok := checkCache(ccs, r); ok {
		return has
	}
	has := ccs.backing.Has(r)
	setCache(ccs, r, has)
	return has
}

func checkCache(ccs *cachingChunkHaver, r hash.Hash) (has, ok bool) {
	ccs.mu.Lock()
	defer ccs.mu.Unlock()
	has, ok = ccs.hasCache[r]
	return
}

func setCache(ccs *cachingChunkHaver, r hash.Hash, has bool) {
	ccs.mu.Lock()
	defer ccs.mu.Unlock()
	ccs.hasCache[r] = has
}
