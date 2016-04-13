package datas

import (
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

type cachingChunkHaver struct {
	backing  chunks.ChunkSource
	hasCache map[ref.Ref]bool
	mu       *sync.Mutex
}

func newCachingChunkHaver(cs chunks.ChunkSource) *cachingChunkHaver {
	return &cachingChunkHaver{cs, map[ref.Ref]bool{}, &sync.Mutex{}}
}

func (ccs *cachingChunkHaver) Has(r ref.Ref) bool {
	if has, ok := checkCache(ccs, r); ok {
		return has
	}
	has := ccs.backing.Has(r)
	setCache(ccs, r, has)
	return has
}

func checkCache(ccs *cachingChunkHaver, r ref.Ref) (has, ok bool) {
	ccs.mu.Lock()
	defer ccs.mu.Unlock()
	has, ok = ccs.hasCache[r]
	return
}

func setCache(ccs *cachingChunkHaver, r ref.Ref, has bool) {
	ccs.mu.Lock()
	defer ccs.mu.Unlock()
	ccs.hasCache[r] = has
}
