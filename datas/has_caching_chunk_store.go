package datas

import (
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

type hasCachingChunkStore struct {
	backing  chunks.ChunkStore
	hasCache map[ref.Ref]bool
	mu       *sync.Mutex
}

func newHasCachingChunkStore(cs chunks.ChunkStore) *hasCachingChunkStore {
	return &hasCachingChunkStore{cs, map[ref.Ref]bool{}, &sync.Mutex{}}
}

// Backing returns the non-caching ChunkStore that backs this instance. Helpful when creating and returing a new DataStore instance that shouldn't share a cache with DataStore that already exists.
func (ccs *hasCachingChunkStore) Backing() chunks.ChunkStore {
	return ccs.backing
}

func (ccs *hasCachingChunkStore) Root() ref.Ref {
	return ccs.backing.Root()
}

func (ccs *hasCachingChunkStore) UpdateRoot(current, last ref.Ref) bool {
	return ccs.backing.UpdateRoot(current, last)
}

func (ccs *hasCachingChunkStore) Has(r ref.Ref) bool {
	has, ok := checkCache(ccs, r)
	if ok {
		return has
	}
	has = ccs.backing.Has(r)
	setCache(ccs, r, has)
	return has
}

func (ccs *hasCachingChunkStore) Get(r ref.Ref) chunks.Chunk {
	has, ok := checkCache(ccs, r)
	if ok && !has {
		return chunks.EmptyChunk
	}
	c := ccs.backing.Get(r)
	setCache(ccs, r, !c.IsEmpty())
	return c
}

func (ccs *hasCachingChunkStore) Put(c chunks.Chunk) {
	r := c.Ref()
	has, _ := checkCache(ccs, r)
	if has {
		return
	}
	ccs.backing.Put(c)
	setCache(ccs, r, true)
}

func (cs *hasCachingChunkStore) Close() error {
	return cs.backing.Close()
}

func checkCache(ccs *hasCachingChunkStore, r ref.Ref) (has, ok bool) {
	ccs.mu.Lock()
	defer ccs.mu.Unlock()
	has, ok = ccs.hasCache[r]
	return
}

func setCache(ccs *hasCachingChunkStore, r ref.Ref, has bool) {
	ccs.mu.Lock()
	defer ccs.mu.Unlock()
	ccs.hasCache[r] = has
}
