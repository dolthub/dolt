package datastore

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// rootCache maintains an in-memory cache of all known roots.
type rootCache struct {
	source chunks.ChunkSource
	refs   map[ref.Ref]bool
}

func (cache *rootCache) updateFromCommit(commitMap types.Map) {
	if _, ok := cache.refs[commitMap.Ref()]; ok {
		return
	}

	parents := commitMap.Get(types.NewString("parents")).(types.Set)
	parents.Iter(func(commit types.Value) (stop bool) {
		cache.updateFromCommit(commit.(types.Map))
		return
	})
	cache.refs[commitMap.Ref()] = true
}

func (cache *rootCache) Update(rootsRef ref.Ref) {
	if rootsRef == (ref.Ref{}) {
		return
	}

	if _, ok := cache.refs[rootsRef]; ok {
		return
	}

	// BUG 11: This is super-inefficient with eager ReadValue and no caching
	rootSet := enc.MustReadValue(rootsRef, cache.source).(types.Set)
	rootSet.Iter(func(commit types.Value) (stop bool) {
		cache.updateFromCommit(commit.(types.Map))
		return
	})
	cache.refs[rootsRef] = true
}

func (cache *rootCache) Contains(candidate ref.Ref) bool {
	_, ok := cache.refs[candidate]
	return ok
}

func NewRootCache(source chunks.ChunkSource) *rootCache {
	return &rootCache{
		source,
		make(map[ref.Ref]bool),
	}
}
