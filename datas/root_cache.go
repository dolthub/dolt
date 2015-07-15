package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// rootCache maintains an in-memory cache of all known roots.
type rootCache struct {
	source chunks.ChunkSource
	refs   map[ref.Ref]bool
}

func (cache *rootCache) updateFromCommit(root Root) {
	if _, ok := cache.refs[root.Ref()]; ok {
		return
	}

	parents := root.Parents()
	parents.Iter(func(commit types.Value) (stop bool) {
		cache.updateFromCommit(RootFromVal(commit))
		return
	})
	cache.refs[root.Ref()] = true
}

func (cache *rootCache) Update(rootsRef ref.Ref) {
	if rootsRef == (ref.Ref{}) {
		return
	}

	if _, ok := cache.refs[rootsRef]; ok {
		return
	}

	rootSet := RootSet{types.MustReadValue(rootsRef, cache.source).(types.Set)}
	rootSet.Iter(func(commit Root) (stop bool) {
		cache.updateFromCommit(commit)
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
