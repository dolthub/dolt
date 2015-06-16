package commit

import (
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/store"
	"github.com/attic-labs/noms/types"
)

// TODO(rafael):
type memcacheReachable struct {
	source store.ChunkSource
	refs   map[string]bool
}

func (cache *memcacheReachable) updateFromCommitMap(commitMap types.Map) {
	refString := commitMap.Ref().String()
	if _, ok := cache.refs[refString]; ok {
		return
	}

	parents := commitMap.Get(types.NewString("parents")).(types.Set)
	parents.Iter(func(commit types.Value) (stop bool) {
		cache.updateFromCommitMap(commit.(types.Map))
		return false
	})
	cache.refs[refString] = true
}

func (cache *memcacheReachable) updateFromRootSet(rootRef ref.Ref) {
	refString := rootRef.String()
	if _, ok := cache.refs[refString]; ok {
		return
	}

	// TODO(rafael): This is super-inefficient with eager ReadValue and no caching
	rootSet := enc.MustReadValue(rootRef, cache.source).(types.Set)
	rootSet.Iter(func(commit types.Value) (stop bool) {
		cache.updateFromCommitMap(commit.(types.Map))
		return false
	})
	cache.refs[refString] = true
}

func (cache *memcacheReachable) IsSupercededFrom(candidate, root ref.Ref) bool {
	Chk.NotEqual(candidate, root)
	if (root == ref.Ref{}) {
		return false
	}

	cache.updateFromRootSet(root)
	_, ok := cache.refs[candidate.String()]
	return ok
}

func NewMemCacheReachable(source store.ChunkSource) Reachable {
	return &memcacheReachable{
		source,
		make(map[string]bool),
	}
}
