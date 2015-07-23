package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// commitCache maintains an in-memory cache of all known commits.
type commitCache struct {
	source chunks.ChunkSource
	refs   map[ref.Ref]bool
}

func (cache *commitCache) updateFromCommit(commit Commit) {
	if _, ok := cache.refs[commit.Ref()]; ok {
		return
	}

	parents := commit.Parents()
	parents.Iter(func(commit types.Value) (stop bool) {
		cache.updateFromCommit(CommitFromVal(commit))
		return
	})
	cache.refs[commit.Ref()] = true
}

func (cache *commitCache) Update(currentCommits SetOfCommit) {
	if currentCommits.Len() == 0 {
		return
	}

	commitsRef := currentCommits.Ref()
	if _, ok := cache.refs[commitsRef]; ok {
		return
	}

	commitSet := SetOfCommit{types.MustReadValue(commitsRef, cache.source).(types.Set)}
	commitSet.Iter(func(commit Commit) (stop bool) {
		cache.updateFromCommit(commit)
		return
	})
	cache.refs[commitsRef] = true
}

func (cache *commitCache) Contains(candidate ref.Ref) bool {
	_, ok := cache.refs[candidate]
	return ok
}

func NewCommitCache(source chunks.ChunkSource) *commitCache {
	return &commitCache{
		source,
		make(map[ref.Ref]bool),
	}
}
