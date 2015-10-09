package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type dataStoreCommon struct {
	chunks.ChunkStore
	head *Commit
}

func commitFromRef(commitRef ref.Ref, cs chunks.ChunkSource) *Commit {
	c := CommitFromVal(types.ReadValue(commitRef, cs))
	return &c
}

func (ds *dataStoreCommon) MaybeHead() (Commit, bool) {
	if ds.head == nil {
		return NewCommit(), false
	}
	return *ds.head, true
}

func (ds *dataStoreCommon) Head() Commit {
	c, ok := ds.MaybeHead()
	d.Chk.True(ok, "DataStore has no Head.")
	return c
}

func (ds *dataStoreCommon) commit(v types.Value) bool {
	p := NewSetOfCommit()
	if head, ok := ds.MaybeHead(); ok {
		p = p.Insert(head)
	}
	return ds.commitWithParents(v, p)
}

func (ds *dataStoreCommon) commitWithParents(v types.Value, p SetOfCommit) bool {
	return ds.doCommit(NewCommit().SetParents(p).SetValue(v))
}

// doCommit manages concurrent access the single logical piece of mutable state: the current head. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootRef is the ref of the current head. The call to UpdateRoot below will fail if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again.
func (ds *dataStoreCommon) doCommit(commit Commit) bool {
	currentRootRef := ds.Root()

	// Note: |currentHead| may be different from ds.head and *must* be consistent with currentRootRef.
	// If currentRoot or currentHead is nil, then any commit is allowed.
	emptyRef := ref.Ref{}
	dsHeadRef := emptyRef
	if ds.head != nil {
		dsHeadRef = ds.head.Ref()
	}

	var currentHead Commit
	if currentRootRef != emptyRef {
		if currentRootRef == dsHeadRef {
			currentHead = *ds.head
		} else {
			currentHead = *commitFromRef(currentRootRef, ds)
		}

		// Allow only fast-forward commits.
		if commit.Equals(currentHead) {
			return true
		} else if !descendsFrom(commit, currentHead) {
			return false
		}
	}
	// TODO: This Commit will be orphaned if this UpdateRoot below fails
	newRootRef := types.WriteValue(commit.NomsValue(), ds)

	ok := ds.UpdateRoot(newRootRef, currentRootRef)
	return ok
}

func descendsFrom(commit, currentHead Commit) bool {
	// BFS because the common case is that the ancestor is only a step or two away
	ancestors := NewSetOfCommit().Insert(commit)
	for !ancestors.Has(currentHead) {
		if ancestors.Empty() {
			return false
		}
		ancestors = getAncestors(ancestors)
	}
	return true
}

func getAncestors(commits SetOfCommit) SetOfCommit {
	ancestors := NewSetOfCommit()
	commits.Iter(func(c Commit) (stop bool) {
		ancestors =
			ancestors.Union(c.Parents())
		return
	})
	return ancestors
}
