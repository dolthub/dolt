package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type dataStoreCommon struct {
	chunks.ChunkStore
	datasets *MapOfStringToCommit
}

func datasetsFromRef(datasetsRef ref.Ref, cs chunks.ChunkSource) *MapOfStringToCommit {
	c := MapOfStringToCommitFromVal(types.ReadValue(datasetsRef, cs))
	return &c
}

func (ds *dataStoreCommon) MaybeHead(datasetID string) (Commit, bool) {
	if ds.datasets != nil {
		return ds.datasets.MaybeGet(datasetID)
	} else {
		return NewCommit(), false
	}
}

func (ds *dataStoreCommon) Head(datasetID string) Commit {
	c, ok := ds.MaybeHead(datasetID)
	d.Chk.True(ok, "DataStore has no Head.")
	return c
}

func (ds *dataStoreCommon) Datasets() MapOfStringToCommit {
	if ds.datasets == nil {
		return NewMapOfStringToCommit()
	} else {
		return *ds.datasets
	}
}

func (ds *dataStoreCommon) commit(datasetID string, commit Commit) bool {
	return ds.doCommit(datasetID, commit)
}

// doCommit manages concurrent access the single logical piece of mutable state: the current head. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootRef is the ref of the current head. The call to UpdateRoot below will fail if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again.
func (ds *dataStoreCommon) doCommit(datasetID string, commit Commit) bool {
	currentRootRef := ds.Root()
	var currentDatasets MapOfStringToCommit
	if ds.datasets != nil && currentRootRef == ds.datasets.Ref() {
		currentDatasets = *ds.datasets
	} else if !currentRootRef.IsEmpty() {
		currentDatasets = *datasetsFromRef(currentRootRef, ds)
	} else {
		currentDatasets = NewMapOfStringToCommit()
	}

	// First commit in store is always fast-foward.
	if !currentRootRef.IsEmpty() {
		var currentHead Commit
		currentHead, hasHead := currentDatasets.MaybeGet(datasetID)

		// First commit in dataset is always fast-foward.
		if hasHead {
			// Allow only fast-forward commits.
			if commit.Equals(currentHead) {
				return true
			} else if !descendsFrom(commit, currentHead) {
				return false
			}
		}
	}
	// TODO: This Commit will be orphaned if the UpdateRoot below fails
	currentDatasets = currentDatasets.Set(datasetID, commit)
	newRootRef := types.WriteValue(currentDatasets.NomsValue(), ds)

	// If the root has been updated by another process in the short window since we read it, this call will fail. See issue #404
	return ds.UpdateRoot(newRootRef, currentRootRef)
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
