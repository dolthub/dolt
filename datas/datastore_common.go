package datas

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type dataStoreCommon struct {
	chunks.ChunkStore
	datasets *MapOfStringToRefOfCommit
}

func datasetsFromRef(datasetsRef ref.Ref, cs chunks.ChunkSource) *MapOfStringToRefOfCommit {
	c := MapOfStringToRefOfCommitFromVal(types.ReadValue(datasetsRef, cs))
	return &c
}

func (ds *dataStoreCommon) MaybeHead(datasetID string) (Commit, bool) {
	if ds.datasets != nil {
		if r, ok := ds.datasets.MaybeGet(datasetID); ok {
			return r.GetValue(ds), true
		}
	}
	return NewCommit(), false
}

func (ds *dataStoreCommon) Head(datasetID string) Commit {
	c, ok := ds.MaybeHead(datasetID)
	d.Chk.True(ok, "DataStore has no Head.")
	return c
}

func (ds *dataStoreCommon) Datasets() MapOfStringToRefOfCommit {
	if ds.datasets == nil {
		return NewMapOfStringToRefOfCommit()
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
	var currentDatasets MapOfStringToRefOfCommit
	if ds.datasets != nil && currentRootRef == ds.datasets.Ref() {
		currentDatasets = *ds.datasets
	} else if !currentRootRef.IsEmpty() {
		currentDatasets = *datasetsFromRef(currentRootRef, ds)
	} else {
		currentDatasets = NewMapOfStringToRefOfCommit()
	}

	// TODO: This Commit will be orphaned if the UpdateRoot below fails
	commitRef := NewRefOfCommit(types.WriteValue(commit, ds))

	// First commit in store is always fast-foward.
	if !currentRootRef.IsEmpty() {
		var currentHeadRef RefOfCommit
		currentHeadRef, hasHead := currentDatasets.MaybeGet(datasetID)

		// First commit in dataset is always fast-foward.
		if hasHead {
			// Allow only fast-forward commits.
			if commitRef.Equals(currentHeadRef) {
				return true
			}
			if !descendsFrom(commitRef, currentHeadRef, ds) {
				return false
			}
		}
	}
	// TODO: This Commit will be orphaned if the UpdateRoot below fails
	currentDatasets = currentDatasets.Set(datasetID, commitRef)
	newRootRef := types.WriteValue(currentDatasets, ds)

	// If the root has been updated by another process in the short window since we read it, this call will fail. See issue #404
	return ds.UpdateRoot(newRootRef, currentRootRef)
}

func descendsFrom(commitRef, currentHeadRef RefOfCommit, cs chunks.ChunkSource) bool {
	// BFS because the common case is that the ancestor is only a step or two away
	ancestors := NewSetOfRefOfCommit().Insert(commitRef)
	for !ancestors.Has(currentHeadRef) {
		if ancestors.Empty() {
			return false
		}
		ancestors = getAncestors(ancestors, cs)
	}
	return true
}

func getAncestors(commits SetOfRefOfCommit, cs chunks.ChunkSource) SetOfRefOfCommit {
	ancestors := NewSetOfRefOfCommit()
	commits.Iter(func(r RefOfCommit) (stop bool) {
		c := r.GetValue(cs)
		ancestors =
			ancestors.Union(c.Parents())
		return
	})
	return ancestors
}
