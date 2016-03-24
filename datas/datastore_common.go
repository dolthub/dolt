package datas

import (
	"errors"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type dataStoreCommon struct {
	cachingValueStore
	rootRef  ref.Ref
	datasets *MapOfStringToRefOfCommit
}

var (
	ErrOptimisticLockFailed = errors.New("Optimistic lock failed on datastore Root update")
	ErrMergeNeeded          = errors.New("Dataset head is not ancestor of commit")
)

func newDataStoreCommon(hcs hintedChunkStore) dataStoreCommon {
	return dataStoreCommon{cachingValueStore: newCachingValueStore(hcs), rootRef: hcs.Root()}
}

func (ds *dataStoreCommon) MaybeHead(datasetID string) (Commit, bool) {
	if r, ok := ds.Datasets().MaybeGet(datasetID); ok {
		return r.TargetValue(ds), true
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
		if ds.rootRef.IsEmpty() {
			emptySet := NewMapOfStringToRefOfCommit()
			ds.datasets = &emptySet
		} else {
			ds.datasets = ds.datasetsFromRef(ds.rootRef)
		}
	}

	return *ds.datasets
}

func (ds *dataStoreCommon) datasetsFromRef(datasetsRef ref.Ref) *MapOfStringToRefOfCommit {
	c := ds.ReadValue(datasetsRef).(MapOfStringToRefOfCommit)
	return &c
}

func (ds *dataStoreCommon) Close() error {
	return ds.hcs.Close()
}

func (ds *dataStoreCommon) commit(datasetID string, commit Commit) error {
	return ds.doCommit(datasetID, commit)
}

// doCommit manages concurrent access the single logical piece of mutable state: the current Root. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootRef is the ref of the current head. The call to UpdateRoot below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again. This method will also fail and return an 'ErrMergeNeeded' error if the |commit| is not a descendent of the current dataset head
func (ds *dataStoreCommon) doCommit(datasetID string, commit Commit) error {
	currentRootRef, currentDatasets := ds.getRootAndDatasets()

	// TODO: This Commit will be orphaned if the tryUpdateRoot() below fails
	commitRef := ds.WriteValue(commit).(RefOfCommit)

	// First commit in store is always fast-foward.
	if !currentRootRef.IsEmpty() {
		var currentHeadRef RefOfCommit
		currentHeadRef, hasHead := currentDatasets.MaybeGet(datasetID)

		// First commit in dataset is always fast-foward.
		if hasHead {
			// Allow only fast-forward commits.
			if commitRef.Equals(currentHeadRef) {
				return nil
			}
			if !descendsFrom(commit, currentHeadRef, ds) {
				return ErrMergeNeeded
			}
		}
	}
	currentDatasets = currentDatasets.Set(datasetID, commitRef)
	return ds.tryUpdateRoot(currentDatasets, currentRootRef)
}

// doDelete manages concurrent access the single logical piece of mutable state: the current Root. doDelete is optimistic in that it is attempting to update head making the assumption that currentRootRef is the ref of the current head. The call to UpdateRoot below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again.
func (ds *dataStoreCommon) doDelete(datasetID string) error {
	currentRootRef, currentDatasets := ds.getRootAndDatasets()
	currentDatasets = currentDatasets.Remove(datasetID)
	return ds.tryUpdateRoot(currentDatasets, currentRootRef)
}

func (ds *dataStoreCommon) getRootAndDatasets() (currentRootRef ref.Ref, currentDatasets MapOfStringToRefOfCommit) {
	currentRootRef = ds.hcs.Root()
	currentDatasets = ds.Datasets()

	if currentRootRef != currentDatasets.Ref() && !currentRootRef.IsEmpty() {
		// The root has been advanced.
		currentDatasets = *ds.datasetsFromRef(currentRootRef)
	}
	return
}

func (ds *dataStoreCommon) tryUpdateRoot(currentDatasets MapOfStringToRefOfCommit, currentRootRef ref.Ref) (err error) {
	// TODO: This Commit will be orphaned if the UpdateRoot below fails
	newRootRef := ds.WriteValue(currentDatasets).TargetRef()
	// If the root has been updated by another process in the short window since we read it, this call will fail. See issue #404
	if !ds.hcs.UpdateRoot(newRootRef, currentRootRef) {
		err = ErrOptimisticLockFailed
	}
	return
}

func descendsFrom(commit Commit, currentHeadRef RefOfCommit, vr types.ValueReader) bool {
	// BFS because the common case is that the ancestor is only a step or two away
	ancestors := commit.Parents()
	for !ancestors.Has(currentHeadRef) {
		if ancestors.Empty() {
			return false
		}
		ancestors = getAncestors(ancestors, vr)
	}
	return true
}

func getAncestors(commits SetOfRefOfCommit, vr types.ValueReader) SetOfRefOfCommit {
	ancestors := NewSetOfRefOfCommit()
	commits.IterAll(func(r RefOfCommit) {
		c := r.TargetValue(vr)
		ancestors = ancestors.Union(c.Parents())
	})
	return ancestors
}
