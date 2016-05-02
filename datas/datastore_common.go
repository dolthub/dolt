package datas

import (
	"errors"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

type dataStoreCommon struct {
	*types.ValueStore
	bs       types.BatchStore
	rt       chunks.RootTracker
	rootRef  ref.Ref
	datasets *types.Map
}

var (
	ErrOptimisticLockFailed = errors.New("Optimistic lock failed on datastore Root update")
	ErrMergeNeeded          = errors.New("Dataset head is not ancestor of commit")
)

func newDataStoreCommon(bs types.BatchStore, rt chunks.RootTracker) dataStoreCommon {
	return dataStoreCommon{ValueStore: types.NewValueStore(bs), bs: bs, rt: rt, rootRef: rt.Root()}
}

func (ds *dataStoreCommon) MaybeHead(datasetID string) (types.Struct, bool) {
	if r, ok := ds.Datasets().MaybeGet(types.NewString(datasetID)); ok {
		return r.(types.Ref).TargetValue(ds).(types.Struct), true
	}
	return NewCommit(), false
}

func (ds *dataStoreCommon) Head(datasetID string) types.Struct {
	c, ok := ds.MaybeHead(datasetID)
	d.Chk.True(ok, "DataStore has no Head.")
	return c
}

func (ds *dataStoreCommon) Datasets() types.Map {
	if ds.datasets == nil {
		if ds.rootRef.IsEmpty() {
			emptyMap := NewMapOfStringToRefOfCommit()
			ds.datasets = &emptyMap
		} else {
			ds.datasets = ds.datasetsFromRef(ds.rootRef)
		}
	}

	return *ds.datasets
}

func (ds *dataStoreCommon) datasetsFromRef(datasetsRef ref.Ref) *types.Map {
	c := ds.ReadValue(datasetsRef).(types.Map)
	return &c
}

func (ds *dataStoreCommon) commit(datasetID string, commit types.Struct) error {
	return ds.doCommit(datasetID, commit)
}

// doCommit manages concurrent access the single logical piece of mutable state: the current Root. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootRef is the ref of the current head. The call to UpdateRoot below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again. This method will also fail and return an 'ErrMergeNeeded' error if the |commit| is not a descendent of the current dataset head
func (ds *dataStoreCommon) doCommit(datasetID string, commit types.Struct) error {
	currentRootRef, currentDatasets := ds.getRootAndDatasets()

	// TODO: This Commit will be orphaned if the tryUpdateRoot() below fails
	ds.WriteValue(commit)
	commitRef := types.NewTypedRefFromValue(commit)

	// First commit in store is always fast-foward.
	if !currentRootRef.IsEmpty() {
		r, hasHead := currentDatasets.MaybeGet(types.NewString(datasetID))

		// First commit in dataset is always fast-foward.
		if hasHead {
			currentHeadRef := r.(types.Ref)
			// Allow only fast-forward commits.
			if commitRef.Equals(currentHeadRef) {
				return nil
			}
			if !descendsFrom(commit, currentHeadRef, ds) {
				return ErrMergeNeeded
			}
		}
	}
	currentDatasets = currentDatasets.Set(types.NewString(datasetID), commitRef)
	return ds.tryUpdateRoot(currentDatasets, currentRootRef)
}

// doDelete manages concurrent access the single logical piece of mutable state: the current Root. doDelete is optimistic in that it is attempting to update head making the assumption that currentRootRef is the ref of the current head. The call to UpdateRoot below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again.
func (ds *dataStoreCommon) doDelete(datasetID string) error {
	currentRootRef, currentDatasets := ds.getRootAndDatasets()
	currentDatasets = currentDatasets.Remove(types.NewString(datasetID))
	return ds.tryUpdateRoot(currentDatasets, currentRootRef)
}

func (ds *dataStoreCommon) getRootAndDatasets() (currentRootRef ref.Ref, currentDatasets types.Map) {
	currentRootRef = ds.rt.Root()
	currentDatasets = ds.Datasets()

	if currentRootRef != currentDatasets.Ref() && !currentRootRef.IsEmpty() {
		// The root has been advanced.
		currentDatasets = *ds.datasetsFromRef(currentRootRef)
	}
	return
}

func (ds *dataStoreCommon) tryUpdateRoot(currentDatasets types.Map, currentRootRef ref.Ref) (err error) {
	// TODO: This Commit will be orphaned if the UpdateRoot below fails
	newRootRef := ds.WriteValue(currentDatasets).TargetRef()
	// If the root has been updated by another process in the short window since we read it, this call will fail. See issue #404
	if !ds.rt.UpdateRoot(newRootRef, currentRootRef) {
		err = ErrOptimisticLockFailed
	}
	return
}

func descendsFrom(commit types.Struct, currentHeadRef types.Ref, vr types.ValueReader) bool {
	// BFS because the common case is that the ancestor is only a step or two away
	ancestors := commit.Get(ParentsField).(types.Set)
	for !ancestors.Has(currentHeadRef) {
		if ancestors.Empty() {
			return false
		}
		ancestors = getAncestors(ancestors, vr)
	}
	return true
}

func getAncestors(commits types.Set, vr types.ValueReader) types.Set {
	ancestors := NewSetOfRefOfCommit()
	commits.IterAll(func(v types.Value) {
		r := v.(types.Ref)
		c := r.TargetValue(vr).(types.Struct)
		ancestors = ancestors.Union(c.Get(ParentsField).(types.Set))
	})
	return ancestors
}
