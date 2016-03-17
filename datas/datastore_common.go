package datas

import (
	"errors"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

type dataStoreCommon struct {
	cs       *hasCachingChunkStore
	rootRef  ref.Ref
	datasets *MapOfStringToRefOfCommit
}

var (
	ErrOptimisticLockFailed = errors.New("Optimistic lock failed on datastore Root update")
	ErrMergeNeeded          = errors.New("Dataset head is not ancestor of commit")
)

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

// ReadValue reads and decodes a value from ds. It is not considered an error for the requested chunk to be empty; in this case, the function simply returns nil.
func (ds *dataStoreCommon) ReadValue(r ref.Ref) types.Value {
	c := ds.cs.Get(r)
	return types.DecodeChunk(c, ds)
}

func (ds *dataStoreCommon) WriteValue(v types.Value) ref.Ref {
	return types.WriteValue(v, ds.cs)
}

// Has should really not be exposed on DataStore :-/
func (ds *dataStoreCommon) Has(r ref.Ref) bool {
	return ds.cs.Has(r)
}

func (ds *dataStoreCommon) Close() error {
	return ds.cs.Close()
}

// CopyMissingChunksP copies to |sink| all chunks in ds that are reachable from (and including) |r|, skipping chunks that |sink| already has
func (ds *dataStoreCommon) CopyMissingChunksP(sourceRef ref.Ref, sink DataStore, concurrency int) {
	sinkCS := sink.transitionalChunkStore()
	tcs := &teeDataSource{ds.cs, sinkCS}

	copyCallback := func(r ref.Ref) bool {
		return sinkCS.Has(r)
	}

	walk.SomeChunksP(sourceRef, tcs, copyCallback, concurrency)
}

func (ds *dataStoreCommon) transitionalChunkSink() chunks.ChunkSink {
	return ds.cs
}

func (ds *dataStoreCommon) transitionalChunkStore() chunks.ChunkStore {
	return ds.cs
}

func (ds *dataStoreCommon) commit(datasetID string, commit Commit) error {
	return ds.doCommit(datasetID, commit)
}

// doCommit manages concurrent access the single logical piece of mutable state: the current Root. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootRef is the ref of the current head. The call to UpdateRoot below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again. This method will also fail and return an 'ErrMergeNeeded' error if the |commit| is not a descendent of the current dataset head
func (ds *dataStoreCommon) doCommit(datasetID string, commit Commit) error {
	currentRootRef, currentDatasets := ds.getRootAndDatasets()

	// TODO: This Commit will be orphaned if the tryUpdateRoot() below fails
	commitRef := NewRefOfCommit(ds.WriteValue(commit))

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
	currentRootRef = ds.cs.Root()
	currentDatasets = ds.Datasets()

	if currentRootRef != currentDatasets.Ref() && !currentRootRef.IsEmpty() {
		// The root has been advanced.
		currentDatasets = *ds.datasetsFromRef(currentRootRef)
	}
	return
}

func (ds *dataStoreCommon) tryUpdateRoot(currentDatasets MapOfStringToRefOfCommit, currentRootRef ref.Ref) (err error) {
	// TODO: This Commit will be orphaned if the UpdateRoot below fails
	newRootRef := ds.WriteValue(currentDatasets)
	// If the root has been updated by another process in the short window since we read it, this call will fail. See issue #404
	if !ds.cs.UpdateRoot(newRootRef, currentRootRef) {
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
