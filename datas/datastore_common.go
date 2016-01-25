package datas

import (
	"sync"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

type dataStoreCommon struct {
	chunks.ChunkStore
	rootRef  ref.Ref
	datasets *MapOfStringToRefOfCommit
	hasCache map[ref.Ref]bool
	mu       *sync.Mutex
}

var (
	ErrOptimisticLockFailed = errors.New("Optimistic lock failed on datastore Root update")
	ErrMergeNeeded          = errors.New("Dataset head is not ancestor of commit")
)

func datasetsFromRef(datasetsRef ref.Ref, cs chunks.ChunkStore) *MapOfStringToRefOfCommit {
	c := types.ReadValue(datasetsRef, cs).(MapOfStringToRefOfCommit)
	return &c
}

func (ds *dataStoreCommon) MaybeHead(datasetID string) (Commit, bool) {
	if r, ok := ds.Datasets().MaybeGet(datasetID); ok {
		return r.TargetValue(ds), true
	}
	return NewCommit(ds), false
}

func (ds *dataStoreCommon) Head(datasetID string) Commit {
	c, ok := ds.MaybeHead(datasetID)
	d.Chk.True(ok, "DataStore has no Head.")
	return c
}

func (ds *dataStoreCommon) Datasets() MapOfStringToRefOfCommit {
	if ds.datasets == nil {
		if ds.rootRef.IsEmpty() {
			emptySet := NewMapOfStringToRefOfCommit(ds)
			ds.datasets = &emptySet
		} else {
			ds.datasets = datasetsFromRef(ds.rootRef, ds)
		}
	}

	return *ds.datasets
}

// Copies all chunks reachable from (and including) |r| in |source| that aren't present in |sink|
func (ds *dataStoreCommon) CopyMissingChunksP(sourceRef ref.Ref, sink chunks.ChunkStore, concurrency int) {
	tcs := &teeChunkSource{ds, sink}

	copyCallback := func(r ref.Ref) bool {
		return sink.Has(r)
	}

	walk.SomeChunksP(sourceRef, tcs, copyCallback, concurrency)
}

func (ds *dataStoreCommon) commit(datasetID string, commit Commit) error {
	return ds.doCommit(datasetID, commit)
}

// doCommit manages concurrent access the single logical piece of mutable state: the current Root. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootRef is the ref of the current head. The call to UpdateRoot below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again. This method will also fail and return an 'ErrMergeNeeded' error if the |commit| is not a descendent of the current dataset head
func (ds *dataStoreCommon) doCommit(datasetID string, commit Commit) error {
	currentRootRef := ds.Root()
	currentDatasets := ds.Datasets()

	if currentRootRef != currentDatasets.Ref() && !currentRootRef.IsEmpty() {
		// The root has been advanced.
		currentDatasets = *datasetsFromRef(currentRootRef, ds)
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
				return nil
			}
			if !descendsFrom(commit, currentHeadRef, ds) {
				return ErrMergeNeeded
			}
		}
	}
	// TODO: This Commit will be orphaned if the UpdateRoot below fails
	currentDatasets = currentDatasets.Set(datasetID, commitRef)
	newRootRef := types.WriteValue(currentDatasets, ds)

	// If the root has been updated by another process in the short window since we read it, this call will fail. See issue #404
	if ds.UpdateRoot(newRootRef, currentRootRef) {
		return nil
	} else {
		return ErrOptimisticLockFailed
	}
}

func checkCache(ds *dataStoreCommon, r ref.Ref) (has, ok bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	has, ok = ds.hasCache[r]
	return
}

func setCache(ds *dataStoreCommon, r ref.Ref, has bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.hasCache[r] = has
}

func (ds *dataStoreCommon) Has(r ref.Ref) bool {
	has, ok := checkCache(ds, r)
	if ok {
		return has
	}
	has = ds.ChunkStore.Has(r)
	setCache(ds, r, has)
	return has
}

func (ds *dataStoreCommon) Get(r ref.Ref) chunks.Chunk {
	has, ok := checkCache(ds, r)
	if ok && !has {
		return chunks.EmptyChunk
	}
	c := ds.ChunkStore.Get(r)
	setCache(ds, r, !c.IsEmpty())
	return c
}

func (ds *dataStoreCommon) Put(c chunks.Chunk) {
	r := c.Ref()
	has, _ := checkCache(ds, r)
	if has {
		return
	}
	ds.ChunkStore.Put(c)
	setCache(ds, r, true)
}

func descendsFrom(commit Commit, currentHeadRef RefOfCommit, cs chunks.ChunkStore) bool {
	// BFS because the common case is that the ancestor is only a step or two away
	ancestors := commit.Parents()
	for !ancestors.Has(currentHeadRef) {
		if ancestors.Empty() {
			return false
		}
		ancestors = getAncestors(ancestors, cs)
	}
	return true
}

func getAncestors(commits SetOfRefOfCommit, cs chunks.ChunkStore) SetOfRefOfCommit {
	ancestors := NewSetOfRefOfCommit(cs)
	commits.IterAll(func(r RefOfCommit) {
		c := r.TargetValue(cs)
		ancestors = ancestors.Union(c.Parents())
	})
	return ancestors
}
