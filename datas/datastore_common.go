package datas

import (
	"errors"
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

type dataStoreCommon struct {
	cs        chunks.ChunkStore
	rootRef   ref.Ref
	datasets  *MapOfStringToRefOfCommit
	typeCache map[ref.Ref]chunkCacheEntry
	mu        *sync.Mutex
}

type chunkCacheEntry interface {
	Present() bool
	Type() types.Type
}

var (
	ErrOptimisticLockFailed = errors.New("Optimistic lock failed on datastore Root update")
	ErrMergeNeeded          = errors.New("Dataset head is not ancestor of commit")
)

func newDataStoreCommon(cs chunks.ChunkStore) dataStoreCommon {
	return dataStoreCommon{cs: cs, rootRef: cs.Root(), typeCache: map[ref.Ref]chunkCacheEntry{}, mu: &sync.Mutex{}}
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

// ReadValue reads and decodes a value from ds. It is not considered an error for the requested chunk to be empty; in this case, the function simply returns nil.
func (ds *dataStoreCommon) ReadValue(r ref.Ref) types.Value {
	v := types.DecodeChunk(ds.cs.Get(r), ds)
	checkAndSet := func(reachable ref.Ref, entry chunkCacheEntry) {
		if ds.checkCache(reachable) == nil {
			ds.setCache(reachable, entry)
		}
	}

	var entry chunkCacheEntry = absentChunk{}
	if v != nil {
		entry = presentChunk(v.Type())
		for _, reachable := range v.Chunks() {
			checkAndSet(reachable.TargetRef(), presentChunk(getTargetType(reachable)))
		}
	}
	checkAndSet(r, entry)
	return v
}

// WriteValue takes a Value, schedules it to be written it to ds, and returns v.Ref(). v is not guaranteed to be actually written until after a successful Commit().
func (ds *dataStoreCommon) WriteValue(v types.Value) (r ref.Ref) {
	if v == nil {
		return
	}

	r = v.Ref()
	if entry := ds.checkCache(r); entry != nil && entry.Present() {
		return
	}

	// Encoding v causes any child chunks, e.g. internal nodes if v is a meta sequence, to get written. That needs to happen before we try to validate v.
	chunk := types.EncodeValue(v, ds)

	for _, reachable := range v.Chunks() {
		entry := ds.checkCache(reachable.TargetRef())
		d.Chk.True(entry != nil && entry.Present(), "Value to write contains ref %s, which points to a non-existent Value.", reachable.TargetRef())

		// BUG 1121
		// It's possible that entry.Type() will be simply 'Value', but that 'reachable' is actually a properly-typed object -- that is, a Ref to some specific Type. The Chk below would fail, though it's possible that the Type is actually correct. We wouldn't be able to verify without reading it, though, so we'll dig into this later.
		targetType := getTargetType(reachable)
		if targetType.Equals(types.MakePrimitiveType(types.ValueKind)) {
			continue
		}
		d.Chk.True(entry.Type().Equals(targetType), "Value to write contains ref %s, which points to a value of a different type: %+v != %+v", reachable.TargetRef(), entry.Type(), targetType)
	}
	ds.cs.Put(chunk) // TODO: DataStore should manage batching and backgrounding Puts.
	ds.setCache(r, presentChunk(v.Type()))

	return
}

func getTargetType(refBase types.RefBase) types.Type {
	refType := refBase.Type()
	d.Chk.Equal(types.RefKind, refType.Kind())
	return refType.Desc.(types.CompoundDesc).ElemTypes[0]
}

func (ds *dataStoreCommon) checkCache(r ref.Ref) chunkCacheEntry {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.typeCache[r]
}

func (ds *dataStoreCommon) setCache(r ref.Ref, entry chunkCacheEntry) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.typeCache[r] = entry
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
	return newHasCachingChunkStore(ds.cs)
}

func (ds *dataStoreCommon) transitionalChunkStore() chunks.ChunkStore {
	return newHasCachingChunkStore(ds.cs)
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

type presentChunk types.Type

func (t presentChunk) Present() bool {
	return true
}

func (t presentChunk) Type() types.Type {
	return types.Type(t)
}

type absentChunk struct{}

func (a absentChunk) Present() bool {
	return false
}

func (a absentChunk) Type() types.Type {
	panic("Not reached. Should never call Type() on an absentChunk.")
}
