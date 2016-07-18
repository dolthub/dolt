// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"errors"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

type databaseCommon struct {
	cch      *cachingChunkHaver
	vs       *types.ValueStore
	rt       chunks.RootTracker
	rootRef  hash.Hash
	datasets *types.Map
}

var (
	ErrOptimisticLockFailed = errors.New("Optimistic lock failed on database Root update")
	ErrMergeNeeded          = errors.New("Dataset head is not ancestor of commit")
)

func newDatabaseCommon(cch *cachingChunkHaver, vs *types.ValueStore, rt chunks.RootTracker) databaseCommon {
	return databaseCommon{cch: cch, vs: vs, rt: rt, rootRef: rt.Root()}
}

func (ds *databaseCommon) MaybeHead(datasetID string) (types.Struct, bool) {
	if r, ok := ds.MaybeHeadRef(datasetID); ok {
		return r.TargetValue(ds).(types.Struct), true
	}
	return types.Struct{}, false
}

func (ds *databaseCommon) MaybeHeadRef(datasetID string) (types.Ref, bool) {
	if r, ok := ds.Datasets().MaybeGet(types.String(datasetID)); ok {
		return r.(types.Ref), true
	}
	return types.Ref{}, false
}

func (ds *databaseCommon) Head(datasetID string) types.Struct {
	c, ok := ds.MaybeHead(datasetID)
	d.Chk.True(ok, "Database \"%s\" has no Head.", datasetID)
	return c
}

func (ds *databaseCommon) HeadRef(datasetID string) types.Ref {
	r, ok := ds.MaybeHeadRef(datasetID)
	d.Chk.True(ok, "Database \"%s\" has no Head.", datasetID)
	return r
}

func (ds *databaseCommon) Datasets() types.Map {
	if ds.datasets == nil {
		if ds.rootRef.IsEmpty() {
			emptyMap := types.NewMap()
			ds.datasets = &emptyMap
		} else {
			ds.datasets = ds.datasetsFromRef(ds.rootRef)
		}
	}

	return *ds.datasets
}

func (ds *databaseCommon) has(h hash.Hash) bool {
	return ds.cch.Has(h)
}

func (ds *databaseCommon) ReadValue(r hash.Hash) types.Value {
	return ds.vs.ReadValue(r)
}

func (ds *databaseCommon) WriteValue(v types.Value) types.Ref {
	return ds.vs.WriteValue(v)
}

func (ds *databaseCommon) Close() error {
	return ds.vs.Close()
}

func (ds *databaseCommon) datasetsFromRef(datasetsRef hash.Hash) *types.Map {
	c := ds.ReadValue(datasetsRef).(types.Map)
	return &c
}

func (ds *databaseCommon) commit(datasetID string, commit types.Struct) error {
	return ds.doCommit(datasetID, commit)
}

// doCommit manages concurrent access the single logical piece of mutable state: the current Root. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootRef is the hash of the current head. The call to UpdateRoot below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again. This method will also fail and return an 'ErrMergeNeeded' error if the |commit| is not a descendent of the current dataset head
func (ds *databaseCommon) doCommit(datasetID string, commit types.Struct) error {
	currentRootRef, currentDatasets := ds.getRootAndDatasets()

	// TODO: This Commit will be orphaned if the tryUpdateRoot() below fails
	commitRef := ds.WriteValue(commit)

	// First commit in store is always fast-foward.
	if !currentRootRef.IsEmpty() {
		r, hasHead := currentDatasets.MaybeGet(types.String(datasetID))

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
	currentDatasets = currentDatasets.Set(types.String(datasetID), commitRef)
	return ds.tryUpdateRoot(currentDatasets, currentRootRef)
}

// doDelete manages concurrent access the single logical piece of mutable state: the current Root. doDelete is optimistic in that it is attempting to update head making the assumption that currentRootRef is the hash of the current head. The call to UpdateRoot below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again.
func (ds *databaseCommon) doDelete(datasetID string) error {
	currentRootRef, currentDatasets := ds.getRootAndDatasets()
	currentDatasets = currentDatasets.Remove(types.String(datasetID))
	return ds.tryUpdateRoot(currentDatasets, currentRootRef)
}

func (ds *databaseCommon) getRootAndDatasets() (currentRootRef hash.Hash, currentDatasets types.Map) {
	currentRootRef = ds.rt.Root()
	currentDatasets = ds.Datasets()

	if currentRootRef != currentDatasets.Hash() && !currentRootRef.IsEmpty() {
		// The root has been advanced.
		currentDatasets = *ds.datasetsFromRef(currentRootRef)
	}
	return
}

func (ds *databaseCommon) tryUpdateRoot(currentDatasets types.Map, currentRootRef hash.Hash) (err error) {
	// TODO: This Commit will be orphaned if the UpdateRoot below fails
	newRootRef := ds.WriteValue(currentDatasets).TargetHash()
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
	ancestors := types.NewSet()
	commits.IterAll(func(v types.Value) {
		r := v.(types.Ref)
		c := r.TargetValue(vr).(types.Struct)
		next := []types.Value{}
		c.Get(ParentsField).(types.Set).IterAll(func(v types.Value) {
			next = append(next, v)
		})
		ancestors = ancestors.Insert(next...)
	})
	return ancestors
}
