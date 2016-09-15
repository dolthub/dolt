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
	*types.ValueStore
	cch      *cachingChunkHaver
	rt       chunks.RootTracker
	rootRef  hash.Hash
	datasets *types.Map
}

var (
	ErrOptimisticLockFailed = errors.New("Optimistic lock failed on database Root update")
	ErrMergeNeeded          = errors.New("Dataset head is not ancestor of commit")
)

func newDatabaseCommon(cch *cachingChunkHaver, vs *types.ValueStore, rt chunks.RootTracker) databaseCommon {
	return databaseCommon{ValueStore: vs, cch: cch, rt: rt, rootRef: rt.Root()}
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
	d.PanicIfFalse(ok, "Database \"%s\" has no Head.", datasetID)
	return c
}

func (ds *databaseCommon) HeadRef(datasetID string) types.Ref {
	r, ok := ds.MaybeHeadRef(datasetID)
	d.PanicIfFalse(ok, "Database \"%s\" has no Head.", datasetID)
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

func (ds *databaseCommon) datasetsFromRef(datasetsRef hash.Hash) *types.Map {
	c := ds.ReadValue(datasetsRef).(types.Map)
	return &c
}

func (ds *databaseCommon) has(h hash.Hash) bool {
	return ds.cch.Has(h)
}

func (ds *databaseCommon) Close() error {
	return ds.ValueStore.Close()
}

func (ds *databaseCommon) doSetHead(datasetID string, commit types.Struct) error {
	d.PanicIfTrue(!IsCommitType(commit.Type()), "Can't commit a non-Commit struct to dataset %s", datasetID)

	currentRootRef, currentDatasets := ds.getRootAndDatasets()
	commitRef := ds.WriteValue(commit) // will be orphaned if the tryUpdateRoot() below fails

	currentDatasets = currentDatasets.Set(types.String(datasetID), commitRef)
	return ds.tryUpdateRoot(currentDatasets, currentRootRef)
}

// doCommit manages concurrent access the single logical piece of mutable state: the current Root. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootRef is the hash of the current head. The call to UpdateRoot below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again. This method will also fail and return an 'ErrMergeNeeded' error if the |commit| is not a descendent of the current dataset head
func (ds *databaseCommon) doCommit(datasetID string, commit types.Struct) error {
	d.PanicIfTrue(!IsCommitType(commit.Type()), "Can't commit a non-Commit struct to dataset %s", datasetID)

	// This could loop forever, given enough simultaneous committers. BUG 2565
	var err error
	for err = ErrOptimisticLockFailed; err == ErrOptimisticLockFailed; {
		currentRootRef, currentDatasets := ds.getRootAndDatasets()
		commitRef := ds.WriteValue(commit) // will be orphaned if the tryUpdateRoot() below fails

		// Allow only fast-forward commits.
		// If there's nothing in the DB yet, skip all this logic.
		if !currentRootRef.IsEmpty() {
			r, hasHead := currentDatasets.MaybeGet(types.String(datasetID))

			// First commit in dataset is always fast-forward, so go through all this iff there's already a Head for datasetID.
			if hasHead {
				currentHeadRef := r.(types.Ref)
				if commitRef.Equals(currentHeadRef) {
					return nil
				}
				// This covers all cases where commit doesn't descend from the Head of datasetID, including the case where we hit an ErrOptimisticLockFailed and looped back around because some other process changed the Head out from under us.
				if !CommitDescendsFrom(commit, currentHeadRef, ds) {
					return ErrMergeNeeded
				}
			}
		}
		currentDatasets = currentDatasets.Set(types.String(datasetID), commitRef)
		err = ds.tryUpdateRoot(currentDatasets, currentRootRef)
	}
	return err
}

// doDelete manages concurrent access the single logical piece of mutable state: the current Root. doDelete is optimistic in that it is attempting to update head making the assumption that currentRootRef is the hash of the current head. The call to UpdateRoot below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again.
func (ds *databaseCommon) doDelete(datasetIDstr string) error {
	datasetID := types.String(datasetIDstr)
	currentRootRef, currentDatasets := ds.getRootAndDatasets()
	var initialHead types.Ref
	if r, hasHead := currentDatasets.MaybeGet(datasetID); !hasHead {
		return nil
	} else {
		initialHead = r.(types.Ref)
	}

	var err error
	for {
		currentDatasets = currentDatasets.Remove(datasetID)
		err = ds.tryUpdateRoot(currentDatasets, currentRootRef)
		if err != ErrOptimisticLockFailed {
			break
		}
		// If the optimistic lock failed because someone changed the Head of datasetID, then return ErrMergeNeeded. If it failed because someone changed a different Dataset, we should try again.
		currentRootRef, currentDatasets = ds.getRootAndDatasets()
		if r, hasHead := currentDatasets.MaybeGet(datasetID); !hasHead || (hasHead && !initialHead.Equals(r)) {
			err = ErrMergeNeeded
			break
		}
	}
	return err
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
	// TODO: This Map will be orphaned if the UpdateRoot below fails
	newRootRef := ds.WriteValue(currentDatasets).TargetHash()
	// If the root has been updated by another process in the short window since we read it, this call will fail. See issue #404
	if !ds.rt.UpdateRoot(newRootRef, currentRootRef) {
		err = ErrOptimisticLockFailed
	}
	return
}
