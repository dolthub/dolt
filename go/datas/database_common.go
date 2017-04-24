// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"errors"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/merge"
	"github.com/attic-labs/noms/go/types"
)

type databaseCommon struct {
	*types.ValueStore
	cch      *cachingChunkHaver
	rt       rootTracker
	datasets *types.Map
}

var (
	ErrOptimisticLockFailed = errors.New("Optimistic lock failed on database Root update")
	ErrMergeNeeded          = errors.New("Dataset head is not ancestor of commit")
)

// rootTracker is a narrowing of the ChunkStore interface, to keep Database disciplined about working directly with Chunks
type rootTracker interface {
	Rebase()
	Root() hash.Hash
	Commit(current, last hash.Hash) bool
}

func newDatabaseCommon(cs chunks.ChunkStore) databaseCommon {
	return databaseCommon{
		ValueStore: types.NewValueStore(cs),
		cch:        newCachingChunkHaver(cs),
		rt:         cs,
	}
}

func (dbc *databaseCommon) validatingChunkStore() chunks.ChunkStore {
	return dbc.ChunkStore()
}

func (dbc *databaseCommon) Datasets() types.Map {
	if dbc.datasets == nil {
		if rootHash := dbc.rt.Root(); rootHash.IsEmpty() {
			emptyMap := types.NewMap()
			dbc.datasets = &emptyMap
		} else {
			dbc.datasets = dbc.datasetsFromRef(rootHash)
		}
	}

	return *dbc.datasets
}

func (dbc *databaseCommon) datasetsFromRef(datasetsRef hash.Hash) *types.Map {
	c := dbc.ReadValue(datasetsRef).(types.Map)
	return &c
}

func getDataset(db Database, datasetID string) Dataset {
	if !DatasetFullRe.MatchString(datasetID) {
		d.Panic("Invalid dataset ID: %s", datasetID)
	}
	if r, ok := db.Datasets().MaybeGet(types.String(datasetID)); ok {
		head := r.(types.Ref).TargetValue(db)
		d.PanicIfFalse(IsCommit(head))
		return Dataset{db, datasetID, types.NewRef(head)}
	}
	return Dataset{store: db, id: datasetID}
}

func (dbc *databaseCommon) has(h hash.Hash) bool {
	return dbc.cch.Has(h)
}

func (dbc *databaseCommon) Rebase() {
	cached := dbc.rt.Root()
	dbc.rt.Rebase()
	if dbc.rt.Root() != cached {
		dbc.datasets = nil
	}
}

func (dbc *databaseCommon) Close() error {
	return dbc.ValueStore.Close()
}

func (dbc *databaseCommon) doSetHead(ds Dataset, newHeadRef types.Ref) error {
	if currentHeadRef, ok := ds.MaybeHeadRef(); ok && newHeadRef == currentHeadRef {
		return nil
	}
	commit := dbc.validateRefAsCommit(newHeadRef)

	currentRootHash, currentDatasets := dbc.rt.Root(), dbc.Datasets()
	commitRef := dbc.WriteValue(commit) // will be orphaned if the tryCommitChunks() below fails

	currentDatasets = currentDatasets.Set(types.String(ds.ID()), types.ToRefOfValue(commitRef))
	return dbc.tryCommitChunks(currentDatasets, currentRootHash)
}

func (dbc *databaseCommon) doFastForward(ds Dataset, newHeadRef types.Ref) error {
	if currentHeadRef, ok := ds.MaybeHeadRef(); ok && newHeadRef == currentHeadRef {
		return nil
	} else if newHeadRef.Height() <= currentHeadRef.Height() {
		return ErrMergeNeeded
	}

	commit := dbc.validateRefAsCommit(newHeadRef)
	return dbc.doCommit(ds.ID(), commit, nil)
}

// doCommit manages concurrent access the single logical piece of mutable state: the current Root. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootHash is the hash of the current head. The call to Commit below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again. This method will also fail and return an 'ErrMergeNeeded' error if the |commit| is not a descendent of the current dataset head
func (dbc *databaseCommon) doCommit(datasetID string, commit types.Struct, mergePolicy merge.Policy) error {
	if !IsCommit(commit) {
		d.Panic("Can't commit a non-Commit struct to dataset %s", datasetID)
	}

	// This could loop forever, given enough simultaneous committers. BUG 2565
	var err error
	for err = ErrOptimisticLockFailed; err == ErrOptimisticLockFailed; {
		currentRootHash, currentDatasets := dbc.rt.Root(), dbc.Datasets()
		commitRef := dbc.WriteValue(commit) // will be orphaned if the tryCommitChunks() below fails

		// If there's nothing in the DB yet, skip all this logic.
		if !currentRootHash.IsEmpty() {
			r, hasHead := currentDatasets.MaybeGet(types.String(datasetID))

			// First commit in dataset is always fast-forward, so go through all this iff there's already a Head for datasetID.
			if hasHead {
				head := r.(types.Ref).TargetValue(dbc)
				currentHeadRef := types.NewRef(head)
				ancestorRef, found := FindCommonAncestor(commitRef, currentHeadRef, dbc)
				if !found {
					return ErrMergeNeeded
				}

				// This covers all cases where currentHeadRef is not an ancestor of commit, including the following edge cases:
				//   - commit is a duplicate of currentHead.
				//   - we hit an ErrOptimisticLockFailed and looped back around because some other process changed the Head out from under us.
				if currentHeadRef.TargetHash() != ancestorRef.TargetHash() || currentHeadRef.TargetHash() == commitRef.TargetHash() {
					if mergePolicy == nil {
						return ErrMergeNeeded
					}

					ancestor, currentHead := dbc.validateRefAsCommit(ancestorRef), dbc.validateRefAsCommit(currentHeadRef)
					merged, err := mergePolicy(commit.Get(ValueField), currentHead.Get(ValueField), ancestor.Get(ValueField), dbc, nil)
					if err != nil {
						return err
					}
					commitRef = dbc.WriteValue(NewCommit(merged, types.NewSet(commitRef, currentHeadRef), types.EmptyStruct))
				}
			}
		}
		currentDatasets = currentDatasets.Set(types.String(datasetID), types.ToRefOfValue(commitRef))
		err = dbc.tryCommitChunks(currentDatasets, currentRootHash)
	}
	return err
}

// doDelete manages concurrent access the single logical piece of mutable state: the current Root. doDelete is optimistic in that it is attempting to update head making the assumption that currentRootHash is the hash of the current head. The call to Commit below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again.
func (dbc *databaseCommon) doDelete(datasetIDstr string) error {
	datasetID := types.String(datasetIDstr)
	currentRootHash, currentDatasets := dbc.rt.Root(), dbc.Datasets()
	var initialHead types.Ref
	if r, hasHead := currentDatasets.MaybeGet(datasetID); !hasHead {
		return nil
	} else {
		initialHead = r.(types.Ref)
	}

	var err error
	for {
		currentDatasets = currentDatasets.Remove(datasetID)
		err = dbc.tryCommitChunks(currentDatasets, currentRootHash)
		if err != ErrOptimisticLockFailed {
			break
		}
		// If the optimistic lock failed because someone changed the Head of datasetID, then return ErrMergeNeeded. If it failed because someone changed a different Dataset, we should try again.
		currentRootHash, currentDatasets = dbc.rt.Root(), dbc.Datasets()
		if r, hasHead := currentDatasets.MaybeGet(datasetID); !hasHead || (hasHead && !initialHead.Equals(r)) {
			err = ErrMergeNeeded
			break
		}
	}
	return err
}

func (dbc *databaseCommon) tryCommitChunks(currentDatasets types.Map, currentRootHash hash.Hash) (err error) {
	newRootHash := dbc.WriteValue(currentDatasets).TargetHash()

	dbc.Flush()

	// Since dbc.rt.Commit() updates the root, dbc.datasets would be out of date upon return. So, nil it out.
	defer func() { dbc.datasets = nil }()
	if !dbc.rt.Commit(newRootHash, currentRootHash) {
		err = ErrOptimisticLockFailed
	}
	return
}

func (dbc *databaseCommon) validateRefAsCommit(r types.Ref) types.Struct {
	v := dbc.ReadValue(r.TargetHash())

	if v == nil {
		panic(r.TargetHash().String() + " not found")
	}
	if !IsCommit(v) {
		panic("Not a commit: " + types.EncodedValueMaxLines(v, 10) + "  ...\n")
	}
	return v.(types.Struct)
}

func buildNewCommit(ds Dataset, v types.Value, opts CommitOptions) types.Struct {
	parents := opts.Parents
	if (parents == types.Set{}) {
		parents = types.NewSet()
		if headRef, ok := ds.MaybeHeadRef(); ok {
			parents = parents.Insert(headRef)
		}
	}

	meta := opts.Meta
	if meta.IsZeroValue() {
		meta = types.EmptyStruct
	}
	return NewCommit(v, parents, meta)
}
