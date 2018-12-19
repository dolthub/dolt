// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"errors"
	"fmt"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/merge"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/random"
)

type database struct {
	*types.ValueStore
	rt rootTracker
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

func newDatabase(cs chunks.ChunkStore) *database {
	vs := types.NewValueStore(cs)
	if _, ok := cs.(*httpChunkStore); ok {
		vs.SetEnforceCompleteness(false)
	}

	return &database{
		ValueStore: vs, // ValueStore is responsible for closing |cs|
		rt:         vs,
	}
}

func (db *database) chunkStore() chunks.ChunkStore {
	return db.ChunkStore()
}

func (db *database) Stats() interface{} {
	return db.ChunkStore().Stats()
}

func (db *database) StatsSummary() string {
	return db.ChunkStore().StatsSummary()
}

func (db *database) Flush() {
	// TODO: This is a pretty ghetto hack - do better.
	// See: https://github.com/attic-labs/noms/issues/3530
	ds := db.GetDataset(fmt.Sprintf("-/flush/%s", random.Id()))
	r := db.WriteValue(types.Bool(true))
	ds, err := db.CommitValue(ds, r)
	d.PanicIfError(err)
	_, err = db.Delete(ds)
	d.PanicIfError(err)
}

func (db *database) Datasets() types.Map {
	rootHash := db.rt.Root()
	if rootHash.IsEmpty() {
		return types.NewMap(db)
	}

	return db.ReadValue(rootHash).(types.Map)
}

func (db *database) GetDataset(datasetID string) Dataset {
	if !DatasetFullRe.MatchString(datasetID) {
		d.Panic("Invalid dataset ID: %s", datasetID)
	}
	var head types.Value
	if r, ok := db.Datasets().MaybeGet(types.String(datasetID)); ok {
		head = r.(types.Ref).TargetValue(db)
	}

	return newDataset(db, datasetID, head)
}

func (db *database) Rebase() {
	db.rt.Rebase()
}

func (db *database) Close() error {
	return db.ValueStore.Close()
}

func (db *database) SetHead(ds Dataset, newHeadRef types.Ref) (Dataset, error) {
	return db.doHeadUpdate(ds, func(ds Dataset) error { return db.doSetHead(ds, newHeadRef) })
}

func (db *database) doSetHead(ds Dataset, newHeadRef types.Ref) error {
	if currentHeadRef, ok := ds.MaybeHeadRef(); ok && newHeadRef.Equals(currentHeadRef) {
		return nil
	}
	commit := db.validateRefAsCommit(newHeadRef)

	currentRootHash, currentDatasets := db.rt.Root(), db.Datasets()
	commitRef := db.WriteValue(commit) // will be orphaned if the tryCommitChunks() below fails

	currentDatasets = currentDatasets.Edit().Set(types.String(ds.ID()), types.ToRefOfValue(commitRef)).Map()
	return db.tryCommitChunks(currentDatasets, currentRootHash)
}

func (db *database) FastForward(ds Dataset, newHeadRef types.Ref) (Dataset, error) {
	return db.doHeadUpdate(ds, func(ds Dataset) error { return db.doFastForward(ds, newHeadRef) })
}

func (db *database) doFastForward(ds Dataset, newHeadRef types.Ref) error {
	currentHeadRef, ok := ds.MaybeHeadRef()
	if ok && newHeadRef.Equals(currentHeadRef) {
		return nil
	}

	if ok && newHeadRef.Height() <= currentHeadRef.Height() {
		return ErrMergeNeeded
	}

	commit := db.validateRefAsCommit(newHeadRef)
	return db.doCommit(ds.ID(), commit, nil)
}

func (db *database) Commit(ds Dataset, v types.Value, opts CommitOptions) (Dataset, error) {
	return db.doHeadUpdate(
		ds,
		func(ds Dataset) error { return db.doCommit(ds.ID(), buildNewCommit(ds, v, opts), opts.Policy) },
	)
}

func (db *database) CommitValue(ds Dataset, v types.Value) (Dataset, error) {
	return db.Commit(ds, v, CommitOptions{})
}

// doCommit manages concurrent access the single logical piece of mutable state: the current Root. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootHash is the hash of the current head. The call to Commit below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again. This method will also fail and return an 'ErrMergeNeeded' error if the |commit| is not a descendent of the current dataset head
func (db *database) doCommit(datasetID string, commit types.Struct, mergePolicy merge.Policy) error {
	if !IsCommit(commit) {
		d.Panic("Can't commit a non-Commit struct to dataset %s", datasetID)
	}

	// This could loop forever, given enough simultaneous committers. BUG 2565
	var err error
	for err = ErrOptimisticLockFailed; err == ErrOptimisticLockFailed; {
		currentRootHash, currentDatasets := db.rt.Root(), db.Datasets()
		commitRef := db.WriteValue(commit) // will be orphaned if the tryCommitChunks() below fails

		// If there's nothing in the DB yet, skip all this logic.
		if !currentRootHash.IsEmpty() {
			r, hasHead := currentDatasets.MaybeGet(types.String(datasetID))

			// First commit in dataset is always fast-forward, so go through all this iff there's already a Head for datasetID.
			if hasHead {
				head := r.(types.Ref).TargetValue(db)
				currentHeadRef := types.NewRef(head)
				ancestorRef, found := FindCommonAncestor(commitRef, currentHeadRef, db)
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

					ancestor, currentHead := db.validateRefAsCommit(ancestorRef), db.validateRefAsCommit(currentHeadRef)
					merged, err := mergePolicy(commit.Get(ValueField), currentHead.Get(ValueField), ancestor.Get(ValueField), db, nil)
					if err != nil {
						return err
					}
					commitRef = db.WriteValue(NewCommit(merged, types.NewSet(db, commitRef, currentHeadRef), types.EmptyStruct))
				}
			}
		}
		currentDatasets = currentDatasets.Edit().Set(types.String(datasetID), types.ToRefOfValue(commitRef)).Map()
		err = db.tryCommitChunks(currentDatasets, currentRootHash)
	}
	return err
}

func (db *database) Delete(ds Dataset) (Dataset, error) {
	return db.doHeadUpdate(ds, func(ds Dataset) error { return db.doDelete(ds.ID()) })
}

// doDelete manages concurrent access the single logical piece of mutable state: the current Root. doDelete is optimistic in that it is attempting to update head making the assumption that currentRootHash is the hash of the current head. The call to Commit below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again.
func (db *database) doDelete(datasetIDstr string) error {
	datasetID := types.String(datasetIDstr)
	currentRootHash, currentDatasets := db.rt.Root(), db.Datasets()
	var initialHead types.Ref
	if r, hasHead := currentDatasets.MaybeGet(datasetID); !hasHead {
		return nil
	} else {
		initialHead = r.(types.Ref)
	}

	var err error
	for {
		currentDatasets = currentDatasets.Edit().Remove(datasetID).Map()
		err = db.tryCommitChunks(currentDatasets, currentRootHash)
		if err != ErrOptimisticLockFailed {
			break
		}
		// If the optimistic lock failed because someone changed the Head of datasetID, then return ErrMergeNeeded. If it failed because someone changed a different Dataset, we should try again.
		currentRootHash, currentDatasets = db.rt.Root(), db.Datasets()
		if r, hasHead := currentDatasets.MaybeGet(datasetID); !hasHead || (hasHead && !initialHead.Equals(r)) {
			err = ErrMergeNeeded
			break
		}
	}
	return err
}

func (db *database) tryCommitChunks(currentDatasets types.Map, currentRootHash hash.Hash) (err error) {
	newRootHash := db.WriteValue(currentDatasets).TargetHash()

	if !db.rt.Commit(newRootHash, currentRootHash) {
		err = ErrOptimisticLockFailed
	}
	return
}

func (db *database) validateRefAsCommit(r types.Ref) types.Struct {
	v := db.ReadValue(r.TargetHash())

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
		parents = types.NewSet(ds.Database())
		if headRef, ok := ds.MaybeHeadRef(); ok {
			parents = parents.Edit().Insert(headRef).Set()
		}
	}

	meta := opts.Meta
	if meta.IsZeroValue() {
		meta = types.EmptyStruct
	}
	return NewCommit(v, parents, meta)
}

func (db *database) doHeadUpdate(ds Dataset, updateFunc func(ds Dataset) error) (Dataset, error) {
	err := updateFunc(ds)
	return db.GetDataset(ds.ID()), err
}
