// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"
	"errors"
	"fmt"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/merge"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/liquidata-inc/ld/dolt/go/store/util/random"
)

type database struct {
	*types.ValueStore
	rt rootTracker
}

var (
	ErrOptimisticLockFailed = errors.New("optimistic lock failed on database Root update")
	ErrMergeNeeded          = errors.New("dataset head is not ancestor of commit")
)

// TODO: fix panics
// rootTracker is a narrowing of the ChunkStore interface, to keep Database disciplined about working directly with Chunks
type rootTracker interface {
	Rebase(ctx context.Context)
	Root(ctx context.Context) hash.Hash
	Commit(ctx context.Context, current, last hash.Hash) (bool, error)
}

func newDatabase(cs chunks.ChunkStore) *database {
	vs := types.NewValueStore(cs)

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

func (db *database) Flush(ctx context.Context) {
	// TODO: This is a pretty ghetto hack - do better.
	// See: https://github.com/attic-labs/noms/issues/3530
	ds := db.GetDataset(ctx, fmt.Sprintf("-/flush/%s", random.Id()))
	r := db.WriteValue(ctx, types.Bool(true))
	ds, err := db.CommitValue(ctx, ds, r)
	d.PanicIfError(err)
	_, err = db.Delete(ctx, ds)
	d.PanicIfError(err)
}

func (db *database) Datasets(ctx context.Context) types.Map {
	rootHash := db.rt.Root(ctx)
	if rootHash.IsEmpty() {
		return types.NewMap(ctx, db)
	}

	return db.ReadValue(ctx, rootHash).(types.Map)
}

func (db *database) GetDataset(ctx context.Context, datasetID string) Dataset {
	if !DatasetFullRe.MatchString(datasetID) {
		d.Panic("Invalid dataset ID: %s", datasetID)
	}
	var head types.Value
	if r, ok := db.Datasets(ctx).MaybeGet(ctx, types.String(datasetID)); ok {
		head = r.(types.Ref).TargetValue(ctx, db)
	}

	return newDataset(db, datasetID, head)
}

func (db *database) Rebase(ctx context.Context) {
	db.rt.Rebase(ctx)
}

func (db *database) Close() error {
	return db.ValueStore.Close()
}

func (db *database) SetHead(ctx context.Context, ds Dataset, newHeadRef types.Ref) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error { return db.doSetHead(ctx, ds, newHeadRef) })
}

func (db *database) doSetHead(ctx context.Context, ds Dataset, newHeadRef types.Ref) error {
	if currentHeadRef, ok := ds.MaybeHeadRef(); ok && newHeadRef.Equals(db.Format(), currentHeadRef) {
		return nil
	}
	commit := db.validateRefAsCommit(ctx, newHeadRef)

	currentRootHash, currentDatasets := db.rt.Root(ctx), db.Datasets(ctx)
	commitRef := db.WriteValue(ctx, commit) // will be orphaned if the tryCommitChunks() below fails

	currentDatasets = currentDatasets.Edit().Set(types.String(ds.ID()), types.ToRefOfValue(commitRef, db.Format())).Map(ctx)
	return db.tryCommitChunks(ctx, currentDatasets, currentRootHash)
}

func (db *database) FastForward(ctx context.Context, ds Dataset, newHeadRef types.Ref) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error { return db.doFastForward(ctx, ds, newHeadRef) })
}

func (db *database) doFastForward(ctx context.Context, ds Dataset, newHeadRef types.Ref) error {
	currentHeadRef, ok := ds.MaybeHeadRef()
	if ok && newHeadRef.Equals(db.Format(), currentHeadRef) {
		return nil
	}

	if ok && newHeadRef.Height() <= currentHeadRef.Height() {
		return ErrMergeNeeded
	}

	commit := db.validateRefAsCommit(ctx, newHeadRef)
	return db.doCommit(ctx, db.Format(), ds.ID(), commit, nil)
}

func (db *database) Commit(ctx context.Context, ds Dataset, v types.Value, opts CommitOptions) (Dataset, error) {
	return db.doHeadUpdate(
		ctx,
		ds,
		func(ds Dataset) error {
			return db.doCommit(ctx, db.Format(), ds.ID(), buildNewCommit(ctx, db.Format(), ds, v, opts), opts.Policy)
		},
	)
}

func (db *database) CommitValue(ctx context.Context, ds Dataset, v types.Value) (Dataset, error) {
	return db.Commit(ctx, ds, v, CommitOptions{})
}

// doCommit manages concurrent access the single logical piece of mutable state: the current Root. doCommit is optimistic in that it is attempting to update head making the assumption that currentRootHash is the hash of the current head. The call to Commit below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again. This method will also fail and return an 'ErrMergeNeeded' error if the |commit| is not a descendent of the current dataset head
func (db *database) doCommit(ctx context.Context, format *types.Format, datasetID string, commit types.Struct, mergePolicy merge.Policy) error {
	if !IsCommit(db.Format(), commit) {
		d.Panic("Can't commit a non-Commit struct to dataset %s", datasetID)
	}

	// This could loop forever, given enough simultaneous committers. BUG 2565
	var err error
	for err = ErrOptimisticLockFailed; err == ErrOptimisticLockFailed; {
		currentRootHash, currentDatasets := db.rt.Root(ctx), db.Datasets(ctx)
		commitRef := db.WriteValue(ctx, commit) // will be orphaned if the tryCommitChunks() below fails

		// If there's nothing in the DB yet, skip all this logic.
		if !currentRootHash.IsEmpty() {
			r, hasHead := currentDatasets.MaybeGet(ctx, types.String(datasetID))

			// First commit in dataset is always fast-forward, so go through all this iff there's already a Head for datasetID.
			if hasHead {
				head := r.(types.Ref).TargetValue(ctx, db)
				currentHeadRef := types.NewRef(head, db.Format())
				ancestorRef, found := FindCommonAncestor(ctx, db.Format(), commitRef, currentHeadRef, db)
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

					ancestor, currentHead := db.validateRefAsCommit(ctx, ancestorRef), db.validateRefAsCommit(ctx, currentHeadRef)
					merged, err := mergePolicy(ctx, commit.Get(ValueField), currentHead.Get(ValueField), ancestor.Get(ValueField), db, nil)
					if err != nil {
						return err
					}
					commitRef = db.WriteValue(ctx, NewCommit(format, merged, types.NewSet(ctx, db, commitRef, currentHeadRef), types.EmptyStruct(db.Format())))
				}
			}
		}
		currentDatasets = currentDatasets.Edit().Set(types.String(datasetID), types.ToRefOfValue(commitRef, db.Format())).Map(ctx)
		err = db.tryCommitChunks(ctx, currentDatasets, currentRootHash)
	}
	return err
}

func (db *database) Delete(ctx context.Context, ds Dataset) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error { return db.doDelete(ctx, ds.ID()) })
}

// doDelete manages concurrent access the single logical piece of mutable state: the current Root. doDelete is optimistic in that it is attempting to update head making the assumption that currentRootHash is the hash of the current head. The call to Commit below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again.
func (db *database) doDelete(ctx context.Context, datasetIDstr string) error {
	datasetID := types.String(datasetIDstr)
	currentRootHash, currentDatasets := db.rt.Root(ctx), db.Datasets(ctx)
	var initialHead types.Ref
	if r, hasHead := currentDatasets.MaybeGet(ctx, datasetID); !hasHead {
		return nil
	} else {
		initialHead = r.(types.Ref)
	}

	var err error
	for {
		currentDatasets = currentDatasets.Edit().Remove(datasetID).Map(ctx)
		err = db.tryCommitChunks(ctx, currentDatasets, currentRootHash)
		if err != ErrOptimisticLockFailed {
			break
		}
		// If the optimistic lock failed because someone changed the Head of datasetID, then return ErrMergeNeeded. If it failed because someone changed a different Dataset, we should try again.
		currentRootHash, currentDatasets = db.rt.Root(ctx), db.Datasets(ctx)
		if r, hasHead := currentDatasets.MaybeGet(ctx, datasetID); !hasHead || (hasHead && !initialHead.Equals(db.Format(), r)) {
			err = ErrMergeNeeded
			break
		}
	}
	return err
}

func (db *database) tryCommitChunks(ctx context.Context, currentDatasets types.Map, currentRootHash hash.Hash) error {
	newRootHash := db.WriteValue(ctx, currentDatasets).TargetHash()

	if success, err := db.rt.Commit(ctx, newRootHash, currentRootHash); err != nil {
		// TODO: fix panics
		d.PanicIfError(err)
	} else if !success {
		return ErrOptimisticLockFailed
	}

	return nil
}

func (db *database) validateRefAsCommit(ctx context.Context, r types.Ref) types.Struct {
	v := db.ReadValue(ctx, r.TargetHash())

	if v == nil {
		panic(r.TargetHash().String() + " not found")
	}
	if !IsCommit(db.Format(), v) {
		panic("Not a commit: " + types.EncodedValueMaxLines(ctx, db.Format(), v, 10) + "  ...\n")
	}
	return v.(types.Struct)
}

func buildNewCommit(ctx context.Context, format *types.Format, ds Dataset, v types.Value, opts CommitOptions) types.Struct {
	parents := opts.Parents
	if (parents == types.Set{}) {
		parents = types.NewSet(ctx, ds.Database())
		if headRef, ok := ds.MaybeHeadRef(); ok {
			parents = parents.Edit().Insert(headRef).Set(ctx)
		}
	}

	meta := opts.Meta
	if meta.IsZeroValue() {
		meta = types.EmptyStruct(format)
	}
	return NewCommit(format, v, parents, meta)
}

func (db *database) doHeadUpdate(ctx context.Context, ds Dataset, updateFunc func(ds Dataset) error) (Dataset, error) {
	err := updateFunc(ds)
	return db.GetDataset(ctx, ds.ID()), err
}
