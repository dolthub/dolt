// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/merge"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/random"
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
	Rebase(ctx context.Context) error
	Root(ctx context.Context) (hash.Hash, error)
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

func (db *database) Flush(ctx context.Context) error {
	// TODO: This is a pretty ghetto hack - do better.
	// See: https://github.com/attic-labs/noms/issues/3530
	ds, err := db.GetDataset(ctx, fmt.Sprintf("-/flush/%s", random.Id()))

	if err != nil {
		return err
	}

	r, err := db.WriteValue(ctx, types.Bool(true))

	if err != nil {
		return err
	}

	ds, err = db.CommitValue(ctx, ds, r)

	if err != nil {
		return err
	}

	_, err = db.Delete(ctx, ds)

	return err
}

func (db *database) Datasets(ctx context.Context) (types.Map, error) {
	rootHash, err := db.rt.Root(ctx)

	if err != nil {
		return types.EmptyMap, err
	}

	if rootHash.IsEmpty() {
		return types.NewMap(ctx, db)
	}

	val, err := db.ReadValue(ctx, rootHash)

	if err != nil {
		return types.EmptyMap, err
	}

	return val.(types.Map), nil
}

func (db *database) GetDataset(ctx context.Context, datasetID string) (Dataset, error) {
	// precondition checks
	if !DatasetFullRe.MatchString(datasetID) {
		d.Panic("Invalid dataset ID: %s", datasetID)
	}

	datasets, err := db.Datasets(ctx)

	if err != nil {
		return Dataset{}, err
	}

	var head types.Value
	if r, ok, err := datasets.MaybeGet(ctx, types.String(datasetID)); err != nil {
		return Dataset{}, err
	} else if ok {
		head, err = r.(types.Ref).TargetValue(ctx, db)

		if err != nil {
			return Dataset{}, err
		}
	}

	return newDataset(db, datasetID, head)
}

func (db *database) Rebase(ctx context.Context) error {
	return db.rt.Rebase(ctx)
}

func (db *database) Close() error {
	return db.ValueStore.Close()
}

func (db *database) SetHead(ctx context.Context, ds Dataset, newHeadRef types.Ref) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error { return db.doSetHead(ctx, ds, newHeadRef) })
}

func (db *database) doSetHead(ctx context.Context, ds Dataset, newHeadRef types.Ref) error {
	newSt, err := newHeadRef.TargetValue(ctx, db)

	if err != nil {
		return err
	}

	headType := newSt.(types.Struct).Name()

	currentHeadRef, ok, err := ds.MaybeHeadRef()
	if err != nil {
		return err
	}
	if ok {
		if newHeadRef.Equals(currentHeadRef) {
			return nil
		}

		currSt, err := currentHeadRef.TargetValue(ctx, db)

		if err != nil {
			return err
		}

		headType = currSt.(types.Struct).Name()
	}

	// the new head value must match the type of the old head value
	switch headType {
	case CommitName:
		_, err = db.validateRefAsCommit(ctx, newHeadRef)
	case TagName:
		err = db.validateTag(ctx, newSt.(types.Struct))
	default:
		return fmt.Errorf("Unrecognized dataset value: %s", headType)
	}

	if err != nil {
		return err
	}

	currentRootHash, err := db.rt.Root(ctx)

	if err != nil {
		return err
	}

	currentDatasets, err := db.Datasets(ctx)

	if err != nil {
		return err
	}

	refSt, err := db.WriteValue(ctx, newSt) // will be orphaned if the tryCommitChunks() below fails

	if err != nil {
		return err
	}

	ref, err := types.ToRefOfValue(refSt, db.Format())

	if err != nil {
		return err
	}

	currentDatasets, err = currentDatasets.Edit().Set(types.String(ds.ID()), ref).Map(ctx)

	if err != nil {
		return err
	}

	return db.tryCommitChunks(ctx, currentDatasets, currentRootHash)
}

func (db *database) FastForward(ctx context.Context, ds Dataset, newHeadRef types.Ref) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error { return db.doFastForward(ctx, ds, newHeadRef) })
}

func (db *database) doFastForward(ctx context.Context, ds Dataset, newHeadRef types.Ref) error {
	currentHeadRef, ok, err := ds.MaybeHeadRef()

	if err != nil {
		return err
	}

	if ok && newHeadRef.Equals(currentHeadRef) {
		return nil
	}

	if ok && newHeadRef.Height() <= currentHeadRef.Height() {
		return ErrMergeNeeded
	}

	commit, err := db.validateRefAsCommit(ctx, newHeadRef)

	if err != nil {
		return err
	}

	return db.doCommit(ctx, ds.ID(), commit, nil)
}

func (db *database) Commit(ctx context.Context, ds Dataset, v types.Value, opts CommitOptions) (Dataset, error) {
	return db.doHeadUpdate(
		ctx,
		ds,
		func(ds Dataset) error {
			st, err := buildNewCommit(ctx, ds, v, opts)

			if err != nil {
				return err
			}

			return db.doCommit(ctx, ds.ID(), st, opts.Policy)
		},
	)
}

func (db *database) CommitDangling(ctx context.Context, v types.Value, opts CommitOptions) (types.Struct, error) {
	if opts.ParentsList == types.EmptyList || opts.ParentsList.Len() == 0 {
		return types.Struct{}, errors.New("cannot create commit without parents")
	}

	if opts.Meta.IsZeroValue() {
		opts.Meta = types.EmptyStruct(db.Format())
	}

	commitStruct, err := NewCommit(ctx, v, opts.ParentsList, opts.Meta)
	if err != nil {
		return types.Struct{}, err
	}

	_, err = db.WriteValue(ctx, commitStruct)
	if err != nil {
		return types.Struct{}, err
	}

	err = db.Flush(ctx)
	if err != nil {
		return types.Struct{}, err
	}

	return commitStruct, nil
}

func (db *database) CommitValue(ctx context.Context, ds Dataset, v types.Value) (Dataset, error) {
	return db.Commit(ctx, ds, v, CommitOptions{})
}

// doCommit manages concurrent access the single logical piece of mutable state: the current Root. doCommit is
// optimistic in that it is attempting to update head making the assumption that currentRootHash is the hash of the
// current head. The call to Commit below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g.
// because of a race with another writer) and the entire algorithm must be tried again. This method will also fail and
// return an 'ErrMergeNeeded' error if the |commit| is not a descendent of the current dataset head
func (db *database) doCommit(ctx context.Context, datasetID string, commit types.Struct, mergePolicy merge.Policy) error {
	if is, err := IsCommit(commit); err != nil {
		return err
	} else if !is {
		d.Panic("Can't commit a non-Commit struct to dataset %s", datasetID)
	}

	// This could loop forever, given enough simultaneous committers. BUG 2565
	var tryCommitErr error
	for tryCommitErr = ErrOptimisticLockFailed; tryCommitErr == ErrOptimisticLockFailed; {
		currentRootHash, err := db.rt.Root(ctx)

		if err != nil {
			return err
		}

		currentDatasets, err := db.Datasets(ctx)

		if err != nil {
			return err
		}

		commitRef, err := db.WriteValue(ctx, commit) // will be orphaned if the tryCommitChunks() below fails

		if err != nil {
			return err
		}

		// If there's nothing in the DB yet, skip all this logic.
		if !currentRootHash.IsEmpty() {
			r, hasHead, err := currentDatasets.MaybeGet(ctx, types.String(datasetID))

			if err != nil {
				return err
			}

			// First commit in dataset is always fast-forward, so go through all this iff there's already a Head for datasetID.
			if hasHead {
				head, err := r.(types.Ref).TargetValue(ctx, db)

				if err != nil {
					return err
				}

				currentHeadRef, err := types.NewRef(head, db.Format())

				if err != nil {
					return err
				}

				ancestorRef, found, err := FindCommonAncestor(ctx, commitRef, currentHeadRef, db, db)

				if err != nil {
					return err
				}

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

					ancestor, err := db.validateRefAsCommit(ctx, ancestorRef)
					if err != nil {
						return err
					}

					currentHead, err := db.validateRefAsCommit(ctx, currentHeadRef)
					if err != nil {
						return err
					}

					cmVal, _, err := commit.MaybeGet(ValueField)
					if err != nil {
						return err
					}

					curVal, _, err := currentHead.MaybeGet(ValueField)
					if err != nil {
						return err
					}

					ancVal, _, err := ancestor.MaybeGet(ValueField)
					if err != nil {
						return err
					}

					merged, err := mergePolicy(ctx, cmVal, curVal, ancVal, db, nil)
					if err != nil {
						return err
					}

					l, err := types.NewList(ctx, db, commitRef, currentHeadRef)
					if err != nil {
						return err
					}

					newCom, err := NewCommit(ctx, merged, l, types.EmptyStruct(db.Format()))
					if err != nil {
						return err
					}

					commitRef, err = db.WriteValue(ctx, newCom)
					if err != nil {
						return err
					}
				}
			}
		}

		ref, err := types.ToRefOfValue(commitRef, db.Format())
		if err != nil {
			return err
		}

		currentDatasets, err = currentDatasets.Edit().Set(types.String(datasetID), ref).Map(ctx)
		if err != nil {
			return err
		}

		tryCommitErr = db.tryCommitChunks(ctx, currentDatasets, currentRootHash)
	}

	return tryCommitErr
}

func (db *database) Tag(ctx context.Context, ds Dataset, ref types.Ref, opts TagOptions) (Dataset, error) {
	return db.doHeadUpdate(
		ctx,
		ds,
		func(ds Dataset) error {
			st, err := NewTag(ctx, ref, opts.Meta)

			if err != nil {
				return err
			}

			return db.doTag(ctx, ds.ID(), st)
		},
	)
}

// doTag manages concurrent access the single logical piece of mutable state: the current Root. It uses
// the same optimistic writing algorithm as doCommit (see above).
func (db *database) doTag(ctx context.Context, datasetID string, tag types.Struct) error {
	err := db.validateTag(ctx, tag)

	if err != nil {
		return err
	}

	// This could loop forever, given enough simultaneous writers. BUG 2565
	var tryCommitErr error
	for tryCommitErr = ErrOptimisticLockFailed; tryCommitErr == ErrOptimisticLockFailed; {
		currentRootHash, err := db.rt.Root(ctx)

		if err != nil {
			return err
		}

		currentDatasets, err := db.Datasets(ctx)

		if err != nil {
			return err
		}

		tagRef, err := db.WriteValue(ctx, tag) // will be orphaned if the tryCommitChunks() below fails

		if err != nil {
			return err
		}

		_, hasHead, err := currentDatasets.MaybeGet(ctx, types.String(datasetID))

		if err != nil {
			return err
		}

		if hasHead {
			return fmt.Errorf(fmt.Sprintf("tag %s already exists and cannot be altered after creation", datasetID))
		}

		ref, err := types.ToRefOfValue(tagRef, db.Format())
		if err != nil {
			return err
		}

		currentDatasets, err = currentDatasets.Edit().Set(types.String(datasetID), ref).Map(ctx)
		if err != nil {
			return err
		}

		tryCommitErr = db.tryCommitChunks(ctx, currentDatasets, currentRootHash)
	}

	return tryCommitErr
}

func (db *database) Delete(ctx context.Context, ds Dataset) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error { return db.doDelete(ctx, ds.ID()) })
}

// doDelete manages concurrent access the single logical piece of mutable state: the current Root. doDelete is optimistic in that it is attempting to update head making the assumption that currentRootHash is the hash of the current head. The call to Commit below will return an 'ErrOptimisticLockFailed' error if that assumption fails (e.g. because of a race with another writer) and the entire algorithm must be tried again.
func (db *database) doDelete(ctx context.Context, datasetIDstr string) error {
	datasetID := types.String(datasetIDstr)
	currentRootHash, err := db.rt.Root(ctx)

	if err != nil {
		return err
	}

	currentDatasets, err := db.Datasets(ctx)

	if err != nil {
		return err
	}

	var initialHead types.Ref
	if r, hasHead, err := currentDatasets.MaybeGet(ctx, datasetID); err != nil {

	} else if !hasHead {
		return nil
	} else {
		initialHead = r.(types.Ref)
	}

	for {
		currentDatasets, err = currentDatasets.Edit().Remove(datasetID).Map(ctx)
		err = db.tryCommitChunks(ctx, currentDatasets, currentRootHash)
		if err != ErrOptimisticLockFailed {
			break
		}

		// If the optimistic lock failed because someone changed the Head of datasetID, then return ErrMergeNeeded. If it failed because someone changed a different Dataset, we should try again.
		currentRootHash, err = db.rt.Root(ctx)

		if err != nil {
			return err
		}

		currentDatasets, err = db.Datasets(ctx)

		if err != nil {
			return err
		}

		var r types.Value
		var hasHead bool
		if r, hasHead, err = currentDatasets.MaybeGet(ctx, datasetID); err != nil {
			return err
		} else if !hasHead || (hasHead && !initialHead.Equals(r)) {
			err = ErrMergeNeeded
			break
		}
	}
	return err
}

func (db *database) tryCommitChunks(ctx context.Context, currentDatasets types.Map, currentRootHash hash.Hash) error {
	newRoot, err := db.WriteValue(ctx, currentDatasets)

	if err != nil {
		return err
	}

	newRootHash := newRoot.TargetHash()

	if success, err := db.rt.Commit(ctx, newRootHash, currentRootHash); err != nil {
		return err
	} else if !success {
		return ErrOptimisticLockFailed
	}

	return nil
}

func (db *database) validateRefAsCommit(ctx context.Context, r types.Ref) (types.Struct, error) {
	v, err := db.ReadValue(ctx, r.TargetHash())

	if err != nil {
		return types.EmptyStruct(r.Format()), err
	}

	if v == nil {
		panic(r.TargetHash().String() + " not found")
	}

	is, err := IsCommit(v)

	if err != nil {
		return types.EmptyStruct(r.Format()), err
	}

	if !is {
		panic("Not a commit")
	}

	return v.(types.Struct), nil
}

func (db *database) validateTag(ctx context.Context, t types.Struct) error {
	is, err := IsTag(t)
	if err != nil {
		return err
	}
	if !is {
		return fmt.Errorf("Tag struct %s is malformed, IsTag() == false", t.String())
	}

	r, ok, err := t.MaybeGet(TagCommitRefField)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("tag is missing field %s", TagCommitRefField)
	}

	_, err = db.validateRefAsCommit(ctx, r.(types.Ref))

	if err != nil {
		return err
	}

	return nil
}

func buildNewCommit(ctx context.Context, ds Dataset, v types.Value, opts CommitOptions) (types.Struct, error) {
	parents := opts.ParentsList
	if parents == types.EmptyList || parents.Len() == 0 {
		var err error
		parents, err = types.NewList(ctx, ds.Database())
		if err != nil {
			return types.EmptyStruct(ds.Database().Format()), err
		}

		if headRef, ok, err := ds.MaybeHeadRef(); err != nil {
			return types.EmptyStruct(ds.Database().Format()), err
		} else if ok {
			le := parents.Edit().Append(headRef)
			parents, err = le.List(ctx)
			if err != nil {
				return types.EmptyStruct(ds.Database().Format()), err
			}
		}
	}

	meta := opts.Meta
	if meta.IsZeroValue() {
		meta = types.EmptyStruct(ds.Database().Format())
	}
	return NewCommit(ctx, v, parents, meta)
}

func (db *database) doHeadUpdate(ctx context.Context, ds Dataset, updateFunc func(ds Dataset) error) (Dataset, error) {
	err := updateFunc(ds)

	if err != nil {
		return Dataset{}, err
	}

	return db.GetDataset(ctx, ds.ID())
}
