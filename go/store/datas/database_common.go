// Copyright 2019-2022 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type database struct {
	*types.ValueStore
	rt rootTracker
}

var (
	ErrOptimisticLockFailed = errors.New("optimistic lock failed on database Root update")
	ErrMergeNeeded          = errors.New("dataset head is not ancestor of commit")
	ErrAlreadyCommitted     = errors.New("dataset head already pointing at given commit")
)

// rootTracker is a narrowing of the ChunkStore interface, to keep Database disciplined about working directly with Chunks
type rootTracker interface {
	Root(ctx context.Context) (hash.Hash, error)
	Commit(ctx context.Context, current, last hash.Hash) (bool, error)
}

func newDatabase(vs *types.ValueStore) *database {
	return &database{
		ValueStore: vs, // ValueStore is responsible for closing |cs|
		rt:         vs,
	}
}

var _ Database = &database{}
var _ GarbageCollector = &database{}

var _ rootTracker = &types.ValueStore{}
var _ GarbageCollector = &types.ValueStore{}

func (db *database) chunkStore() chunks.ChunkStore {
	return db.ChunkStore()
}

func (db *database) Stats() interface{} {
	return db.ChunkStore().Stats()
}

func (db *database) StatsSummary() string {
	return db.ChunkStore().StatsSummary()
}

// DatasetsInRoot returns the Map of datasets in the root represented by the |rootHash| given
func (db *database) loadDatasetsNomsMap(ctx context.Context, rootHash hash.Hash) (types.Map, error) {
	if rootHash.IsEmpty() {
		return types.NewMap(ctx, db)
	}

	val, err := db.ReadValue(ctx, rootHash)
	if err != nil {
		return types.EmptyMap, err
	}

	return val.(types.Map), nil
}

func (db *database) loadDatasetsRefmap(ctx context.Context, rootHash hash.Hash) (prolly.AddressMap, error) {
	if rootHash == (hash.Hash{}) {
		return prolly.NewEmptyAddressMap(tree.NewNodeStore(db.chunkStore())), nil
	}

	val, err := db.ReadValue(ctx, rootHash)
	if err != nil {
		return prolly.AddressMap{}, err
	}

	return parse_storeroot([]byte(val.(types.SerialMessage)), db.chunkStore()), nil
}

func getParentsClosure(ctx context.Context, vrw types.ValueReadWriter, parentRefsL types.List) (types.Ref, bool, error) {
	parentRefs := make([]types.Ref, int(parentRefsL.Len()))
	parents := make([]types.Struct, len(parentRefs))
	if len(parents) == 0 {
		return types.Ref{}, false, nil
	}
	err := parentRefsL.IterAll(ctx, func(v types.Value, i uint64) error {
		r, ok := v.(types.Ref)
		if !ok {
			return errors.New("parentsRef element was not a Ref")
		}
		parentRefs[int(i)] = r
		tv, err := r.TargetValue(ctx, vrw)
		if err != nil {
			return err
		}
		s, ok := tv.(types.Struct)
		if !ok {
			return errors.New("parentRef target value was not a Struct")
		}
		parents[int(i)] = s
		return nil
	})
	if err != nil {
		return types.Ref{}, false, err
	}
	parentMaps := make([]types.Map, len(parents))
	parentParentLists := make([]types.List, len(parents))
	for i, p := range parents {
		v, ok, err := p.MaybeGet(parentsClosureField)
		if err != nil {
			return types.Ref{}, false, err
		}
		if !ok || types.IsNull(v) {
			empty, err := types.NewMap(ctx, vrw)
			if err != nil {
				return types.Ref{}, false, err
			}
			parentMaps[i] = empty
		} else {
			r, ok := v.(types.Ref)
			if !ok {
				return types.Ref{}, false, errors.New("unexpected field value type for parents_closure in commit struct")
			}
			tv, err := r.TargetValue(ctx, vrw)
			if err != nil {
				return types.Ref{}, false, err
			}
			parentMaps[i], ok = tv.(types.Map)
			if !ok {
				return types.Ref{}, false, fmt.Errorf("unexpected target value type for parents_closure in commit struct: %v", tv)
			}
		}
		v, ok, err = p.MaybeGet(parentsListField)
		if err != nil {
			return types.Ref{}, false, err
		}
		if !ok || types.IsNull(v) {
			empty, err := types.NewList(ctx, vrw)
			if err != nil {
				return types.Ref{}, false, err
			}
			parentParentLists[i] = empty
		} else {
			parentParentLists[i], ok = v.(types.List)
			if !ok {
				return types.Ref{}, false, errors.New("unexpected field value or type for parents_list in commit struct")
			}
		}
		if parentMaps[i].Len() == 0 && parentParentLists[i].Len() != 0 {
			// If one of the commits has an empty parents_closure, but non-empty parents, we will not record
			// a parents_closure here.
			return types.Ref{}, false, nil
		}
	}
	// Convert parent lists to List<Ref<Value>>
	for i, l := range parentParentLists {
		newRefs := make([]types.Value, int(l.Len()))
		err := l.IterAll(ctx, func(v types.Value, i uint64) error {
			r, ok := v.(types.Ref)
			if !ok {
				return errors.New("unexpected entry type for parents_list in commit struct")
			}
			newRefs[int(i)], err = types.ToRefOfValue(r, vrw.Format())
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return types.Ref{}, false, err
		}
		parentParentLists[i], err = types.NewList(ctx, vrw, newRefs...)
		if err != nil {
			return types.Ref{}, false, err
		}
	}
	editor := parentMaps[0].Edit()
	for i, r := range parentRefs {
		h := r.TargetHash()
		key, err := types.NewTuple(vrw.Format(), types.Uint(r.Height()), types.InlineBlob(h[:]))
		if err != nil {
			editor.Close(ctx)
			return types.Ref{}, false, err
		}
		editor.Set(key, parentParentLists[i])
	}
	for i := 1; i < len(parentMaps); i++ {
		changes := make(chan types.ValueChanged)
		var derr error
		go func() {
			defer close(changes)
			derr = parentMaps[1].Diff(ctx, parentMaps[0], changes)
		}()
		for c := range changes {
			if c.ChangeType == types.DiffChangeAdded {
				editor.Set(c.Key, c.NewValue)
			}
		}
		if derr != nil {
			editor.Close(ctx)
			return types.Ref{}, false, derr
		}
	}
	m, err := editor.Map(ctx)
	if err != nil {
		return types.Ref{}, false, err
	}
	r, err := vrw.WriteValue(ctx, m)
	if err != nil {
		return types.Ref{}, false, err
	}
	r, err = types.ToRefOfValue(r, vrw.Format())
	if err != nil {
		return types.Ref{}, false, err
	}
	return r, true, nil
}

type refmapDatasetsMap struct {
	am prolly.AddressMap
}

func (m refmapDatasetsMap) Len() uint64 {
	return uint64(m.am.Count())
}

func (m refmapDatasetsMap) IterAll(ctx context.Context, cb func(string, hash.Hash) error) error {
	return m.am.IterAll(ctx, cb)
}

type nomsDatasetsMap struct {
	m types.Map
}

func (m nomsDatasetsMap) Len() uint64 {
	return m.m.Len()
}

func (m nomsDatasetsMap) IterAll(ctx context.Context, cb func(string, hash.Hash) error) error {
	return m.m.IterAll(ctx, func(k, v types.Value) error {
		// TODO: very fast and loose with error checking here.
		return cb(string(k.(types.String)), v.(types.Ref).TargetHash())
	})
}

// Datasets returns the Map of Datasets in the current root. If you intend to edit the map and commit changes back,
// then you should fetch the current root, then call DatasetsInRoot with that hash. Otherwise another writer could
// change the root value between when you get the root hash and call this method.
func (db *database) Datasets(ctx context.Context) (DatasetsMap, error) {
	rootHash, err := db.rt.Root(ctx)
	if err != nil {
		return nil, err
	}

	if db.Format().UsesFlatbuffers() {
		rm, err := db.loadDatasetsRefmap(ctx, rootHash)
		if err != nil {
			return nil, err
		}
		return refmapDatasetsMap{rm}, nil
	}

	m, err := db.loadDatasetsNomsMap(ctx, rootHash)
	if err != nil {
		return nil, err
	}

	return nomsDatasetsMap{m}, nil
}

var ErrInvalidDatasetID = errors.New("Invalid dataset ID")

func (db *database) GetDataset(ctx context.Context, datasetID string) (Dataset, error) {
	// precondition checks
	if !DatasetFullRe.MatchString(datasetID) {
		return Dataset{}, fmt.Errorf("%w: %s", ErrInvalidDatasetID, datasetID)
	}

	datasets, err := db.Datasets(ctx)
	if err != nil {
		return Dataset{}, err
	}

	return db.datasetFromMap(ctx, datasetID, datasets)
}

func (db *database) datasetFromMap(ctx context.Context, datasetID string, dsmap DatasetsMap) (Dataset, error) {
	if ndsmap, ok := dsmap.(nomsDatasetsMap); ok {
		datasets := ndsmap.m
		var headAddr hash.Hash
		var head types.Value
		if r, ok, err := datasets.MaybeGet(ctx, types.String(datasetID)); err != nil {
			return Dataset{}, err
		} else if ok {
			headAddr = r.(types.Ref).TargetHash()
			head, err = r.(types.Ref).TargetValue(ctx, db)
			if err != nil {
				return Dataset{}, err
			}
		}
		return newDataset(db, datasetID, head, headAddr)
	} else if rmdsmap, ok := dsmap.(refmapDatasetsMap); ok {
		var err error
		curr, err := rmdsmap.am.Get(ctx, datasetID)
		if err != nil {
			return Dataset{}, err
		}
		var head types.Value
		if !curr.IsEmpty() {
			head, err = db.ReadValue(ctx, curr)
			if err != nil {
				return Dataset{}, err
			}
		}
		return newDataset(db, datasetID, head, curr)
	} else {
		return Dataset{}, errors.New("unimplemented or unsupported DatasetsMap type")
	}
}

func (db *database) readHead(ctx context.Context, addr hash.Hash) (dsHead, error) {
	head, err := db.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}
	return newHead(head, addr)
}

func (db *database) Close() error {
	return db.ValueStore.Close()
}

func (db *database) SetHead(ctx context.Context, ds Dataset, newHeadAddr hash.Hash) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error { return db.doSetHead(ctx, ds, newHeadAddr) })
}

func (db *database) doSetHead(ctx context.Context, ds Dataset, addr hash.Hash) error {
	newHead, err := db.readHead(ctx, addr)
	if err != nil {
		return err
	}

	newVal := newHead.value()

	headType := newHead.TypeName()
	switch headType {
	case commitName:
		iscommit, err := IsCommit(newVal)
		if err != nil {
			return err
		}
		if !iscommit {
			return fmt.Errorf("SetHead failed: reffered to value is not a commit:")
		}
	case tagName:
		istag, err := IsTag(newVal)
		if err != nil {
			return err
		}
		if !istag {
			return fmt.Errorf("SetHead failed: reffered to value is not a tag:")
		}
		_, commitaddr, err := newHead.HeadTag()
		if err != nil {
			return err
		}
		commitval, err := db.ReadValue(ctx, commitaddr)
		if err != nil {
			return err
		}
		iscommit, err := IsCommit(commitval)
		if err != nil {
			return err
		}
		if !iscommit {
			return fmt.Errorf("SetHead failed: reffered to value is not a tag:")
		}
	default:
		return fmt.Errorf("Unrecognized dataset value: %s", headType)
	}

	key := types.String(ds.ID())

	return db.update(ctx, func(ctx context.Context, datasets types.Map) (types.Map, error) {
		currRef, ok, err := datasets.MaybeGet(ctx, key)
		if err != nil {
			return types.Map{}, err
		}
		if ok {
			currSt, err := currRef.(types.Ref).TargetValue(ctx, db)
			if err != nil {
				return types.Map{}, err
			}
			currType := currSt.(types.Struct).Name()
			if currType != headType {
				return types.Map{}, fmt.Errorf("cannot change type of head; currently points at %s but new value would point at %s", currType, headType)
			}
		}

		vref, err := types.NewRef(newVal, db.Format())
		if err != nil {
			return types.Map{}, err
		}

		ref, err := types.ToRefOfValue(vref, db.Format())
		if err != nil {
			return types.Map{}, err
		}

		return datasets.Edit().Set(key, ref).Map(ctx)
	}, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
		curr, err := am.Get(ctx, ds.ID())
		if err != nil {
			return prolly.AddressMap{}, err
		}
		if curr != (hash.Hash{}) {
			currHead, err := db.readHead(ctx, curr)
			if err != nil {
				return prolly.AddressMap{}, err
			}
			currType := currHead.TypeName()
			if currType != headType {
				return prolly.AddressMap{}, fmt.Errorf("cannot change type of head; currently points at %s but new value would point at %s", currType, headType)
			}
		}
		h, err := newVal.Hash(db.Format())
		if err != nil {
			return prolly.AddressMap{}, err
		}
		ae := am.Editor()
		err = ae.Update(ctx, ds.ID(), h)
		if err != nil {
			return prolly.AddressMap{}, err
		}
		return ae.Flush(ctx)
	})
}

func (db *database) FastForward(ctx context.Context, ds Dataset, newHeadAddr hash.Hash) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error { return db.doFastForward(ctx, ds, newHeadAddr) })
}

func (db *database) doFastForward(ctx context.Context, ds Dataset, newHeadAddr hash.Hash) error {
	newHead, err := db.readHead(ctx, newHeadAddr)
	if err != nil {
		return err
	}
	if newHead == nil {
		return fmt.Errorf("FastForward: new head address %v not found", newHeadAddr)
	}
	if newHead.TypeName() != commitName {
		return fmt.Errorf("FastForward: target value of new head address %v is not a commit.", newHeadAddr)
	}

	v := newHead.value()
	iscommit, err := IsCommit(v)
	if err != nil {
		return err
	}
	if !iscommit {
		return fmt.Errorf("FastForward: target value of new head address %v is not a commit.", newHeadAddr)
	}

	newCommit, err := commitFromValue(db.Format(), v)
	if err != nil {
		return err
	}

	currentHeadAddr, ok := ds.MaybeHeadAddr()
	if ok {
		currentHeadValue, _ := ds.MaybeHead()
		currCommit, err := commitFromValue(db.Format(), currentHeadValue)
		if err != nil {
			return err
		}
		ancestorHash, found, err := FindCommonAncestor(ctx, currCommit, newCommit, db, db)
		if err != nil {
			return err
		}
		if !found || mergeNeeded(currentHeadAddr, ancestorHash) {
			return ErrMergeNeeded
		}
	}

	err = db.doCommit(ctx, ds.ID(), currentHeadAddr, v)
	if err == ErrAlreadyCommitted {
		return nil
	}
	return err
}

func (db *database) Commit(ctx context.Context, ds Dataset, v types.Value, opts CommitOptions) (Dataset, error) {
	currentAddr, _ := ds.MaybeHeadAddr()
	commit, err := buildNewCommit(ctx, ds, v, opts)
	if err != nil {
		return Dataset{}, err
	}

	val := commit.NomsValue()

	_, err = db.WriteValue(ctx, val)
	if err != nil {
		return Dataset{}, err
	}

	return db.doHeadUpdate(
		ctx,
		ds,
		func(ds Dataset) error {
			return db.doCommit(ctx, ds.ID(), currentAddr, val)
		},
	)
}

// Calls db.Commit with empty CommitOptions{}.
func CommitValue(ctx context.Context, db Database, ds Dataset, v types.Value) (Dataset, error) {
	return db.Commit(ctx, ds, v, CommitOptions{})
}

func (db *database) doCommit(ctx context.Context, datasetID string, datasetCurrentAddr hash.Hash, newCommitValue types.Value) error {
	return db.update(ctx, func(ctx context.Context, datasets types.Map) (types.Map, error) {
		curr, hasHead, err := datasets.MaybeGet(ctx, types.String(datasetID))
		if err != nil {
			return types.Map{}, err
		}

		newCommitRef, err := types.NewRef(newCommitValue, db.Format())
		if err != nil {
			return types.Map{}, err
		}

		newCommitValueRef, err := types.ToRefOfValue(newCommitRef, db.Format())
		if err != nil {
			return types.Map{}, err
		}

		if hasHead {
			currRef := curr.(types.Ref)
			if currRef.TargetHash() != datasetCurrentAddr {
				return types.Map{}, ErrMergeNeeded
			}
			if currRef.TargetHash() == newCommitValueRef.TargetHash() {
				return types.Map{}, ErrAlreadyCommitted
			}
		} else if datasetCurrentAddr != (hash.Hash{}) {
			return types.Map{}, ErrMergeNeeded
		}

		return datasets.Edit().Set(types.String(datasetID), newCommitValueRef).Map(ctx)
	}, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
		curr, err := am.Get(ctx, datasetID)
		if err != nil {
			return prolly.AddressMap{}, err
		}
		if curr != datasetCurrentAddr {
			return prolly.AddressMap{}, ErrMergeNeeded
		}
		h, err := newCommitValue.Hash(db.Format())
		if err != nil {
			return prolly.AddressMap{}, err
		}
		if curr != (hash.Hash{}) {
			if curr == h {
				return prolly.AddressMap{}, ErrAlreadyCommitted
			}
		}
		ae := am.Editor()
		err = ae.Update(ctx, datasetID, h)
		if err != nil {
			return prolly.AddressMap{}, err
		}
		return ae.Flush(ctx)
	})
}

func mergeNeeded(currentAddr hash.Hash, ancestorAddr hash.Hash) bool {
	return currentAddr != ancestorAddr
}

func (db *database) Tag(ctx context.Context, ds Dataset, commitAddr hash.Hash, opts TagOptions) (Dataset, error) {
	return db.doHeadUpdate(
		ctx,
		ds,
		func(ds Dataset) error {
			addr, tagRef, err := newTag(ctx, db, commitAddr, opts.Meta)
			if err != nil {
				return err
			}
			return db.doTag(ctx, ds.ID(), addr, tagRef)
		},
	)
}

// doTag manages concurrent access the single logical piece of mutable state: the current Root. It uses
// the same optimistic writing algorithm as doCommit (see above).
func (db *database) doTag(ctx context.Context, datasetID string, tagAddr hash.Hash, tagRef types.Ref) error {
	return db.update(ctx, func(ctx context.Context, datasets types.Map) (types.Map, error) {
		_, hasHead, err := datasets.MaybeGet(ctx, types.String(datasetID))
		if err != nil {
			return types.Map{}, err
		}
		if hasHead {
			return types.Map{}, fmt.Errorf("tag %s already exists and cannot be altered after creation", datasetID)
		}

		return datasets.Edit().Set(types.String(datasetID), tagRef).Map(ctx)
	}, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
		curr, err := am.Get(ctx, datasetID)
		if err != nil {
			return prolly.AddressMap{}, err
		}
		if curr != (hash.Hash{}) {
			return prolly.AddressMap{}, fmt.Errorf("tag %s already exists and cannot be altered after creation", datasetID)
		}
		ae := am.Editor()
		err = ae.Update(ctx, datasetID, tagAddr)
		if err != nil {
			return prolly.AddressMap{}, err
		}
		return ae.Flush(ctx)
	})
}

func (db *database) UpdateWorkingSet(ctx context.Context, ds Dataset, workingSet WorkingSetSpec, prevHash hash.Hash) (Dataset, error) {
	return db.doHeadUpdate(
		ctx,
		ds,
		func(ds Dataset) error {
			addr, ref, err := newWorkingSet(ctx, db, workingSet.Meta, workingSet.WorkingRoot, workingSet.StagedRoot, workingSet.MergeState)
			if err != nil {
				return err
			}
			return db.doUpdateWorkingSet(ctx, ds.ID(), addr, ref, prevHash)
		},
	)
}

// Update the entry in the datasets map for |datasetID| to point to a ref of
// |workingSet|. Unlike |doCommit|, |doTag|, etc., this method requires a
// compare-and-set for the current target hash of the datasets entry, and will
// return an error if the application is working with a stale value for the
// workingset.
func (db *database) doUpdateWorkingSet(ctx context.Context, datasetID string, addr hash.Hash, ref types.Ref, currHash hash.Hash) error {
	return db.update(ctx, func(ctx context.Context, datasets types.Map) (types.Map, error) {
		success, err := assertDatasetHash(ctx, datasets, datasetID, currHash)
		if err != nil {
			return types.Map{}, err
		}
		if !success {
			return types.Map{}, ErrOptimisticLockFailed
		}

		return datasets.Edit().Set(types.String(datasetID), ref).Map(ctx)
	}, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
		curr, err := am.Get(ctx, datasetID)
		if err != nil {
			return prolly.AddressMap{}, err
		}
		if curr != currHash {
			return prolly.AddressMap{}, ErrOptimisticLockFailed
		}
		ae := am.Editor()
		err = ae.Update(ctx, datasetID, addr)
		if err != nil {
			return prolly.AddressMap{}, err
		}
		return ae.Flush(ctx)
	})
}

// assertDatasetHash returns true if the hash of the dataset matches the one given. Use an empty hash for a dataset you
// expect not to exist.
// Typically this is called using optimistic locking by the caller in order to implement atomic test-and-set semantics.
func assertDatasetHash(
	ctx context.Context,
	datasets types.Map,
	datasetID string,
	currHash hash.Hash,
) (bool, error) {
	curr, ok, err := datasets.MaybeGet(ctx, types.String(datasetID))
	if err != nil {
		return false, err
	}
	if !ok {
		return currHash.IsEmpty(), nil
	}
	return curr.(types.Ref).TargetHash().Equal(currHash), nil
}

// CommitWithWorkingSet updates two Datasets atomically: the working set, and its corresponding HEAD. Uses the same
// global locking mechanism as UpdateWorkingSet.
func (db *database) CommitWithWorkingSet(
	ctx context.Context,
	commitDS, workingSetDS Dataset,
	val types.Value, workingSetSpec WorkingSetSpec,
	prevWsHash hash.Hash, opts CommitOptions,
) (Dataset, Dataset, error) {
	wsAddr, wsValRef, err := newWorkingSet(ctx, db, workingSetSpec.Meta, workingSetSpec.WorkingRoot, workingSetSpec.StagedRoot, workingSetSpec.MergeState)
	if err != nil {
		return Dataset{}, Dataset{}, err
	}

	commit, err := buildNewCommit(ctx, commitDS, val, opts)
	if err != nil {
		return Dataset{}, Dataset{}, err
	}

	commitRef, err := db.WriteValue(ctx, commit.NomsValue())
	if err != nil {
		return Dataset{}, Dataset{}, err
	}

	commitValRef, err := types.ToRefOfValue(commitRef, db.Format())
	if err != nil {
		return Dataset{}, Dataset{}, err
	}

	currDSHash, _ := commitDS.MaybeHeadAddr()

	err = db.update(ctx, func(ctx context.Context, datasets types.Map) (types.Map, error) {
		success, err := assertDatasetHash(ctx, datasets, workingSetDS.ID(), prevWsHash)
		if err != nil {
			return types.Map{}, err
		}

		if !success {
			return types.Map{}, ErrOptimisticLockFailed
		}

		var currDS hash.Hash

		if r, hasHead, err := datasets.MaybeGet(ctx, types.String(commitDS.ID())); err != nil {
			return types.Map{}, err
		} else if hasHead {
			currDS = r.(types.Ref).TargetHash()
		}

		if currDS != currDSHash {
			return types.Map{}, ErrMergeNeeded
		}

		return datasets.Edit().
			Set(types.String(workingSetDS.ID()), wsValRef).
			Set(types.String(commitDS.ID()), commitValRef).
			Map(ctx)
	}, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
		currWS, err := am.Get(ctx, workingSetDS.ID())
		if err != nil {
			return prolly.AddressMap{}, err
		}
		if currWS != prevWsHash {
			return prolly.AddressMap{}, ErrOptimisticLockFailed
		}
		currDS, err := am.Get(ctx, commitDS.ID())
		if err != nil {
			return prolly.AddressMap{}, err
		}
		if currDS != currDSHash {
			return prolly.AddressMap{}, ErrMergeNeeded
		}
		ae := am.Editor()
		err = ae.Update(ctx, commitDS.ID(), commitValRef.TargetHash())
		if err != nil {
			return prolly.AddressMap{}, err
		}
		err = ae.Update(ctx, workingSetDS.ID(), wsAddr)
		if err != nil {
			return prolly.AddressMap{}, err
		}
		return ae.Flush(ctx)
	})

	if err != nil {
		return Dataset{}, Dataset{}, err
	}

	currentDatasets, err := db.Datasets(ctx)
	if err != nil {
		return Dataset{}, Dataset{}, err
	}

	commitDS, err = db.datasetFromMap(ctx, commitDS.ID(), currentDatasets)
	if err != nil {
		return Dataset{}, Dataset{}, err
	}

	workingSetDS, err = db.datasetFromMap(ctx, workingSetDS.ID(), currentDatasets)
	if err != nil {
		return Dataset{}, Dataset{}, err
	}

	return commitDS, workingSetDS, nil
}

func (db *database) Delete(ctx context.Context, ds Dataset) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error { return db.doDelete(ctx, ds.ID()) })
}

func (db *database) update(ctx context.Context,
	edit func(context.Context, types.Map) (types.Map, error),
	editFB func(context.Context, prolly.AddressMap) (prolly.AddressMap, error)) error {
	var (
		err      error
		root     hash.Hash
		datasets types.Map
	)
	for {
		root, err = db.rt.Root(ctx)
		if err != nil {
			return err
		}

		var newRootHash hash.Hash

		if db.Format().UsesFlatbuffers() {
			datasets, err := db.loadDatasetsRefmap(ctx, root)
			if err != nil {
				return err
			}

			datasets, err = editFB(ctx, datasets)
			if err != nil {
				return err
			}

			data := storeroot_flatbuffer(datasets)
			r, err := db.WriteValue(ctx, types.SerialMessage(data))
			if err != nil {
				return err
			}

			newRootHash = r.TargetHash()
		} else {
			datasets, err = db.loadDatasetsNomsMap(ctx, root)
			if err != nil {
				return err
			}

			datasets, err = edit(ctx, datasets)
			if err != nil {
				return err
			}

			newRoot, err := db.WriteValue(ctx, datasets)
			if err != nil {
				return err
			}

			newRootHash = newRoot.TargetHash()
		}

		err = db.tryCommitChunks(ctx, newRootHash, root)
		if err != ErrOptimisticLockFailed {
			break
		}
	}
	return err
}

func (db *database) doDelete(ctx context.Context, datasetIDstr string) error {
	var first types.Value
	var firstHash hash.Hash

	datasetID := types.String(datasetIDstr)
	return db.update(ctx, func(ctx context.Context, datasets types.Map) (types.Map, error) {
		curr, ok, err := datasets.MaybeGet(ctx, datasetID)
		if err != nil {
			return types.Map{}, err
		} else if !ok {
			if first != nil {
				return types.Map{}, ErrMergeNeeded
			}
			return datasets, nil
		} else if first == nil {
			first = curr
		} else if !first.Equals(curr) {
			return types.Map{}, ErrMergeNeeded
		}
		return datasets.Edit().Remove(datasetID).Map(ctx)
	}, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
		curr, err := am.Get(ctx, datasetIDstr)
		if err != nil {
			return prolly.AddressMap{}, err
		}
		if curr != (hash.Hash{}) && firstHash == (hash.Hash{}) {
			firstHash = curr
		}
		if curr != firstHash {
			return prolly.AddressMap{}, ErrMergeNeeded
		}
		ae := am.Editor()
		err = ae.Delete(ctx, datasetIDstr)
		if err != nil {
			return prolly.AddressMap{}, err
		}
		return ae.Flush(ctx)
	})
}

// GC traverses the database starting at the Root and removes all unreferenced data from persistent storage.
func (db *database) GC(ctx context.Context, oldGenRefs, newGenRefs hash.HashSet) error {
	return db.ValueStore.GC(ctx, oldGenRefs, newGenRefs)
}

func (db *database) tryCommitChunks(ctx context.Context, newRootHash hash.Hash, currentRootHash hash.Hash) error {
	if success, err := db.rt.Commit(ctx, newRootHash, currentRootHash); err != nil {
		return err
	} else if !success {
		return ErrOptimisticLockFailed
	}
	return nil
}

func (db *database) validateRefAsCommit(ctx context.Context, r types.Ref) (types.Struct, error) {
	rHead, err := db.readHead(ctx, r.TargetHash())
	if err != nil {
		return types.Struct{}, err
	}
	if rHead == nil {
		return types.Struct{}, fmt.Errorf("validateRefAsCommit: unable to validate ref; %s not found", r.TargetHash().String())
	}
	if rHead.TypeName() != commitName {
		return types.Struct{}, fmt.Errorf("validateRefAsCommit: referred valus is not a commit")
	}

	var v types.Value
	v = rHead.(nomsHead).st

	is, err := IsCommit(v)

	if err != nil {
		return types.Struct{}, err
	}

	if !is {
		return types.Struct{}, fmt.Errorf("validateRefAsCommit: referred valus is not a commit")
	}

	return v.(types.Struct), nil
}

func buildNewCommit(ctx context.Context, ds Dataset, v types.Value, opts CommitOptions) (*Commit, error) {
	if opts.Parents == nil || len(opts.Parents) == 0 {
		headAddr, ok := ds.MaybeHeadAddr()
		if ok {
			opts.Parents = []hash.Hash{headAddr}
		}
	} else {
		curr, ok := ds.MaybeHeadAddr()
		if ok {
			found := false
			for _, h := range opts.Parents {
				if h == curr {
					found = true
					break
				}
			}
			if !found {
				return nil, ErrMergeNeeded
			}
		}
	}

	return newCommitForValue(ctx, ds.db, v, opts)
}

func (db *database) doHeadUpdate(ctx context.Context, ds Dataset, updateFunc func(ds Dataset) error) (Dataset, error) {
	err := updateFunc(ds)
	if err != nil {
		return Dataset{}, err
	}

	return db.GetDataset(ctx, ds.ID())
}
