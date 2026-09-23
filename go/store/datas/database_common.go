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

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type database struct {
	*types.ValueStore
	rt rootTracker
	ns tree.NodeStore
}

var (
	ErrOptimisticLockFailed = errors.New("optimistic lock failed on database Root update")
	ErrMergeNeeded          = errors.New("dataset head is not ancestor of commit")
	ErrAlreadyCommitted     = errors.New("dataset head already pointing at given commit")
	ErrDirtyWorkspace       = errors.New("target has uncommitted changes. --force required to overwrite")
)

// rootTracker is a narrowing of the ChunkStore interface, to keep Database disciplined about working directly with Chunks
type rootTracker interface {
	Root(ctx context.Context) (hash.Hash, error)
	Commit(ctx context.Context, current, last hash.Hash) (bool, error)
}

func newDatabase(vs *types.ValueStore, ns tree.NodeStore) *database {
	return &database{
		ValueStore: vs, // ValueStore is responsible for closing |cs|
		rt:         vs,
		ns:         ns,
	}
}

var _ Database = &database{}
var _ GarbageCollector = &database{}

var _ rootTracker = &types.ValueStore{}
var _ GarbageCollector = &types.ValueStore{}

func (db *database) chunkStore() chunks.ChunkStore {
	return db.ChunkStore()
}

func (db *database) nodeStore() tree.NodeStore {
	return db.ns
}

func (db *database) Stats() interface{} {
	return db.ChunkStore().Stats()
}

func (db *database) StatsSummary() string {
	return db.ChunkStore().StatsSummary()
}

func (db *database) loadDatasetsRefmap(ctx context.Context, rootHash hash.Hash) (prolly.AddressMap, error) {
	if rootHash.IsEmpty() {
		return prolly.NewEmptyAddressMap(db.ns)
	}

	val, err := db.ReadValue(ctx, rootHash)
	if err != nil {
		return prolly.AddressMap{}, err
	}

	if val == nil {
		return prolly.AddressMap{}, fmt.Errorf("root hash doesn't exist: %s", rootHash)
	}

	return parse_storeroot(val.(types.SerialMessage), db.nodeStore())
}

type refmapDatasetsMap struct {
	am prolly.AddressMap
}

func (m refmapDatasetsMap) Len() (uint64, error) {
	c, err := m.am.Count()
	return uint64(c), err
}

func (m refmapDatasetsMap) IterAll(ctx context.Context, cb func(string, hash.Hash) error) error {
	return m.am.IterAll(ctx, cb)
}

// Datasets returns the Map of Datasets in the current root. If you intend to edit the map and commit changes back,
// then you should fetch the current root, then call DatasetsInRoot with that hash. Otherwise, another writer could
// change the root value between when you get the root hash and call this method.
func (db *database) Datasets(ctx context.Context) (DatasetsMap, error) {
	rootHash, err := db.rt.Root(ctx)
	if err != nil {
		return nil, err
	}

	rm, err := db.loadDatasetsRefmap(ctx, rootHash)
	if err != nil {
		return nil, err
	}
	return refmapDatasetsMap{rm}, nil
}

var ErrInvalidDatasetID = errors.New("Invalid dataset ID")

func (db *database) GetDataset(ctx context.Context, datasetID string) (Dataset, error) {
	// precondition checks
	if err := ValidateDatasetId(datasetID); err != nil {
		return Dataset{}, fmt.Errorf("%w: %s", err, datasetID)
	}

	datasets, err := db.Datasets(ctx)
	if err != nil {
		return Dataset{}, err
	}

	return db.datasetFromMap(ctx, datasetID, datasets)
}

func (db *database) GetDatasetByRootHash(ctx context.Context, datasetID string, rootHash hash.Hash) (Dataset, error) {
	// precondition checks
	if err := ValidateDatasetId(datasetID); err != nil {
		return Dataset{}, fmt.Errorf("%w: %s", err, datasetID)
	}

	datasets, err := db.DatasetsByRootHash(ctx, rootHash)
	if err != nil {
		return Dataset{}, err
	}

	return db.datasetFromMap(ctx, datasetID, datasets)
}

func (db *database) DatasetsByRootHash(ctx context.Context, rootHash hash.Hash) (DatasetsMap, error) {
	rm, err := db.loadDatasetsRefmap(ctx, rootHash)
	if err != nil {
		return nil, err
	}
	return refmapDatasetsMap{rm}, nil
}

func (db *database) datasetFromMap(ctx context.Context, datasetID string, dsmap DatasetsMap) (Dataset, error) {
	if rmdsmap, ok := dsmap.(refmapDatasetsMap); ok {
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
		return newDataset(ctx, db, datasetID, head, curr)
	} else {
		return Dataset{}, errors.New("unimplemented or unsupported DatasetsMap type")
	}
}

func (db *database) readHead(ctx context.Context, addr hash.Hash) (dsHead, error) {
	head, err := db.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}
	return newHead(ctx, head, addr)
}

func (db *database) Close() error {
	return db.ValueStore.Close()
}

// Precondition validates a pending update to the dataset map before it's
// finalized. It receives the current |datasets| and the |targetID| being
// written, and returning an error aborts the update.
//
// It runs on each write attempt, before the database's compare-and-swap,
// re-validating against the latest datasets when a concurrent
// write forces a retry.
type Precondition func(ctx context.Context, datasets prolly.AddressMap, targetID string) error

func (db *database) SetHead(ctx context.Context, ds Dataset, newHeadAddr hash.Hash, workingSetPath string, preconditions ...Precondition) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error { return db.doSetHead(ctx, ds, newHeadAddr, workingSetPath, preconditions) })
}

func (db *database) doSetHead(ctx context.Context, ds Dataset, addr hash.Hash, workingSetPath string, preconditions []Precondition) error {
	newHead, err := db.readHead(ctx, addr)
	if err != nil {
		return err
	}

	if newHead == nil {
		// This can happen on an attempt to set a head to an address which does not exist in the database.
		return fmt.Errorf("SetHead failed: attempt to set a dataset head to an address which is not in the store")
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
			return fmt.Errorf("SetHead failed: referred to value is not a commit:")
		}
	case tagName:
		istag, err := IsTag(ctx, newVal)
		if err != nil {
			return err
		}
		if !istag {
			return fmt.Errorf("SetHead failed: referred to value is not a tag:")
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
			return fmt.Errorf("SetHead failed: referred to value is not a tag:")
		}
	default:
		return fmt.Errorf("Unrecognized dataset value: %s", headType)
	}

	_, err = db.update(ctx, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
		for _, check := range preconditions {
			if err := check(ctx, am, ds.ID()); err != nil {
				return prolly.AddressMap{}, err
			}
		}

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

		var newWSHash hash.Hash
		if workingSetPath != "" {
			hasWS, err := am.Has(ctx, workingSetPath)
			if err != nil {
				return prolly.AddressMap{}, err
			}
			cmtRtHsh, err := GetCommitRootHash(newVal)
			if err != nil {
				return prolly.AddressMap{}, err
			}
			// If the current root has a working set, assert it isn't dirty and then update it.
			// If this branch does not have a working set yet, create it to match the commit contents.
			if hasWS {
				currWSHash, err := am.Get(ctx, workingSetPath)
				if err != nil {
					return prolly.AddressMap{}, err
				}

				targetCmt, err := db.ReadValue(ctx, currWSHash)
				if err != nil {
					return prolly.AddressMap{}, err
				}

				if _, ok := targetCmt.(types.SerialMessage); ok {
					// TODO - construct new meta instance rather than using the default
					updateWS := workingset_flatbuffer(cmtRtHsh, &cmtRtHsh, nil, nil, nil)
					ref, err := db.WriteValue(ctx, types.SerialMessage(updateWS))
					if err != nil {
						return prolly.AddressMap{}, err
					}
					newWSHash = ref.TargetHash()
				} else {
					// This _should_ never happen. We've already ended up on this code path because we are on
					// modern storage.
					return prolly.AddressMap{}, errors.New("Modern Dolt Database required.")
				}
			} else {
				// TODO - construct new meta instance rather than using the default
				updateWS := workingset_flatbuffer(cmtRtHsh, &cmtRtHsh, nil, nil, nil)
				ref, err := db.WriteValue(ctx, types.SerialMessage(updateWS))
				if err != nil {
					return prolly.AddressMap{}, err
				}
				newWSHash = ref.TargetHash()
			}
		}

		ae := am.Editor()
		err = ae.Update(ctx, ds.ID(), h)
		if err != nil {
			return prolly.AddressMap{}, err
		}

		if workingSetPath != "" && newWSHash != (hash.Hash{}) {
			err = ae.Update(ctx, workingSetPath, newWSHash)
			if err != nil {
				return prolly.AddressMap{}, err
			}
		}

		return ae.Flush(ctx)
	})

	return err
}

func (db *database) FastForward(ctx context.Context, ds Dataset, newHeadAddr hash.Hash, wsPath string, allowDirtyWorking bool) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error {
		return db.doFastForward(ctx, ds, newHeadAddr, wsPath, allowDirtyWorking)
	})
}

func (db *database) doFastForward(ctx context.Context, ds Dataset, newHeadAddr hash.Hash, workingSetPath string, allowDirtyWorking bool) error {
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

	cmtValue := newHead.value()
	iscommit, err := IsCommit(cmtValue)
	if err != nil {
		return err
	}
	if !iscommit {
		return fmt.Errorf("FastForward: target value of new head address %v is not a commit.", newHeadAddr)
	}

	newCommit, err := CommitFromValue(db.Format(), cmtValue)
	if err != nil {
		return err
	}

	currentHeadAddr, ok := ds.MaybeHeadAddr()
	if ok {
		currentHeadValue, _ := ds.MaybeHead()
		currCommit, err := CommitFromValue(db.Format(), currentHeadValue)
		if err != nil {
			return err
		}
		ancestorHash, found, err := FindCommonAncestor(ctx, currCommit, newCommit, db, db, db.ns, db.ns)
		if err != nil {
			return err
		}
		if !found || mergeNeeded(currentHeadAddr, ancestorHash) {
			return ErrMergeNeeded
		}
	}

	_, err = db.update(ctx, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
		curr, err := am.Get(ctx, ds.ID())
		if err != nil {
			return prolly.AddressMap{}, err
		}
		if curr != currentHeadAddr {
			return prolly.AddressMap{}, ErrMergeNeeded
		}
		h, err := cmtValue.Hash(db.Format())
		if err != nil {
			return prolly.AddressMap{}, err
		}
		if curr != (hash.Hash{}) {
			if curr == h {
				return prolly.AddressMap{}, ErrAlreadyCommitted
			}
		}

		var newWSHash hash.Hash
		if workingSetPath != "" {
			hasWS, err := am.Has(ctx, workingSetPath)
			if err != nil {
				return prolly.AddressMap{}, err
			}
			cmtRtHsh, err := GetCommitRootHash(cmtValue)
			if err != nil {
				return prolly.AddressMap{}, err
			}
			// If the current root has a working set, assert it isn't dirty before updating it.
			// Otherwise, create a new working set with the incoming root hash.
			if hasWS {
				currWSHash, err := am.Get(ctx, workingSetPath)
				if err != nil {
					return prolly.AddressMap{}, err
				}

				targetCmt, err := db.ReadValue(ctx, currWSHash)
				if err != nil {
					return prolly.AddressMap{}, err
				}

				if sm, ok := targetCmt.(types.SerialMessage); ok {
					msg, err := serial.TryGetRootAsWorkingSet(sm, serial.MessagePrefixSz)
					if err != nil {
						return prolly.AddressMap{}, err
					}

					stagedHash := hash.New(msg.StagedRootAddrBytes())
					workingSetHash := hash.New(msg.WorkingRootAddrBytes())
					if !allowDirtyWorking && stagedHash != workingSetHash {
						return prolly.AddressMap{}, ErrDirtyWorkspace
					}

					targetHead, err := db.ReadValue(ctx, curr)
					if err != nil {
						return prolly.AddressMap{}, err
					}
					targetRootHash, err := GetCommitRootHash(targetHead)
					if err != nil {
						return prolly.AddressMap{}, err
					}

					if stagedHash != targetRootHash {
						return prolly.AddressMap{}, ErrDirtyWorkspace
					}

					// TODO - construct new meta instance rather than using the default
					updateWS := workingset_flatbuffer(cmtRtHsh, &cmtRtHsh, nil, nil, nil)
					ref, err := db.WriteValue(ctx, types.SerialMessage(updateWS))
					if err != nil {
						return prolly.AddressMap{}, err
					}
					newWSHash = ref.TargetHash()
				} else {
					// This _should_ never happen. We've already ended up on this code path because we are on
					// modern storage.
					return prolly.AddressMap{}, errors.New("Modern Dolt Database required.")
				}
			} else {
				updateWS := workingset_flatbuffer(cmtRtHsh, &cmtRtHsh, nil, nil, nil)
				ref, err := db.WriteValue(ctx, types.SerialMessage(updateWS))
				if err != nil {
					return prolly.AddressMap{}, err
				}
				newWSHash = ref.TargetHash()
			}
		}

		// This is the bit where we construct the new root. The Editor.Update call below will update the
		// branch reference directly. If we've been given a working set, we'll update the ID based on what was returned
		// calculated for the newWSHash.
		ae := am.Editor()
		err = ae.Update(ctx, ds.ID(), h)
		if err != nil {
			return prolly.AddressMap{}, err
		}

		if workingSetPath != "" && newWSHash != (hash.Hash{}) {
			err = ae.Update(ctx, workingSetPath, newWSHash)
			if err != nil {
				return prolly.AddressMap{}, err
			}
		}

		return ae.Flush(ctx)
	})

	if err == ErrAlreadyCommitted {
		return nil
	}

	return err
}

// BuildNewCommit creates a new commit for the dataset with the value and options given. An ordinary commit must
// have the current dataset head among its parents. An amend commit must name the current dataset head in
// [CommitOptions.AmendedCommit]. A force commit skips head validation.
func (db *database) BuildNewCommit(ctx context.Context, ds Dataset, v types.Value, opts CommitOptions) (*Commit, error) {
	if opts.Force && !opts.AmendedCommit.IsEmpty() {
		return nil, errors.New("datas: the Force and AmendedCommit commit options are mutually exclusive")
	}
	headAddr, hasHead := ds.MaybeHeadAddr()
	if !opts.AmendedCommit.IsEmpty() {
		if !hasHead {
			return nil, fmt.Errorf("cannot amend head of dataset '%s': dataset has no head: %w",
				ds.ID(), ErrMergeNeeded)
		}
		if headAddr != opts.AmendedCommit {
			return nil, fmt.Errorf("cannot amend head of dataset '%s': is at %s but expected %s: %w",
				ds.ID(), headAddr, opts.AmendedCommit, ErrMergeNeeded)
		}
	} else if hasHead && !opts.Force {
		if len(opts.Parents) == 0 {
			opts.Parents = []hash.Hash{headAddr}
		} else if !hasParentHash(opts, headAddr) {
			return nil, ErrMergeNeeded
		}
	}

	return newCommitForValue(ctx, ds.db.chunkStore(), ds.db, ds.db.nodeStore(), v, opts)
}

func (db *database) Commit(ctx context.Context, ds Dataset, v types.Value, opts CommitOptions) (Dataset, error) {
	commit, err := db.BuildNewCommit(ctx, ds, v, opts)
	if err != nil {
		return Dataset{}, err
	}
	return db.WriteCommit(ctx, ds, commit)
}

func (db *database) WriteCommit(ctx context.Context, ds Dataset, commit *Commit) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error {
		_, err := db.CommitDatasets(ctx, []DatasetUpdate{PrebuiltCommitUpdate{CommitDS: ds, Commit: commit}})
		return err
	})
}

// Calls db.Commit with empty CommitOptions{}.
func CommitValue(ctx context.Context, db Database, ds Dataset, v types.Value) (Dataset, error) {
	return db.Commit(ctx, ds, v, CommitOptions{Meta: &CommitMeta{}})
}

func mergeNeeded(currentAddr hash.Hash, ancestorAddr hash.Hash) bool {
	return currentAddr != ancestorAddr
}

func (db *database) Tag(ctx context.Context, ds Dataset, commitAddr hash.Hash, opts TagOptions) (Dataset, error) {
	return db.doHeadUpdate(
		ctx,
		ds,
		func(ds Dataset) error {
			addr, err := newTag(ctx, db, commitAddr, opts.Meta)
			if err != nil {
				return err
			}
			return db.doTag(ctx, ds.ID(), addr)
		},
	)
}

// doTag manages concurrent access the single logical piece of mutable state: the current Root. It uses
// the same optimistic writing algorithm as doCommit (see above).
func (db *database) doTag(ctx context.Context, datasetID string, tagAddr hash.Hash) error {
	_, err := db.update(ctx, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
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

	return err
}

func (db *database) SetTuple(ctx context.Context, ds Dataset, val []byte) (Dataset, error) {
	tupleAddr, _, err := newTuple(ctx, db, val)
	if err != nil {
		return Dataset{}, err
	}
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error {
		_, err = db.update(ctx, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
			ae := am.Editor()
			err := ae.Update(ctx, ds.ID(), tupleAddr)
			if err != nil {
				return prolly.AddressMap{}, err
			}
			return ae.Flush(ctx)
		})
		return err
	})
}

func (db *database) SetStatsRef(ctx context.Context, ds Dataset, mapAddr hash.Hash) (Dataset, error) {
	statAddr, _, err := newStat(ctx, db, mapAddr)
	if err != nil {
		return Dataset{}, err
	}
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error {
		_, err = db.update(ctx, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
			ae := am.Editor()
			err := ae.Update(ctx, ds.ID(), statAddr)
			if err != nil {
				return prolly.AddressMap{}, err
			}
			return ae.Flush(ctx)
		})
		return err
	})
}

// UpdateStashList updates the stash list dataset only with given address hash to the updated stash list.
// The new/updated stash list address should be obtained before calling this function depending on
// whether add or remove a stash actions have been performed. This function does not perform any actions
// on the stash list itself.
func (db *database) UpdateStashList(ctx context.Context, ds Dataset, stashListAddr hash.Hash) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error {
		// TODO: this function needs concurrency control for using stash in SQL context
		// this will update the dataset for stashes address map
		_, err := db.update(ctx, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
			ae := am.Editor()
			err := ae.Update(ctx, ds.ID(), stashListAddr)
			if err != nil {
				return prolly.AddressMap{}, err
			}
			return ae.Flush(ctx)
		})
		return err
	})
}

func (db *database) UpdateWorkingSet(ctx context.Context, ds Dataset, workingSetSpec WorkingSetSpec, prevHash hash.Hash) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error {
		_, err := db.CommitDatasets(ctx, []DatasetUpdate{WorkingSetUpdate{WorkingSetDS: ds.ID(), WorkingSet: workingSetSpec, PrevWsHash: prevHash}})
		return err
	})
}

func (db *database) PersistGhostCommitIDs(ctx context.Context, ghosts hash.HashSet) error {
	cs := db.ChunkStore()

	gcs, ok := cs.(chunks.GenerationalCS)
	if !ok {
		return errors.New("Generational Chunk Store expected. database does not support shallow clone instances.")
	}

	err := gcs.GhostGen().PersistGhostHashes(ctx, ghosts)

	return err
}

type DatasetUpdate interface {
	// DatasetID returns the ID of the dataset to update
	DatasetID() string
	// BuildCommitValue writes the value to write as a commit to the dataset and returns a ref to it
	BuildCommitValue(ctx context.Context, db *database) (hash.Hash, error)
	// LockDatasetID returns the ID of the dataset to check for optimistic locking. See |LockPrevHash|.
	LockDatasetID() string
	// LockPrevHash returns the expected hash of the current head of |LockDatasetId()|.
	// This value must be current for the update to succeed.
	LockPrevHash() hash.Hash
	// validateHead checks any additional constraints on the dataset being updated.
	validateHead(ctx context.Context, datasets prolly.AddressMap, newHead hash.Hash) error
}

// WorkingSetUpdate is a DatasetUpdate that updates a working set.
type WorkingSetUpdate struct {
	// WorkingSetDS is the working set Dataset to update with a new working set.
	WorkingSetDS string
	// WorkingSet is the new working set to write to the WorkingSetDS.
	WorkingSet WorkingSetSpec
	// PrevWsHash is the expected hash of the current working set for the WorkingSetDS.
	// If the current working set does not match this hash, the update will fail with ErrOptimisticLockFailed.
	PrevWsHash hash.Hash
}

var _ DatasetUpdate = &WorkingSetUpdate{}

func (w WorkingSetUpdate) DatasetID() string {
	return w.WorkingSetDS
}

func (w WorkingSetUpdate) BuildCommitValue(ctx context.Context, db *database) (hash.Hash, error) {
	return newWorkingSet(ctx, db, w.WorkingSet)
}

func (w WorkingSetUpdate) LockDatasetID() string {
	return w.WorkingSetDS
}

func (w WorkingSetUpdate) LockPrevHash() hash.Hash {
	return w.PrevWsHash
}

func (w WorkingSetUpdate) validateHead(context.Context, prolly.AddressMap, hash.Hash) error {
	return nil
}

type CommitUpdate struct {
	CommitDS     Dataset
	CommitOpts   CommitOptions
	WorkingSetDS string
	PrevWsHash   hash.Hash
	RootVal      types.Value
}

func (c CommitUpdate) DatasetID() string {
	return c.CommitDS.ID()
}

func (c CommitUpdate) BuildCommitValue(ctx context.Context, db *database) (hash.Hash, error) {
	// Prepend the current head hash to the list of parents if one was provided. This is only necessary if parents were
	// provided because we fill it in automatically in buildNewCommit otherwise.
	if len(c.CommitOpts.Parents) > 0 && c.CommitOpts.AmendedCommit.IsEmpty() && !c.CommitOpts.Force {
		headHash, ok := c.CommitDS.MaybeHeadAddr()
		if ok {
			if !hasParentHash(c.CommitOpts, headHash) {
				c.CommitOpts.Parents = append([]hash.Hash{headHash}, c.CommitOpts.Parents...)
			}
		}
	}

	commit, err := db.BuildNewCommit(ctx, c.CommitDS, c.RootVal, c.CommitOpts)
	if err != nil {
		return hash.Hash{}, err
	}

	commitRef, err := db.WriteValue(ctx, commit.NomsValue())
	if err != nil {
		return hash.Hash{}, err
	}

	return commitRef.TargetHash(), nil
}

func (c CommitUpdate) LockDatasetID() string {
	return c.WorkingSetDS
}

func (c CommitUpdate) LockPrevHash() hash.Hash {
	return c.PrevWsHash
}

func (c CommitUpdate) validateHead(ctx context.Context, datasets prolly.AddressMap, newHead hash.Hash) error {
	current, err := datasets.Get(ctx, c.DatasetID())
	if err != nil {
		return err
	}
	expected, _ := c.CommitDS.MaybeHeadAddr()
	if current != expected {
		return ErrMergeNeeded
	}

	return nil
}

var _ DatasetUpdate = &CommitUpdate{}

// PrebuiltCommitUpdate publishes an already constructed commit without changing its parents.
type PrebuiltCommitUpdate struct {
	CommitDS Dataset
	Commit   *Commit
}

var _ DatasetUpdate = PrebuiltCommitUpdate{}

func (c PrebuiltCommitUpdate) DatasetID() string       { return c.CommitDS.ID() }
func (c PrebuiltCommitUpdate) LockDatasetID() string   { return "" }
func (c PrebuiltCommitUpdate) LockPrevHash() hash.Hash { return hash.Hash{} }

func (c PrebuiltCommitUpdate) BuildCommitValue(ctx context.Context, db *database) (hash.Hash, error) {
	r, err := db.WriteValue(ctx, c.Commit.NomsValue())
	if err != nil {
		return hash.Hash{}, err
	}
	return r.TargetHash(), nil
}

func (c PrebuiltCommitUpdate) validateHead(ctx context.Context, datasets prolly.AddressMap, newHead hash.Hash) error {
	current, err := datasets.Get(ctx, c.DatasetID())
	if err != nil {
		return err
	}
	expected, _ := c.CommitDS.MaybeHeadAddr()
	if current != expected {
		return ErrMergeNeeded
	}
	if !current.IsEmpty() && current == newHead {
		return ErrAlreadyCommitted
	}
	return nil
}

// CommitDatasets updates the given Datasets atomically.
func (db *database) CommitDatasets(
	ctx context.Context,
	atomicCommit []DatasetUpdate,
) ([]Dataset, error) {
	if len(atomicCommit) == 0 {
		return []Dataset{}, nil
	}
	pending := make([]hash.Hash, len(atomicCommit))
	seen := make(map[string]struct{}, len(atomicCommit))

	for i, cmt := range atomicCommit {
		if err := ValidateDatasetId(cmt.DatasetID()); err != nil {
			return nil, err
		}
		if _, ok := seen[cmt.DatasetID()]; ok {
			return nil, fmt.Errorf("duplicate dataset update: %s", cmt.DatasetID())
		}
		seen[cmt.DatasetID()] = struct{}{}
		newRefHash, err := cmt.BuildCommitValue(ctx, db)
		if err != nil {
			return nil, err
		}
		pending[i] = newRefHash
	}

	currentDatasets, err := db.update(ctx, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
		ae := am.Editor()

		for i := range atomicCommit {
			if lockID := atomicCommit[i].LockDatasetID(); lockID != "" {
				currHash, err := am.Get(ctx, lockID)
				if err != nil {
					return prolly.AddressMap{}, err
				}
				if currHash != atomicCommit[i].LockPrevHash() {
					return prolly.AddressMap{}, ErrOptimisticLockFailed
				}
			}

			if err := atomicCommit[i].validateHead(ctx, am, pending[i]); err != nil {
				return prolly.AddressMap{}, err
			}

			err := ae.Update(ctx, atomicCommit[i].DatasetID(), pending[i])
			if err != nil {
				return prolly.AddressMap{}, err
			}
		}

		return ae.Flush(ctx)
	})

	if err != nil {
		return nil, err
	}

	updatedDatasets := make([]Dataset, len(atomicCommit))
	dsMap := DatasetsMap(refmapDatasetsMap{currentDatasets})
	for i := range atomicCommit {
		updatedDS, err := db.datasetFromMap(ctx, atomicCommit[i].DatasetID(), dsMap)
		if err != nil {
			return nil, err
		}
		updatedDatasets[i] = updatedDS
	}

	return updatedDatasets, nil
}

func (db *database) Delete(ctx context.Context, ds Dataset, wsIDStr string) (Dataset, error) {
	return db.doHeadUpdate(ctx, ds, func(ds Dataset) error { return db.doDelete(ctx, ds.ID(), wsIDStr) })
}

func (db *database) update(
	ctx context.Context,
	editFB func(context.Context, prolly.AddressMap) (prolly.AddressMap, error),
) (prolly.AddressMap, error) {
	var (
		err  error
		root hash.Hash
	)

	for {
		root, err = db.rt.Root(ctx)
		if err != nil {
			return prolly.AddressMap{}, err
		}

		var newRootHash hash.Hash

		datasets, err := db.loadDatasetsRefmap(ctx, root)
		if err != nil {
			return prolly.AddressMap{}, err
		}

		datasets, err = editFB(ctx, datasets)
		if err != nil {
			return prolly.AddressMap{}, err
		}

		data := storeroot_flatbuffer(datasets)
		r, err := db.WriteValue(ctx, types.SerialMessage(data))
		if err != nil {
			return prolly.AddressMap{}, err
		}

		newRootHash = r.TargetHash()

		err = db.tryCommitChunks(ctx, newRootHash, root)
		if err == ErrOptimisticLockFailed {
			continue
		}

		return datasets, err
	}
}

func (db *database) doDelete(ctx context.Context, datasetIDstr string, workingsetIDstr string) error {
	var firstHash hash.Hash

	_, err := db.update(ctx, func(ctx context.Context, am prolly.AddressMap) (prolly.AddressMap, error) {
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

		if workingsetIDstr != "" {
			// We verify that the working set is clean before deleting the branch. If this block doesn't return,
			// the implication that it's safe to delete the branch ref and working set.
			hasWs, err := am.Has(ctx, workingsetIDstr)
			if err != nil {
				return prolly.AddressMap{}, err
			}

			if hasWs {
				currWSHash, err := am.Get(ctx, workingsetIDstr)
				if err != nil {
					return prolly.AddressMap{}, err
				}
				targetCmt, err := db.ReadValue(ctx, currWSHash)
				if err != nil {
					return prolly.AddressMap{}, err
				}

				if sm, ok := targetCmt.(types.SerialMessage); ok {

					msg, err := serial.TryGetRootAsWorkingSet(sm, serial.MessagePrefixSz)
					if err != nil {
						return prolly.AddressMap{}, err
					}

					stagedHash := hash.New(msg.StagedRootAddrBytes())
					workingSetHash := hash.New(msg.WorkingRootAddrBytes())
					if stagedHash != workingSetHash {
						return prolly.AddressMap{}, ErrDirtyWorkspace
					}

					targetHead, err := db.ReadValue(ctx, curr)
					if err != nil {
						return prolly.AddressMap{}, err
					}
					targetRootHash, err := GetCommitRootHash(targetHead)
					if err != nil {
						return prolly.AddressMap{}, err
					}

					if stagedHash != targetRootHash {
						return prolly.AddressMap{}, ErrDirtyWorkspace
					}

					// No reason found to prevent deletion. Continue.
				} else {
					// This _should_ never happen. We've already ended up on this code path because we are on
					// modern storage.
					return prolly.AddressMap{}, errors.New("Modern Dolt Database required.")
				}
			}
		}

		ae := am.Editor()
		err = ae.Delete(ctx, datasetIDstr)
		if err != nil {
			return prolly.AddressMap{}, err
		}
		if workingsetIDstr != "" {
			err = ae.Delete(ctx, workingsetIDstr)
			if err != nil {
				return prolly.AddressMap{}, err
			}
		}

		return ae.Flush(ctx)
	})

	return err
}

// GC traverses the database starting at the Root and removes all unreferenced data from persistent storage.
func (db *database) GC(ctx context.Context, gcConfig chunks.GCConfig, oldGenRefs, newGenRefs hash.HashSet, safepointController types.GCSafepointController) error {
	return db.ValueStore.GC(ctx, gcConfig, oldGenRefs, newGenRefs, safepointController)
}

func (db *database) tryCommitChunks(ctx context.Context, newRootHash hash.Hash, currentRootHash hash.Hash) error {
	if success, err := db.rt.Commit(ctx, newRootHash, currentRootHash); err != nil {
		return err
	} else if !success {
		return ErrOptimisticLockFailed
	}
	return nil
}

func hasParentHash(opts CommitOptions, curr hash.Hash) bool {
	found := false
	for _, h := range opts.Parents {
		if h == curr {
			found = true
			break
		}
	}
	return found
}

func (db *database) doHeadUpdate(ctx context.Context, ds Dataset, updateFunc func(ds Dataset) error) (Dataset, error) {
	err := updateFunc(ds)
	if err != nil {
		return Dataset{}, err
	}

	return db.GetDataset(ctx, ds.ID())
}
