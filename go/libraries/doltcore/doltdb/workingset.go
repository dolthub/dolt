// Copyright 2021 Dolthub, Inc.
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

package doltdb

import (
	"context"
	"fmt"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type MergeState struct {
	commit          *Commit
	preMergeWorking *RootValue
}

// TodoWorkingSetMeta returns an incomplete WorkingSetMeta, suitable for methods that don't have the means to construct
// a real one. These should be considered temporary and cleaned up when possible, similar to Context.TODO
func TodoWorkingSetMeta() *datas.WorkingSetMeta {
	return &datas.WorkingSetMeta{
		Name:        "TODO",
		Email:       "TODO",
		Timestamp:   uint64(time.Now().Unix()),
		Description: "TODO",
	}
}

// MergeStateFromCommitAndWorking returns a new MergeState.
// Most clients should not construct MergeState objects directly, but instead use WorkingSet.StartMerge
func MergeStateFromCommitAndWorking(commit *Commit, preMergeWorking *RootValue) *MergeState {
	return &MergeState{commit: commit, preMergeWorking: preMergeWorking}
}

func newMergeState(ctx context.Context, vrw types.ValueReadWriter, mergeState types.Struct) (*MergeState, error) {
	commitSt, ok, err := mergeState.MaybeGet(datas.MergeStateCommitField)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("corrupted MergeState struct")
	}

	dc, err := datas.CommitFromValue(vrw.Format(), commitSt)
	if err != nil {
		return nil, err
	}

	commit, err := NewCommit(ctx, vrw, dc)
	if err != nil {
		return nil, err
	}

	workingRootRef, ok, err := mergeState.MaybeGet(datas.MergeStateWorkingPreMergeField)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("corrupted MergeState struct")
	}

	workingRootValSt, err := workingRootRef.(types.Ref).TargetValue(ctx, vrw)
	if err != nil {
		return nil, err
	}

	workingRoot, err := newRootValue(vrw, workingRootValSt)
	if err != nil {
		return nil, err
	}

	return &MergeState{
		commit:          commit,
		preMergeWorking: workingRoot,
	}, nil
}

func (m MergeState) Commit() *Commit {
	return m.commit
}

func (m MergeState) PreMergeWorkingRoot() *RootValue {
	return m.preMergeWorking
}

type WorkingSet struct {
	Name        string
	meta        *datas.WorkingSetMeta
	addr        *hash.Hash
	workingRoot *RootValue
	stagedRoot  *RootValue
	mergeState  *MergeState
}

// TODO: remove this, require working and staged
func EmptyWorkingSet(wsRef ref.WorkingSetRef) *WorkingSet {
	return &WorkingSet{
		Name: wsRef.GetPath(),
	}
}

func (ws WorkingSet) WithStagedRoot(stagedRoot *RootValue) *WorkingSet {
	ws.stagedRoot = stagedRoot
	return &ws
}

func (ws WorkingSet) WithWorkingRoot(workingRoot *RootValue) *WorkingSet {
	ws.workingRoot = workingRoot
	return &ws
}

func (ws WorkingSet) WithMergeState(mergeState *MergeState) *WorkingSet {
	ws.mergeState = mergeState
	return &ws
}

func (ws WorkingSet) StartMerge(commit *Commit) *WorkingSet {
	ws.mergeState = &MergeState{
		commit:          commit,
		preMergeWorking: ws.workingRoot,
	}

	return &ws
}

func (ws WorkingSet) AbortMerge() *WorkingSet {
	ws.workingRoot = ws.mergeState.PreMergeWorkingRoot()
	ws.stagedRoot = ws.workingRoot
	ws.mergeState = nil
	return &ws
}

func (ws WorkingSet) ClearMerge() *WorkingSet {
	ws.mergeState = nil
	return &ws
}

func (ws *WorkingSet) WorkingRoot() *RootValue {
	return ws.workingRoot
}

func (ws *WorkingSet) StagedRoot() *RootValue {
	return ws.stagedRoot
}

func (ws *WorkingSet) MergeState() *MergeState {
	return ws.mergeState
}

func (ws *WorkingSet) MergeActive() bool {
	return ws.mergeState != nil
}

func (ws WorkingSet) Meta() *datas.WorkingSetMeta {
	return ws.meta
}

// NewWorkingSet creates a new WorkingSet object.
func NewWorkingSet(ctx context.Context, name string, vrw types.ValueReadWriter, ds datas.Dataset) (*WorkingSet, error) {
	dsws, err := ds.HeadWorkingSet()
	if err != nil {
		return nil, err
	}

	meta := dsws.Meta
	if meta == nil {
		meta = &datas.WorkingSetMeta{
			Name:        "not present",
			Email:       "not present",
			Timestamp:   0,
			Description: "not present",
		}
	}

	workingRootVal, err := vrw.ReadValue(ctx, dsws.WorkingAddr)
	if err != nil {
		return nil, err
	}
	workingRoot, err := newRootValue(vrw, workingRootVal)
	if err != nil {
		return nil, err
	}

	var stagedRoot *RootValue
	if dsws.StagedAddr != nil {
		stagedRootVal, err := vrw.ReadValue(ctx, *dsws.StagedAddr)
		if err != nil {
			return nil, err
		}

		stagedRoot, err = newRootValue(vrw, stagedRootVal)
		if err != nil {
			return nil, err
		}
	}

	var mergeState *MergeState
	if dsws.MergeStateAddr != nil {
		mergeStateValSt, err := vrw.ReadValue(ctx, *dsws.MergeStateAddr)
		if err != nil {
			return nil, err
		}

		mergeState, err = newMergeState(ctx, vrw, mergeStateValSt.(types.Struct))
		if err != nil {
			return nil, err
		}
	}

	addr, _ := ds.MaybeHeadAddr()

	return &WorkingSet{
		Name:        name,
		meta:        meta,
		addr:        &addr,
		workingRoot: workingRoot,
		stagedRoot:  stagedRoot,
		mergeState:  mergeState,
	}, nil
}

// HashOf returns the hash of the workingset struct, which is not the same as the hash of the root value stored in the
// working set. This value is used for optimistic locking when updating a working set for a head ref.
func (ws *WorkingSet) HashOf() (hash.Hash, error) {
	if ws == nil || ws.addr == nil {
		return hash.Hash{}, nil
	}
	return *ws.addr, nil
}

// Ref returns a WorkingSetRef for this WorkingSet.
func (ws *WorkingSet) Ref() ref.WorkingSetRef {
	return ref.NewWorkingSetRef(ws.Name)
}

// writeValues write the values in this working set to the database and returns them
func (ws *WorkingSet) writeValues(ctx context.Context, db *DoltDB) (
	workingRoot types.Ref,
	stagedRoot types.Ref,
	mergeState *types.Ref,
	err error,
) {

	if ws.stagedRoot == nil || ws.workingRoot == nil {
		return types.Ref{}, types.Ref{}, nil, fmt.Errorf("StagedRoot and workingRoot must be set. This is a bug.")
	}

	workingRoot, err = db.writeRootValue(ctx, ws.workingRoot)
	if err != nil {
		return types.Ref{}, types.Ref{}, nil, err
	}

	stagedRoot, err = db.writeRootValue(ctx, ws.stagedRoot)
	if err != nil {
		return types.Ref{}, types.Ref{}, nil, err
	}

	if ws.mergeState != nil {
		var mergeStateRef types.Ref
		preMergeWorking, err := db.writeRootValue(ctx, ws.mergeState.preMergeWorking)
		if err != nil {
			return types.Ref{}, types.Ref{}, nil, err
		}

		commitH, err := ws.mergeState.commit.HashOf()
		if err != nil {
			return types.Ref{}, types.Ref{}, nil, err
		}
		commitV, err := db.vrw.ReadValue(ctx, commitH)
		if err != nil {
			return types.Ref{}, types.Ref{}, nil, err
		}

		mergeStateRefSt, err := datas.NewMergeState(ctx, preMergeWorking, commitV)
		if err != nil {
			return types.Ref{}, types.Ref{}, nil, err
		}

		mergeStateRef, err = db.vrw.WriteValue(ctx, mergeStateRefSt)
		if err != nil {
			return types.Ref{}, types.Ref{}, nil, err
		}

		mergeState = &mergeStateRef
	}

	return workingRoot, stagedRoot, mergeState, nil
}
