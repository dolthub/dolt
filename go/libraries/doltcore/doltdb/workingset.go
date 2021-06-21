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

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	workingMetaStName        = "workingset"
	workingMetaVersionStName = "version"
	workingMetaVersion       = "1.0"
)

type MergeState struct {
	valueSt         types.Struct
	commit          *Commit
	preMergeWorking *RootValue
}

func (m MergeState) Commit() *Commit {
	return m.commit
}

func (m MergeState) PreMergeWorkingRoot() *RootValue {
	return m.preMergeWorking
}

type WorkingSet struct {
	Name        string
	format      *types.NomsBinFormat
	st          types.Struct
	workingRoot *RootValue
	stagedRoot  *RootValue
	mergeState  *MergeState
}

func EmptyWorkingSet(wsRef ref.WorkingSetRef) *WorkingSet {
	return &WorkingSet{
		Name: wsRef.GetPath(),
		format:      types.Format_Default,
		st:          types.Struct{},
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

func (ws *WorkingSet) WorkingRoot() *RootValue {
	return ws.workingRoot
}

func (ws *WorkingSet) StagedRoot() *RootValue {
	return ws.stagedRoot
}

func (ws *WorkingSet) MergeState() *MergeState {
	return ws.mergeState
}

// NewWorkingSet creates a new WorkingSet object.
func NewWorkingSet(ctx context.Context, name string, vrw types.ValueReadWriter, workingSetSt types.Struct) (*WorkingSet, error) {
	// TODO: meta struct
	// metaSt, ok, err := workingSetSt.MaybeGet(datas.TagMetaField)
	//
	// if err != nil {
	// 	return nil, err
	// }
	// if !ok {
	// 	return nil, fmt.Errorf("tag struct does not have field %s", datas.TagMetaField)
	// }
	//
	// meta, err := tagMetaFromNomsSt(metaSt.(types.Struct))
	//
	// if err != nil {
	// 	return nil, err
	// }

	workingRootRef, ok, err := workingSetSt.MaybeGet(datas.WorkingRootRefField)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("workingset struct does not have field %s", datas.WorkingRootRefField)
	}

	workingRootValSt, err := workingRootRef.(types.Ref).TargetValue(ctx, vrw)
	if err != nil {
		return nil, err
	}

	workingRoot, err := newRootValue(vrw, workingRootValSt.(types.Struct))
	if err != nil {
		return nil, err
	}

	stagedRootRef, ok, err := workingSetSt.MaybeGet(datas.StagedRootRefField)
	if err != nil {
		return nil, err
	}

	var stagedRoot *RootValue
	if ok {
		stagedRootValSt, err := stagedRootRef.(types.Ref).TargetValue(ctx, vrw)
		if err != nil {
			return nil, err
		}

		stagedRoot, err = newRootValue(vrw, stagedRootValSt.(types.Struct))
		if err != nil {
			return nil, err
		}
	}

	// TODO: merge state

	return &WorkingSet{
		Name:        name,
		format:      vrw.Format(),
		st:          workingSetSt,
		workingRoot: workingRoot,
		stagedRoot:  stagedRoot,
	}, nil
}

// RootValue returns the root value stored by this workingset
func (ws *WorkingSet) RootValue() *RootValue {
	return ws.workingRoot
}

// HashOf returns the hash of the workingset struct, which is not the same as the hash of the root value stored in the
// working set. This value is used for optimistic locking when updating a working set for a head ref.
func (ws *WorkingSet) HashOf() (hash.Hash, error) {
	return ws.st.Hash(ws.format)
}

// Ref returns a WorkingSetRef for this WorkingSet.
func (ws *WorkingSet) Ref() ref.WorkingSetRef {
	return ref.NewWorkingSetRef(ws.Name)
}

// writeValues write the values in this working set to the database and returns them
func (ws *WorkingSet) writeValues(ctx context.Context, db *DoltDB) (
		workingRoot types.Ref,
		stagedRoot *types.Ref,
		mergeState *types.Ref,
		err error,
){

	workingRoot, err = db.writeRootValue(ctx, ws.workingRoot)
	if err != nil {
		return types.Ref{}, nil, nil, err
	}

	if ws.stagedRoot != nil {
		var stagedRootRef types.Ref
		stagedRootRef, err = db.writeRootValue(ctx, ws.stagedRoot)
		if err != nil {
			return types.Ref{}, nil, nil, err
		}
		stagedRoot = &stagedRootRef
	}

	if ws.mergeState != nil {
		// TODO: write nested merge state refs here?
		var mergeStateRef types.Ref
		mergeStateRef, err = db.writeRootValue(ctx, ws.stagedRoot)
		if err != nil {
			return types.Ref{}, nil, nil, err
		}
		mergeState = &mergeStateRef
	}

	return workingRoot, stagedRoot, mergeState, nil
}

// WorkingSetMeta contains all the metadata that is associated with a working set
type WorkingSetMeta struct {
	// empty for now
}

func NewWorkingSetMeta() *WorkingSetMeta {
	return &WorkingSetMeta{}
}

func (tm *WorkingSetMeta) toNomsStruct(nbf *types.NomsBinFormat) (types.Struct, error) {
	metadata := types.StructData{
		workingMetaVersionStName: types.String(workingMetaVersion),
	}

	return types.NewStruct(nbf, workingMetaStName, metadata)
}
