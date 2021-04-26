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
	"github.com/dolthub/dolt/go/store/types"
)

const (
	workingMetaStName  = "workingset"
	workingMetaVersionStName = "version"
	workingMetaVersion = "1.0"
)

type WorkingSet struct {
	Name      string
	format    *types.NomsBinFormat
	st        types.Struct
	rootValue *RootValue
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

	rootRef, ok, err := workingSetSt.MaybeGet(datas.WorkingSetRefField)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("workingset struct does not have field %s", datas.WorkingSetRefField)
	}

	rootValSt, err := rootRef.(types.Ref).TargetValue(ctx, vrw)
	if err != nil {
		return nil, err
	}

	rootVal, err := newRootValue(vrw, rootValSt.(types.Struct))
	if err != nil {
		return nil, err
	}

	return &WorkingSet{
		Name:      name,
		format:    vrw.Format(),
		st:        workingSetSt,
		rootValue: rootVal,
	}, nil
}

// RootValue returns the root value stored by this workingset
func (t *WorkingSet) RootValue() *RootValue {
	return t.rootValue
}

// Struct returns the struct used to construct this WorkingSet.
func (t *WorkingSet) Struct() types.Struct {
	return t.st
}

// DoltRef returns a DoltRef for this WorkingSet.
func (t *WorkingSet) DoltRef() ref.DoltRef {
	return ref.NewWorkingSetRef(t.Name)
}

// IsWorkingSetRef returns whether the given ref is a valid workingset ref
func IsWorkingSetRef(dref ref.DoltRef) bool {
	path := dref.GetPath()
	return dref.GetType() == ref.WorkingSetRefType &&
		ref.IsValidBranchName(path) // same naming rules as branches
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
		workingMetaVersionStName:   types.String(workingMetaVersion),
	}

	return types.NewStruct(nbf, workingMetaStName, metadata)
}