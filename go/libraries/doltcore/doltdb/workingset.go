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
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	workingMetaStName  = "workingset"
	workingMetaVersion = "1.0"
)

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
		workingMetaVersion:   types.String(workingMetaVersion),
	}

	return types.NewStruct(nbf, workingMetaStName, metadata)
}