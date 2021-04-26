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

package datas

import (
	"context"

	"github.com/dolthub/dolt/go/store/nomdl"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	WorkspaceMetaField = "meta"
	WorkingSetRefField = "ref"
	WorkingSetName     = "WorkingSet"
)

type WorkingSetMeta struct {
	Meta types.Struct
}

var workingSetTemplate = types.MakeStructTemplate(WorkingSetName, []string{WorkingSetRefField})

// ref is a Ref<Value>, any Value
var valueWorkingSetType = nomdl.MustParseType(`Struct WorkingSet {
        ref:  Ref<Value>,
}`)

// NewWorkingSet creates a new working set object.
// A working set is a value that has been persisted but is not necessarily referenced by a Commit. As the name implies,
// it's storage for data changes that have not yet been incorporated into the commit graph but need durable storage.
//
// A working set struct has the following type:
//
// ```
// struct WorkingSet {
//   meta: M,
//   ref: R,
// }
// ```
// where M is a struct type and R is a ref type.
func NewWorkingSet(_ context.Context, valueRef types.Ref) (types.Struct, error) {
	return workingSetTemplate.NewStruct(valueRef.Format(), []types.Value{valueRef})
}

func IsWorkingSet(v types.Value) (bool, error) {
	if s, ok := v.(types.Struct); !ok {
		return false, nil
	} else {
		return types.IsValueSubtypeOf(s.Format(), v, valueWorkingSetType)
	}
}