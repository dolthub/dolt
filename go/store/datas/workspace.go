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
	WorkspaceRefField  = "ref"
	WorkspaceName      = "Workspace"
)

type WorkspaceMeta struct {
	Meta types.Struct
}

var workspaceTemplate = types.MakeStructTemplate(WorkspaceName, []string{WorkspaceMetaField, WorkspaceRefField})

// ref is a Ref<Value>, any Value (not necessarily a commit)
var workspaceTagType = nomdl.MustParseType(`Struct Workspace {
        meta: Struct {},
        ref:  Ref<Value>,
}`)

// NewWorkspace creates a new workspace object.
//
// A workspace has the following type:
//
// ```
// struct Workspace {
//   meta: M,
//   ref: R,
// }
// ```
// where M is a struct type and R is a ref type.
func NewWorkspace(_ context.Context, valueRef types.Ref, meta types.Struct) (types.Struct, error) {
	return workspaceTemplate.NewStruct(meta.Format(), []types.Value{meta, valueRef})
}

func IsWorkspace(v types.Value) (bool, error) {
	if s, ok := v.(types.Struct); !ok {
		return false, nil
	} else {
		return types.IsValueSubtypeOf(s.Format(), v, workspaceTagType)
	}
}