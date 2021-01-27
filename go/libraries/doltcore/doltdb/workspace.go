// Copyright 2020 Dolthub, Inc.
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

type Workspace struct {
	Name   string
	vrw    types.ValueReadWriter
	workSt types.Struct
	Commit *Commit
}

// NewWorkspace creates a new Workspace object.
func NewWorkspace(ctx context.Context, name string, vrw types.ValueReadWriter, workSt types.Struct) (*Workspace, error) {
	commitRef, ok, err := workSt.MaybeGet(datas.TagCommitRefField)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("tag struct does not have field %s", datas.TagCommitRefField)
	}

	commitSt, err := commitRef.(types.Ref).TargetValue(ctx, vrw)
	if err != nil {
		return nil, err
	}

	commit := NewCommit(vrw, commitSt.(types.Struct))

	return &Workspace{
		Name:   name,
		vrw:    vrw,
		workSt: workSt,
		Commit: commit,
	}, nil
}

// GetStRef returns a Noms Ref for this Workspace's Noms tag Struct.
func (t *Workspace) GetStRef() (types.Ref, error) {
	return types.NewRef(t.workSt, t.vrw.Format())
}

// GetDoltRef returns a DoltRef for this Workspace.
func (t *Workspace) GetDoltRef() ref.DoltRef {
	return ref.NewWorkspaceRef(t.Name)
}
