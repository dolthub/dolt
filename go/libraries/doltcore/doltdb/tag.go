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

type Tag struct {
	Name   string
	vrw    types.ValueReadWriter
	tagSt  types.Struct
	Meta   *datas.TagMeta
	Commit *Commit
}

// NewTag creates a new Tag object.
func NewTag(ctx context.Context, name string, vrw types.ValueReadWriter, tagSt types.Struct) (*Tag, error) {
	metaSt, ok, err := tagSt.MaybeGet(datas.TagMetaField)

	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("tag struct does not have field %s", datas.TagMetaField)
	}

	meta, err := datas.TagMetaFromNomsSt(metaSt.(types.Struct))

	if err != nil {
		return nil, err
	}

	commitRef, ok, err := tagSt.MaybeGet(datas.TagCommitRefField)

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

	return &Tag{
		Name:   name,
		vrw:    vrw,
		tagSt:  tagSt,
		Meta:   meta,
		Commit: commit,
	}, nil
}

// GetStRef returns a Noms Ref for this Tag's Noms tag Struct.
func (t *Tag) GetStRef() (types.Ref, error) {
	return types.NewRef(t.tagSt, t.vrw.Format())
}

// GetDoltRef returns a DoltRef for this Tag.
func (t *Tag) GetDoltRef() ref.DoltRef {
	return ref.NewTagRef(t.Name)
}
