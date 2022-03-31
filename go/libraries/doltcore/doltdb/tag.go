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

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type Tag struct {
	Name   string
	vrw    types.ValueReadWriter
	addr   hash.Hash
	Meta   *datas.TagMeta
	Commit *Commit
}

// NewTag creates a new Tag object.
func NewTag(ctx context.Context, name string, ds datas.Dataset, vrw types.ValueReadWriter) (*Tag, error) {
	meta, commitAddr, err := ds.HeadTag()
	if err != nil {
		return nil, err
	}
	commitSt, err := vrw.ReadValue(ctx, commitAddr)
	if err != nil {
		return nil, err
	}
	// TODO: tomfoolery.
	ref, err := types.NewRef(commitSt, vrw.Format())
	if err != nil {
		return nil, err
	}
	dc, err := datas.LoadCommitRef(ctx, vrw, ref)
	if err != nil {
		return nil, err
	}
	commit, err := NewCommit(ctx, vrw, dc)
	if err != nil {
		return nil, err
	}

	addr, _ := ds.MaybeHeadAddr()

	return &Tag{
		Name:   name,
		vrw:    vrw,
		addr:   addr,
		Meta:   meta,
		Commit: commit,
	}, nil
}

// GetAddr returns a content address hash for this Tag.
func (t *Tag) GetAddr() (hash.Hash, error) {
	return t.addr, nil
}

// GetDoltRef returns a DoltRef for this Tag.
func (t *Tag) GetDoltRef() ref.DoltRef {
	return ref.NewTagRef(t.Name)
}
