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
	"time"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
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
func NewTag(ctx context.Context, name string, ds datas.Dataset, vrw types.ValueReadWriter, ns tree.NodeStore) (*Tag, error) {
	startHT := time.Now()
	meta, commitAddr, err := ds.HeadTag()
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(color.Output, "DUSTIN: NewTag: head tag: success: elapsed: %v\n", time.Since(startHT))

	startLoad := time.Now()
	dc, err := datas.LoadCommitAddr(ctx, vrw, commitAddr)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(color.Output, "DUSTIN: NewTag: load addr: success: elapsed: %v\n", time.Since(startLoad))

	if dc.IsGhost() {
		return nil, ErrGhostCommitEncountered
	}

	startCommit := time.Now()
	commit, err := NewCommit(ctx, vrw, ns, dc)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(color.Output, "DUSTIN: NewTag: startCommit: success: elapsed: %v\n", time.Since(startCommit))

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
