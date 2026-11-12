// Copyright 2026 Dolthub, Inc.
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
	"errors"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

// RebuildVirtualMergeBase returns the virtual merge base built from |mergeBases|. It is set by the merge package,
// which this package cannot import.
var RebuildVirtualMergeBase func(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, mergeBases []hash.Hash) (*Commit, error)

// RebuildRekeyedMergeBase returns the root of the rekeyed merge base of a conflict with |meta| from |theirRootIsh|.
// It is set by the merge package, which this package cannot import.
var RebuildRekeyedMergeBase func(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, meta prolly.ConflictMetadata, theirRootIsh hash.Hash) (RootValue, error)

// LoadConflictBaseRoot returns the root holding the base values of a conflict with |meta| from |theirRootIsh|. A
// virtual or rekeyed merge base that has been garbage collected is rebuilt from what |meta| records. Histories with
// no common ancestor merge against the virtual merge base of no merge bases, which is rebuilt when |meta| names it.
func LoadConflictBaseRoot(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, meta prolly.ConflictMetadata, theirRootIsh hash.Hash) (RootValue, error) {
	v, err := vrw.ReadValue(ctx, meta.BaseRootIsh)
	if err != nil {
		return nil, err
	}
	if v == nil {
		if !meta.RekeyedFrom.IsEmpty() {
			return RebuildRekeyedMergeBase(ctx, vrw, ns, meta, theirRootIsh)
		}
		rebuilt, err := RebuildVirtualMergeBase(ctx, vrw, ns, meta.MergeBases)
		if err != nil {
			return nil, err
		}
		rebuiltAddr, err := rebuilt.HashOf()
		if err != nil {
			return nil, err
		}
		if len(meta.MergeBases) > 0 || rebuiltAddr == meta.BaseRootIsh {
			return rebuilt.GetRootValue(ctx)
		}
	}
	return LoadRootValueFromRootIshAddr(ctx, vrw, ns, meta.BaseRootIsh)
}

// NewDanglingCommit writes |root| and a commit of it with |parents|, which may be empty, to |root|'s store. No ref
// points to the commit.
func NewDanglingCommit(ctx context.Context, root RootValue, parents []*Commit, meta *datas.CommitMeta) (*Commit, error) {
	vrw, ns := root.VRW(), root.NodeStore()
	store, ok := vrw.(interface{ ChunkStore() chunks.ChunkStore })
	if !ok {
		return nil, errors.New("cannot write a dangling commit: value store does not expose its chunk store")
	}

	root, err := root.SetFeatureVersion(DoltFeatureVersion)
	if err != nil {
		return nil, err
	}
	if _, err = vrw.WriteValue(ctx, root.NomsValue()); err != nil {
		return nil, err
	}

	parentAddrs := make([]hash.Hash, len(parents))
	for i, p := range parents {
		if parentAddrs[i], err = p.HashOf(); err != nil {
			return nil, err
		}
	}
	dc, err := datas.NewCommitForValue(ctx, store.ChunkStore(), vrw, ns, root.NomsValue(), datas.CommitOptions{Parents: parentAddrs, Meta: meta})
	if err != nil {
		return nil, err
	}
	if _, err = vrw.WriteValue(ctx, dc.NomsValue()); err != nil {
		return nil, err
	}
	return NewCommit(ctx, vrw, ns, dc)
}
