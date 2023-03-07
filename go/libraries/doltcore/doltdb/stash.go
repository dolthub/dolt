// Copyright 2023 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type Stash struct {
	Name        string
	BranchName  string
	Description string
	HeadCommit  *Commit
}

// getStashList returns array of Stash objects containing all stash entries in the stash list map.
func getStashList(ctx context.Context, ds datas.Dataset, vrw types.ValueReadWriter, ns tree.NodeStore) ([]*Stash, error) {
	v, ok := ds.MaybeHead()
	if !ok {
		return nil, errors.New("stashes not found")
	}

	stashHashes, err := datas.GetHashListFromStashList(ctx, ns, v)
	if err != nil {
		return nil, err
	}

	var sl = make([]*Stash, len(stashHashes))
	for i, stashHash := range stashHashes {
		var s Stash
		s.Name = fmt.Sprintf("stash@{%v}", i)
		stashVal, err := vrw.ReadValue(ctx, stashHash)
		if err != nil {
			return nil, err
		}

		_, headCommitAddr, meta, err := datas.GetStashData(stashVal)
		if err != nil {
			return nil, err
		}

		hc, err := datas.LoadCommitAddr(ctx, vrw, headCommitAddr)
		if err != nil {
			return nil, err
		}
		headCommit, err := NewCommit(ctx, vrw, ns, hc)
		if err != nil {
			return nil, err
		}

		s.HeadCommit = headCommit
		s.BranchName = meta.BranchName
		s.Description = meta.Description

		sl[i] = &s
	}

	return sl, nil
}

// getStashList returns hash address only of the stash at given index.
func getStashHashAtIdx(ctx context.Context, ds datas.Dataset, ns tree.NodeStore, idx int) (hash.Hash, error) {
	v, ok := ds.MaybeHead()
	if !ok {
		return hash.Hash{}, errors.New("stashes not found")
	}

	return datas.GetStashAtIdx(ctx, ns, v, idx)
}

// getStashList returns stash root value and head commit of a stash entry at given index.
func getStashAtIdx(ctx context.Context, ds datas.Dataset, vrw types.ValueReadWriter, ns tree.NodeStore, idx int) (*RootValue, *Commit, *datas.StashMeta, error) {
	v, ok := ds.MaybeHead()
	if !ok {
		return nil, nil, nil, errors.New("stashes not found")
	}

	stashHash, err := datas.GetStashAtIdx(ctx, ns, v, idx)
	if err != nil {
		return nil, nil, nil, err
	}
	stashVal, err := vrw.ReadValue(ctx, stashHash)
	if err != nil {
		return nil, nil, nil, err
	}

	stashRootAddr, headCommitAddr, meta, err := datas.GetStashData(stashVal)
	if err != nil {
		return nil, nil, nil, err
	}

	hc, err := datas.LoadCommitAddr(ctx, vrw, headCommitAddr)
	if err != nil {
		return nil, nil, nil, err
	}
	headCommit, err := NewCommit(ctx, vrw, ns, hc)
	if err != nil {
		return nil, nil, nil, err
	}

	stashRootVal, err := vrw.ReadValue(ctx, stashRootAddr)
	if err != nil {
		return nil, nil, nil, err
	}
	stashRoot, err := newRootValue(vrw, ns, stashRootVal)
	if err != nil {
		return nil, nil, nil, err
	}

	return stashRoot, headCommit, meta, nil
}
