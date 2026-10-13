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
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/commitgraph"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// CommitResolver resolves commits for commitgraph, and answers ancestry questions from their parent closures.
type CommitResolver struct {
	load      func(ctx context.Context, h hash.Hash) (*OptionalCommit, error)
	commits   map[hash.Hash]*OptionalCommit
	ancestors map[hash.Hash]*CommitAncestry
}

var _ commitgraph.HashResolver = (*CommitResolver)(nil)
var _ commitgraph.AncestryIndex = (*CommitResolver)(nil)

func NewCommitResolver(ddb *DoltDB) *CommitResolver {
	return newCommitResolver(ddb.ResolveHash)
}

// newCommitResolverFor resolves commits in the store of |c|.
func newCommitResolverFor(c *Commit) *CommitResolver {
	return newCommitResolver(func(ctx context.Context, h hash.Hash) (*OptionalCommit, error) {
		dc, err := datas.LoadCommitAddr(ctx, c.vrw, h)
		if err != nil {
			return nil, err
		}
		oc := &OptionalCommit{Addr: h}
		if !dc.IsGhost() {
			if oc.Commit, err = NewCommit(ctx, c.vrw, c.ns, dc); err != nil {
				return nil, err
			}
		}
		return oc, nil
	})
}

func newCommitResolver(load func(ctx context.Context, h hash.Hash) (*OptionalCommit, error)) *CommitResolver {
	return &CommitResolver{
		load:      load,
		commits:   make(map[hash.Hash]*OptionalCommit),
		ancestors: make(map[hash.Hash]*CommitAncestry),
	}
}

func (r *CommitResolver) ResolveCommitHash(ctx context.Context, h hash.Hash) (*commitgraph.CommitInfo, error) {
	oc, err := r.load(ctx, h)
	if err != nil {
		return nil, err
	}
	r.commits[h] = oc

	info := &commitgraph.CommitInfo{Hash: h}
	commit, ok := oc.ToCommit()
	if !ok {
		info.IsGhost = true
		return info, nil
	}
	if info.Height, err = commit.Height(); err != nil {
		return nil, err
	}
	if info.Meta, err = commit.GetCommitMeta(ctx); err != nil {
		return nil, err
	}
	for _, p := range commit.DatasParents() {
		info.Parents = append(info.Parents, p.Addr())
	}
	return info, nil
}

// LookupOptionalCommit returns the commit |h| if it has been resolved.
func (r *CommitResolver) LookupOptionalCommit(h hash.Hash) *OptionalCommit {
	return r.commits[h]
}

func (r *CommitResolver) IsAncestor(ctx context.Context, ancestor *commitgraph.CommitInfo, descendant hash.Hash) (bool, error) {
	a, err := r.ancestry(ctx, descendant)
	if err != nil {
		return false, err
	}
	return a.Contains(ctx, ancestor.Hash, ancestor.Height)
}

func (r *CommitResolver) GhostHeight(ctx context.Context, ghost hash.Hash, child *commitgraph.CommitInfo) (uint64, error) {
	a, err := r.ancestry(ctx, child.Hash)
	if err != nil {
		return 0, err
	}
	return a.HeightOf(ctx, ghost)
}

func (r *CommitResolver) ancestry(ctx context.Context, h hash.Hash) (*CommitAncestry, error) {
	if a, ok := r.ancestors[h]; ok {
		return a, nil
	}
	oc, ok := r.commits[h]
	if !ok {
		var err error
		if oc, err = r.load(ctx, h); err != nil {
			return nil, err
		}
	}
	commit, ok := oc.ToCommit()
	if !ok {
		return nil, fmt.Errorf("%w Missing commit: %s", ErrGhostCommitEncountered, h)
	}
	a, err := commit.GetAncestry(ctx)
	if err != nil {
		return nil, err
	}
	r.ancestors[h] = a
	return a, nil
}
