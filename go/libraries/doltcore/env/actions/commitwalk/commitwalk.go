// Copyright 2019 Dolthub, Inc.
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

// Package commitwalk provides topological commit graph iterators that return
// doltdb types. The walk algorithm is implemented in the lightweight
// commitgraph package; this package adapts it to *doltdb.DoltDB.
package commitwalk

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/commitgraph"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// doltDBResolver adapts *doltdb.DoltDB to commitgraph.HashResolver.
type doltDBResolver struct {
	ddb *doltdb.DoltDB
	// cache maps hashes to resolved OptionalCommits so the adapter layer
	// can return them alongside commitgraph.CommitInfo results.
	cache map[hash.Hash]*doltdb.OptionalCommit
}

func newResolver(ddb *doltdb.DoltDB) *doltDBResolver {
	return &doltDBResolver{ddb: ddb, cache: make(map[hash.Hash]*doltdb.OptionalCommit)}
}

func (r *doltDBResolver) ResolveCommitHash(ctx context.Context, h hash.Hash) (*commitgraph.CommitInfo, error) {
	oc, err := r.ddb.ResolveHash(ctx, h)
	if err != nil {
		return nil, err
	}

	r.cache[h] = oc

	info := &commitgraph.CommitInfo{Hash: h}
	commit, ok := oc.ToCommit()
	if !ok {
		info.IsGhost = true
		return info, nil
	}

	info.Height, err = commit.Height()
	if err != nil {
		return nil, err
	}

	info.Meta, err = commit.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	for _, p := range commit.DatasParents() {
		info.Parents = append(info.Parents, p.Addr())
	}

	return info, nil
}

func (r *doltDBResolver) lookupOptionalCommit(h hash.Hash) *doltdb.OptionalCommit {
	return r.cache[h]
}

// GetDotDotRevisions returns the commits reachable from commit at hashes
// `includedHeads` that are not reachable from hashes `excludedHeads`.
// `includedHeads` and `excludedHeads` must be commits in `ddb`. Returns up
// to `num` commits, in reverse topological order starting at `includedHeads`,
// with tie breaking based on the height of commit graph between
// concurrent commits --- higher commits appear first. Remaining
// ties are broken by timestamp; newer commits appear first.
//
// Roughly mimics `git log main..feature` or `git log main...feature` (if
// more than one `includedHead` is provided).
func GetDotDotRevisions(ctx context.Context, includedDB *doltdb.DoltDB, includedHeads []hash.Hash, excludedDB *doltdb.DoltDB, excludedHeads []hash.Hash, num int) ([]*doltdb.OptionalCommit, error) {
	itr, err := GetDotDotRevisionsIterator[context.Context](ctx, includedDB, includedHeads, excludedDB, excludedHeads, nil)
	if err != nil {
		return nil, err
	}

	var commitList []*doltdb.OptionalCommit
	for num < 0 || len(commitList) < num {
		_, commit, _, _, err := itr.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		commitList = append(commitList, commit)
	}

	return commitList, nil
}

// GetTopologicalOrderIterator returns an iterator for commits in reverse
// topological order, wrapping the lightweight commitgraph.Iterator.
func GetTopologicalOrderIterator[C doltdb.Context](ctx context.Context, ddb *doltdb.DoltDB, startCommitHashes []hash.Hash, matchFn func(*doltdb.OptionalCommit) (bool, error)) (doltdb.CommitItr[C], error) {
	r := newResolver(ddb)
	var cgMatchFn func(*commitgraph.CommitInfo) (bool, error)
	if matchFn != nil {
		cgMatchFn = func(ci *commitgraph.CommitInfo) (bool, error) {
			oc := r.lookupOptionalCommit(ci.Hash)
			if oc == nil {
				return true, nil
			}
			return matchFn(oc)
		}
	}
	inner, err := commitgraph.GetTopologicalOrderIterator(ctx, r, startCommitHashes, cgMatchFn)
	if err != nil {
		return nil, err
	}
	return &commiterator[C]{resolver: r, inner: inner}, nil
}

type commiterator[C doltdb.Context] struct {
	resolver *doltDBResolver
	inner    commitgraph.Iterator
}

var _ doltdb.CommitItr[context.Context] = (*commiterator[context.Context])(nil)

// Next implements doltdb.CommitItr
func (iter *commiterator[C]) Next(ctx C) (hash.Hash, *doltdb.OptionalCommit, *datas.CommitMeta, uint64, error) {
	ci, err := iter.inner.Next(ctx)
	if err != nil {
		return hash.Hash{}, nil, nil, 0, err
	}
	oc := iter.resolver.lookupOptionalCommit(ci.Hash)
	if oc == nil {
		oc = &doltdb.OptionalCommit{Addr: ci.Hash}
	}
	return ci.Hash, oc, ci.Meta, ci.Height, nil
}

// Reset implements doltdb.CommitItr
func (iter *commiterator[C]) Reset(ctx context.Context) error {
	return iter.inner.Reset(ctx)
}

// GetDotDotRevisionsIterator returns an iterator for commits generated with
// the same semantics as GetDotDotRevisions.
func GetDotDotRevisionsIterator[C doltdb.Context](ctx context.Context, includedDdb *doltdb.DoltDB, startCommitHashes []hash.Hash, excludedDdb *doltdb.DoltDB, excludingCommitHashes []hash.Hash, matchFn func(*doltdb.OptionalCommit) (bool, error)) (doltdb.CommitItr[C], error) {
	inclR := newResolver(includedDdb)
	exclR := newResolver(excludedDdb)

	var cgMatchFn func(*commitgraph.CommitInfo) (bool, error)
	if matchFn != nil {
		cgMatchFn = func(ci *commitgraph.CommitInfo) (bool, error) {
			oc := inclR.lookupOptionalCommit(ci.Hash)
			if oc == nil {
				oc = exclR.lookupOptionalCommit(ci.Hash)
			}
			if oc == nil {
				return true, nil
			}
			return matchFn(oc)
		}
	}

	inner, err := commitgraph.GetDotDotIterator(ctx, inclR, startCommitHashes, exclR, excludingCommitHashes, cgMatchFn)
	if err != nil {
		return nil, err
	}
	return &dotDotCommiterator[C]{inclResolver: inclR, exclResolver: exclR, inner: inner}, nil
}

// GetTopNTopoOrderedCommitsMatching returns the first N commits (If N <= 0 then all commits) reachable from the commits in
// `startCommitHashes` in reverse topological order, with tiebreaking done by the height of the commit graph -- higher
// commits appear first. Remaining ties are broken by timestamp; newer commits appear first. DO NOT DELETE, USED IN DOLTHUB
func GetTopNTopoOrderedCommitsMatching(ctx context.Context, ddb *doltdb.DoltDB, startCommitHashes []hash.Hash, n int, matchFn func(commit *doltdb.OptionalCommit) (bool, error)) ([]*doltdb.Commit, error) {
	itr, err := GetTopologicalOrderIterator[context.Context](ctx, ddb, startCommitHashes, matchFn)
	if err != nil {
		return nil, err
	}

	var commitList []*doltdb.Commit
	for n < 0 || len(commitList) < n {
		_, optCmt, _, _, err := itr.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		commit, ok := optCmt.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitEncountered
		}
		commitList = append(commitList, commit)
	}
	return commitList, nil
}

type dotDotCommiterator[C doltdb.Context] struct {
	inclResolver *doltDBResolver
	exclResolver *doltDBResolver
	inner        commitgraph.Iterator
}

var _ doltdb.CommitItr[context.Context] = (*dotDotCommiterator[context.Context])(nil)

// Next implements doltdb.CommitItr
func (i *dotDotCommiterator[C]) Next(ctx C) (hash.Hash, *doltdb.OptionalCommit, *datas.CommitMeta, uint64, error) {
	ci, err := i.inner.Next(ctx)
	if err != nil {
		return hash.Hash{}, nil, nil, 0, err
	}
	oc := i.inclResolver.lookupOptionalCommit(ci.Hash)
	if oc == nil {
		oc = i.exclResolver.lookupOptionalCommit(ci.Hash)
	}
	if oc == nil {
		oc = &doltdb.OptionalCommit{Addr: ci.Hash}
	}
	return ci.Hash, oc, ci.Meta, ci.Height, nil
}

// Reset implements doltdb.CommitItr
func (i *dotDotCommiterator[C]) Reset(ctx context.Context) error {
	return i.inner.Reset(ctx)
}
