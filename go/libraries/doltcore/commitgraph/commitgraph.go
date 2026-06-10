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

// Package commitgraph provides a lightweight topological commit graph iterator.
//
// Unlike env/actions/commitwalk, this package does NOT depend on the doltdb
// package and therefore does not transitively pull in go-mysql-server or
// go-icu-regex. It depends only on store/datas and store/hash.
//
// Consumers inside Dolt that already import doltdb should continue using
// commitwalk. This package is for lightweight consumers (such as DumboDB) that
// only need commit graph traversal without the full SQL engine dependency.
package commitgraph

import (
	"container/heap"
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// HashResolver resolves a commit hash to its CommitInfo. Implementations
// should return a CommitInfo with IsGhost=true for ghost (placeholder) commits.
type HashResolver interface {
	ResolveCommitHash(ctx context.Context, h hash.Hash) (*CommitInfo, error)
}

// CommitInfo holds the commit data needed for graph traversal.
type CommitInfo struct {
	Hash    hash.Hash
	Height  uint64
	Meta    *datas.CommitMeta
	Parents []hash.Hash // parent1, parent2, ...
	IsGhost bool
}

// Iterator walks commits in reverse topological order.
type Iterator interface {
	// Next returns the next commit. Returns io.EOF when exhausted.
	Next(ctx context.Context) (*CommitInfo, error)
	// Reset restarts the walk from the original start hashes.
	Reset(ctx context.Context) error
}

// GetTopologicalOrderIterator returns an Iterator that walks every commit
// reachable from startHashes in reverse topological order (highest/newest
// first). Ties at the same height are broken by timestamp (newer first).
// Ghost commits sort before resolved commits.
//
// The optional matchFn filters commits: return true to include, false to skip.
func GetTopologicalOrderIterator(
	ctx context.Context,
	resolver HashResolver,
	startHashes []hash.Hash,
	matchFn func(*CommitInfo) (bool, error),
) (Iterator, error) {
	return newTopoIter(ctx, resolver, startHashes, matchFn)
}

// GetDotDotIterator returns an Iterator that yields commits reachable from
// includedHashes but NOT reachable from excludedHashes, analogous to
// `git log excluded..included`.
func GetDotDotIterator(
	ctx context.Context,
	includedResolver HashResolver,
	includedHashes []hash.Hash,
	excludedResolver HashResolver,
	excludedHashes []hash.Hash,
	matchFn func(*CommitInfo) (bool, error),
) (Iterator, error) {
	return newDotDotIter(ctx, includedResolver, includedHashes, excludedResolver, excludedHashes, matchFn)
}

// --- priority queue --------------------------------------------------------

type entry struct {
	resolver  HashResolver
	info      *CommitInfo
	invisible bool
	queued    bool
}

type pq struct {
	pending           []*entry
	numVisiblePending int
	loaded            map[hash.Hash]*entry
}

func newPQ() *pq { return &pq{loaded: make(map[hash.Hash]*entry)} }

func (q *pq) Len() int      { return len(q.pending) }
func (q *pq) Swap(i, j int) { q.pending[i], q.pending[j] = q.pending[j], q.pending[i] }

func (q *pq) Push(x interface{}) { q.pending = append(q.pending, x.(*entry)) }
func (q *pq) Pop() interface{} {
	old := q.pending
	ret := old[len(old)-1]
	q.pending = old[:len(old)-1]
	return ret
}

func (q *pq) Less(i, j int) bool {
	ei, ej := q.pending[i], q.pending[j]
	if ei.info.IsGhost && !ej.info.IsGhost {
		return true
	}
	if !ei.info.IsGhost && ej.info.IsGhost {
		return false
	}
	if ei.info.IsGhost && ej.info.IsGhost {
		return ei.info.Hash.String() < ej.info.Hash.String()
	}
	if ei.info.Height > ej.info.Height {
		return true
	}
	if ei.info.Height == ej.info.Height && ei.info.Meta != nil && ej.info.Meta != nil {
		return ei.info.Meta.UserTimestampMillis() > ej.info.Meta.UserTimestampMillis()
	}
	return false
}

func (q *pq) get(ctx context.Context, r HashResolver, h hash.Hash) (*entry, error) {
	if e, ok := q.loaded[h]; ok {
		return e, nil
	}
	info, err := r.ResolveCommitHash(ctx, h)
	if err != nil {
		return nil, err
	}
	e := &entry{resolver: r, info: info}
	q.loaded[h] = e
	return e, nil
}

func (q *pq) addIfUnseen(ctx context.Context, r HashResolver, h hash.Hash) error {
	e, err := q.get(ctx, r, h)
	if err != nil {
		return err
	}
	if !e.queued {
		e.queued = true
		heap.Push(q, e)
		if !e.invisible {
			q.numVisiblePending++
		}
	}
	return nil
}

func (q *pq) setInvisible(ctx context.Context, r HashResolver, h hash.Hash) error {
	e, err := q.get(ctx, r, h)
	if err != nil {
		return err
	}
	if !e.invisible {
		e.invisible = true
		if e.queued {
			q.numVisiblePending--
		}
	}
	return nil
}

func (q *pq) popPending() *entry {
	e := heap.Pop(q).(*entry)
	if !e.invisible {
		q.numVisiblePending--
	}
	return e
}

// --- topological iterator --------------------------------------------------

type topoIter struct {
	resolver    HashResolver
	startHashes []hash.Hash
	matchFn     func(*CommitInfo) (bool, error)
	q           *pq
}

func newTopoIter(ctx context.Context, resolver HashResolver, startHashes []hash.Hash, matchFn func(*CommitInfo) (bool, error)) (*topoIter, error) {
	it := &topoIter{resolver: resolver, startHashes: startHashes, matchFn: matchFn}
	if err := it.Reset(ctx); err != nil {
		return nil, err
	}
	return it, nil
}

func (it *topoIter) Next(ctx context.Context) (*CommitInfo, error) {
	for it.q.numVisiblePending > 0 {
		e := it.q.popPending()
		for _, ph := range e.info.Parents {
			if err := it.q.addIfUnseen(ctx, e.resolver, ph); err != nil {
				return nil, err
			}
		}
		if it.matchFn != nil {
			ok, err := it.matchFn(e.info)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		return e.info, nil
	}
	return nil, io.EOF
}

func (it *topoIter) Reset(ctx context.Context) error {
	it.q = newPQ()
	for _, h := range it.startHashes {
		if err := it.q.addIfUnseen(ctx, it.resolver, h); err != nil {
			return err
		}
	}
	return nil
}

// --- dot-dot iterator ------------------------------------------------------

type dotDotIter struct {
	includedResolver HashResolver
	excludedResolver HashResolver
	startHashes      []hash.Hash
	excludedHashes   []hash.Hash
	matchFn          func(*CommitInfo) (bool, error)
	q                *pq
}

func newDotDotIter(ctx context.Context, inclR HashResolver, inclH []hash.Hash, exclR HashResolver, exclH []hash.Hash, matchFn func(*CommitInfo) (bool, error)) (*dotDotIter, error) {
	it := &dotDotIter{
		includedResolver: inclR,
		excludedResolver: exclR,
		startHashes:      inclH,
		excludedHashes:   exclH,
		matchFn:          matchFn,
	}
	if err := it.Reset(ctx); err != nil {
		return nil, err
	}
	return it, nil
}

func (it *dotDotIter) Next(ctx context.Context) (*CommitInfo, error) {
	for it.q.numVisiblePending > 0 {
		e := it.q.popPending()
		for _, ph := range e.info.Parents {
			if e.invisible {
				if err := it.q.setInvisible(ctx, e.resolver, ph); err != nil {
					return nil, err
				}
			}
			if err := it.q.addIfUnseen(ctx, e.resolver, ph); err != nil {
				return nil, err
			}
		}
		if e.invisible {
			continue
		}
		if it.matchFn != nil {
			ok, err := it.matchFn(e.info)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		return e.info, nil
	}
	return nil, io.EOF
}

func (it *dotDotIter) Reset(ctx context.Context) error {
	it.q = newPQ()
	for _, h := range it.excludedHashes {
		if err := it.q.setInvisible(ctx, it.excludedResolver, h); err != nil {
			return err
		}
		if err := it.q.addIfUnseen(ctx, it.excludedResolver, h); err != nil {
			return err
		}
	}
	for _, h := range it.startHashes {
		if err := it.q.addIfUnseen(ctx, it.includedResolver, h); err != nil {
			return err
		}
	}
	return nil
}
