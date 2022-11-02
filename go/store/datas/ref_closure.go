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

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// CommitClosure is a transitive closure of commit parents.
type CommitClosure interface {
	// Contains returns true if |commit| is contained in the closure.
	Contains(ctx context.Context, commit *Commit) (bool, error)
}

// NewSetCommitClosure computes the entire transitive closure of |commit|.
func NewSetCommitClosure(ctx context.Context, vr types.ValueReader, commit *Commit) (CommitClosure, error) {
	s, err := transitiveClosure(ctx, vr, commit)
	if err != nil {
		return setCommitClosure{}, err
	}

	return setCommitClosure{HashSet: s}, nil
}

type setCommitClosure struct {
	hash.HashSet
}

var _ CommitClosure = setCommitClosure{}

// Contains returns true if |commit| is contained in the closure.
func (s setCommitClosure) Contains(ctx context.Context, commit *Commit) (ok bool, err error) {
	ok = s.HashSet.Has(commit.Addr())
	return
}

func transitiveClosure(ctx context.Context, vr types.ValueReader, commit *Commit) (s hash.HashSet, err error) {
	h := &CommitByHeightHeap{commit}
	s = hash.NewHashSet()

	var curr []*Commit
	for !h.Empty() {
		curr = h.PopCommitsOfHeight(h.MaxHeight())
		for _, c := range curr {
			s.Insert(c.Addr())
		}

		err = parentsToQueue(ctx, curr, h, vr)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

// NewLazyCommitClosure makes a lazy CommitClosure, which computes the
// transitive closure of |commit| on demand to answer Contains() queries.
func NewLazyCommitClosure(commit *Commit, vr types.ValueReader) CommitClosure {
	return lazyCommitClosure{
		seen: hash.NewHashSet(commit.Addr()),
		heap: &CommitByHeightHeap{commit},
		vr:   vr,
	}
}

type lazyCommitClosure struct {
	seen hash.HashSet
	heap *CommitByHeightHeap
	vr   types.ValueReader
}

var _ CommitClosure = lazyCommitClosure{}

// Contains returns true if |commit| is contained in the closure.
func (l lazyCommitClosure) Contains(ctx context.Context, commit *Commit) (ok bool, err error) {
	err = l.traverseBelowDepth(ctx, commit.Height())
	if err != nil {
		return false, err
	}
	return l.seen.Has(commit.Addr()), nil
}

// traverseBelowDepth traverses through all of the refs of height |depth| or higher,
// adding them to the set |l.seen|.
func (l lazyCommitClosure) traverseBelowDepth(ctx context.Context, depth uint64) (err error) {
	var curr []*Commit
	for !l.heap.Empty() && depth <= l.heap.MaxHeight() {
		curr = l.heap.PopCommitsOfHeight(l.heap.MaxHeight())
		for _, r := range curr {
			l.seen.Insert(r.Addr())
		}
		err = parentsToQueue(ctx, curr, l.heap, l.vr)
		if err != nil {
			return err
		}
	}
	return nil
}
