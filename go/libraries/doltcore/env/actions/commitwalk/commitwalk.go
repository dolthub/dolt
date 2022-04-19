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

package commitwalk

import (
	"container/heap"
	"context"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

type c struct {
	ddb       *doltdb.DoltDB
	commit    *doltdb.Commit
	meta      *datas.CommitMeta
	hash      hash.Hash
	height    uint64
	invisible bool
	queued    bool
}

type q struct {
	pending           []*c
	numVisiblePending int
	loaded            map[hash.Hash]*c
}

func (q *q) NumVisiblePending() int {
	return q.numVisiblePending
}

func (q *q) Push(x interface{}) {
	q.pending = append(q.pending, x.(*c))
}

func (q *q) Pop() interface{} {
	old := q.pending
	ret := old[len(old)-1]
	q.pending = old[:len(old)-1]
	return ret
}

func (q *q) Len() int {
	return len(q.pending)
}

func (q *q) Swap(i, j int) {
	q.pending[i], q.pending[j] = q.pending[j], q.pending[i]
}

func (q *q) Less(i, j int) bool {
	if q.pending[i].height > q.pending[j].height {
		return true
	}
	if q.pending[i].height == q.pending[j].height {
		return q.pending[i].meta.UserTimestamp > q.pending[j].meta.UserTimestamp
	}
	return false
}

func (q *q) PopPending() *c {
	c := heap.Pop(q).(*c)
	if !c.invisible {
		q.numVisiblePending--
	}
	return c
}

func (q *q) AddPendingIfUnseen(ctx context.Context, ddb *doltdb.DoltDB, id hash.Hash) error {
	c, err := q.Get(ctx, ddb, id)
	if err != nil {
		return err
	}
	if !c.queued {
		c.queued = true
		heap.Push(q, c)
		if !c.invisible {
			q.numVisiblePending++
		}
	}
	return nil
}

func (q *q) SetInvisible(ctx context.Context, ddb *doltdb.DoltDB, id hash.Hash) error {
	c, err := q.Get(ctx, ddb, id)
	if err != nil {
		return err
	}
	if !c.invisible {
		c.invisible = true
		if c.queued {
			q.numVisiblePending--
		}
	}
	return nil
}

func load(ctx context.Context, ddb *doltdb.DoltDB, h hash.Hash) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(h.String())
	if err != nil {
		return nil, err
	}
	c, err := ddb.Resolve(ctx, cs, nil)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (q *q) Get(ctx context.Context, ddb *doltdb.DoltDB, id hash.Hash) (*c, error) {
	if l, ok := q.loaded[id]; ok {
		return l, nil
	}

	l, err := load(ctx, ddb, id)
	if err != nil {
		return nil, err
	}
	h, err := l.Height()
	if err != nil {
		return nil, err
	}
	meta, err := l.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	c := &c{ddb: ddb, commit: l, meta: meta, height: h, hash: id}
	q.loaded[id] = c
	return c, nil
}

func newQueue() *q {
	return &q{loaded: make(map[hash.Hash]*c)}
}

// GetDotDotRevisions returns the commits reachable from commit at hash
// `includedHead` that are not reachable from hash `excludedHead`.
// `includedHead` and `excludedHead` must be commits in `ddb`. Returns up
// to `num` commits, in reverse topological order starting at `includedHead`,
// with tie breaking based on the height of commit graph between
// concurrent commits --- higher commits appear first. Remaining
// ties are broken by timestamp; newer commits appear first.
//
// Roughly mimics `git log main..feature`.
func GetDotDotRevisions(ctx context.Context, includedDB *doltdb.DoltDB, includedHead hash.Hash, excludedDB *doltdb.DoltDB, excludedHead hash.Hash, num int) ([]*doltdb.Commit, error) {
	commitList := make([]*doltdb.Commit, 0, num)
	q := newQueue()
	if err := q.SetInvisible(ctx, excludedDB, excludedHead); err != nil {
		return nil, err
	}
	if err := q.AddPendingIfUnseen(ctx, excludedDB, excludedHead); err != nil {
		return nil, err
	}
	if err := q.AddPendingIfUnseen(ctx, includedDB, includedHead); err != nil {
		return nil, err
	}
	for q.NumVisiblePending() > 0 {
		nextC := q.PopPending()
		parents, err := nextC.commit.ParentHashes(ctx)
		if err != nil {
			return nil, err
		}
		for _, parentID := range parents {
			if nextC.invisible {
				if err := q.SetInvisible(ctx, nextC.ddb, parentID); err != nil {
					return nil, err
				}
			}
			if err := q.AddPendingIfUnseen(ctx, nextC.ddb, parentID); err != nil {
				return nil, err
			}
		}
		if !nextC.invisible {
			commitList = append(commitList, nextC.commit)
			if len(commitList) == num {
				return commitList, nil
			}
		}
	}
	return commitList, nil
}

// GetTopologicalOrderCommits returns the commits reachable from the commit at hash `startCommitHash`
// in reverse topological order, with tiebreaking done by the height of the commit graph -- higher commits
// appear first. Remaining ties are broken by timestamp; newer commits appear first.
func GetTopologicalOrderCommits(ctx context.Context, ddb *doltdb.DoltDB, startCommitHash hash.Hash) ([]*doltdb.Commit, error) {
	return GetTopNTopoOrderedCommitsMatching(ctx, ddb, startCommitHash, -1, nil)
}

// GetTopologicalOrderCommitIterator returns an iterator for commits generated with the same semantics as
// GetTopologicalOrderCommits
func GetTopologicalOrderIterator(ctx context.Context, ddb *doltdb.DoltDB, startCommitHash hash.Hash) (doltdb.CommitItr, error) {
	return newCommiterator(ctx, ddb, startCommitHash)
}

type commiterator struct {
	ddb             *doltdb.DoltDB
	startCommitHash hash.Hash
	q               *q
}

var _ doltdb.CommitItr = (*commiterator)(nil)

func newCommiterator(ctx context.Context, ddb *doltdb.DoltDB, startCommitHash hash.Hash) (*commiterator, error) {
	itr := &commiterator{
		ddb:             ddb,
		startCommitHash: startCommitHash,
	}

	err := itr.Reset(ctx)
	if err != nil {
		return nil, err
	}

	return itr, nil
}

// Next implements doltdb.CommitItr
func (i *commiterator) Next(ctx context.Context) (hash.Hash, *doltdb.Commit, error) {
	if i.q.NumVisiblePending() > 0 {
		nextC := i.q.PopPending()
		parents, err := nextC.commit.ParentHashes(ctx)
		if err != nil {
			return hash.Hash{}, nil, err
		}

		for _, parentID := range parents {
			if err := i.q.AddPendingIfUnseen(ctx, nextC.ddb, parentID); err != nil {
				return hash.Hash{}, nil, err
			}
		}

		return nextC.hash, nextC.commit, nil
	}

	return hash.Hash{}, nil, io.EOF
}

// Reset implements doltdb.CommitItr
func (i *commiterator) Reset(ctx context.Context) error {
	i.q = newQueue()
	if err := i.q.AddPendingIfUnseen(ctx, i.ddb, i.startCommitHash); err != nil {
		return err
	}
	return nil
}

// GetTopNTopoOrderedCommitsMatching returns the first N commits (If N <= 0 then all commits) reachable from the commit at hash
// `startCommitHash` in reverse topological order, with tiebreaking done by the height of the commit graph -- higher
// commits appear first. Remaining ties are broken by timestamp; newer commits appear first.
func GetTopNTopoOrderedCommitsMatching(ctx context.Context, ddb *doltdb.DoltDB, startCommitHash hash.Hash, n int, matchFn func(*doltdb.Commit) (bool, error)) ([]*doltdb.Commit, error) {
	itr, err := GetTopologicalOrderIterator(ctx, ddb, startCommitHash)
	if err != nil {
		return nil, err
	}

	var commitList []*doltdb.Commit
	for n < 0 || len(commitList) < n {
		_, commit, err := itr.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		matches := true
		if matchFn != nil {
			matches, err = matchFn(commit)

			if err != nil {
				return nil, err
			}
		}

		if matches {
			commitList = append(commitList, commit)
		}
	}

	return commitList, nil
}
