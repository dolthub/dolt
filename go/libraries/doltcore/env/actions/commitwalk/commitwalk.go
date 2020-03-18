// Copyright 2019 Liquidata, Inc.
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
	"context"
	"io"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

type c struct {
	commit    *doltdb.Commit
	hash 			hash.Hash
	height    uint64
	invisible bool
	queued    bool
}

type q struct {
	pending           []*c
	numVisiblePending int
	loaded            map[hash.Hash]*c

	ddb *doltdb.DoltDB
}

func (q *q) NumVisiblePending() int {
	return q.numVisiblePending
}

func (q *q) PopPending() *c {
	c := q.pending[len(q.pending)-1]
	q.pending = q.pending[:len(q.pending)-1]
	if !c.invisible {
		q.numVisiblePending--
	}
	return c
}

func (q *q) AddPendingIfUnseen(ctx context.Context, id hash.Hash) error {
	c, err := q.Get(ctx, id)
	if err != nil {
		return err
	}
	if !c.queued {
		c.queued = true
		var i int
		for i = 0; i < len(q.pending); i++ {
			if q.pending[i].height > c.height {
				break
			}
			if q.pending[i].height < c.height {
				continue
			}

			// if the commits have equal height, tiebreak on timestamp
			pendingMeta, err := q.pending[i].commit.GetCommitMeta()
			if err != nil {
				return err
			}
			commitMeta, err := c.commit.GetCommitMeta()
			if err != nil {
				return err
			}
			if pendingMeta.UserTimestamp > commitMeta.UserTimestamp {
				break
			}
		}
		q.pending = append(q.pending, nil)
		copy(q.pending[i+1:], q.pending[i:])
		q.pending[i] = c
		if !c.invisible {
			q.numVisiblePending++
		}
	}
	return nil
}

func (q *q) SetInvisible(ctx context.Context, id hash.Hash) error {
	c, err := q.Get(ctx, id)
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

func (q *q) load(ctx context.Context, h hash.Hash) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(h.String(), "")
	if err != nil {
		return nil, err
	}
	c, err := q.ddb.Resolve(ctx, cs)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (q *q) Get(ctx context.Context, id hash.Hash) (*c, error) {
	if l, ok := q.loaded[id]; ok {
		return l, nil
	}

	l, err := q.load(ctx, id)
	if err != nil {
		return nil, err
	}
	h, err := l.Height()
	if err != nil {
		return nil, err
	}

	c := &c{commit: l, height: h, hash: id}
	q.loaded[id] = c
	return c, nil
}

func newQueue(ddb *doltdb.DoltDB) *q {
	return &q{ddb: ddb, loaded: make(map[hash.Hash]*c)}
}

// GetDotDotRevisions returns the commits reachable from commit at hash
// `includedHead` that are not reachable from hash `excludedHead`.
// `includedHead` and `excludedHead` must be commits in `ddb`. Returns up
// to `num` commits, in reverse topological order starting at `includedHead`,
// with tie breaking based on the height of commit graph between
// concurrent commits --- higher commits appear first. Remaining
// ties are broken by timestamp; newer commits appear first.
//
// Roughly mimics `git log master..feature`.
func GetDotDotRevisions(ctx context.Context, ddb *doltdb.DoltDB, includedHead hash.Hash, excludedHead hash.Hash, num int) ([]*doltdb.Commit, error) {
	commitList := make([]*doltdb.Commit, 0, num)
	q := newQueue(ddb)
	if err := q.SetInvisible(ctx, excludedHead); err != nil {
		return nil, err
	}
	if err := q.AddPendingIfUnseen(ctx, excludedHead); err != nil {
		return nil, err
	}
	if err := q.AddPendingIfUnseen(ctx, includedHead); err != nil {
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
				if err := q.SetInvisible(ctx, parentID); err != nil {
					return nil, err
				}
			}
			if err := q.AddPendingIfUnseen(ctx, parentID); err != nil {
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
	return GetTopNTopoOrderedCommits(ctx, ddb, startCommitHash, -1)
}

// GetTopologicalOrderCommitIterator returns an iterator for commits generated with the same semantics as
// GetTopologicalOrderCommits
func GetTopologicalOrderIterator(ctx context.Context, ddb *doltdb.DoltDB, startCommitHash hash.Hash) (doltdb.CommitItr, error) {
	return newCommiterator(ctx, ddb, startCommitHash)
}

type commiterator struct {
	ddb *doltdb.DoltDB
	startCommitHash hash.Hash
	q   *q
}

var _ doltdb.CommitItr = (*commiterator)(nil)

func newCommiterator(ctx context.Context, ddb *doltdb.DoltDB, startCommitHash hash.Hash) (*commiterator, error) {
	itr := &commiterator{
		ddb: ddb,
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
			if err := i.q.AddPendingIfUnseen(ctx, parentID); err != nil {
				return hash.Hash{}, nil, err
			}
		}

		return nextC.hash, nextC.commit, nil
	}

	return hash.Hash{}, nil, io.EOF
}

// Reset implements doltdb.CommitItr
func (i *commiterator) Reset(ctx context.Context) error {
	i.q = newQueue(i.ddb)
	if err := i.q.AddPendingIfUnseen(ctx, i.startCommitHash); err != nil {
		return err
	}
	return nil
}

// GetTopNTopoOrderedCommits returns the first N commits (If N <= 0 then all commits) reachable from the commit at hash
// `startCommitHash` in reverse topological order, with tiebreaking done by the height of the commit graph -- higher
// commits appear first. Remaining ties are broken by timestamp; newer commits appear first.
func GetTopNTopoOrderedCommits(ctx context.Context, ddb *doltdb.DoltDB, startCommitHash hash.Hash, n int) ([]*doltdb.Commit, error) {
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
		commitList = append(commitList, commit)
	}

	return commitList, nil
}
