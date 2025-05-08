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
	commit    *doltdb.OptionalCommit
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

// Less returns true if the commit at index i is "less" than the commit at index j. It may be the case that you are comparing
// two resolved commits, two ghost commits, or a resolved commit and a ghost commit. Ghost commits will always be "less" than
// resolved commits. If both commits are resolved, then the commit with the higher height is "less". If the heights are equal, then
// the commit with the newer timestamp is "less". Finally if both commits are ghost commits, we don't really have enough
// information to compare on, so we just compare the hashes to ensure that the results are stable.
func (q *q) Less(i, j int) bool {
	_, okI := q.pending[i].commit.ToCommit()
	_, okJ := q.pending[i].commit.ToCommit()

	if !okI && okJ {
		return true
	} else if okI && !okJ {
		return false
	} else if !okI && !okJ {
		return q.pending[i].hash.String() < q.pending[j].hash.String()
	}

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

func load(ctx context.Context, ddb *doltdb.DoltDB, h hash.Hash) (*doltdb.OptionalCommit, error) {
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

	optCmt, err := load(ctx, ddb, id)
	if err != nil {
		return nil, err
	}

	commit, ok := optCmt.ToCommit()
	if !ok {
		return &c{ddb: ddb, commit: optCmt, hash: id}, nil
	}

	h, err := commit.Height()
	if err != nil {
		return nil, err
	}
	meta, err := commit.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	c := &c{ddb: ddb, commit: &doltdb.OptionalCommit{Commit: commit, Addr: id}, meta: meta, height: h, hash: id}
	q.loaded[id] = c
	return c, nil
}

func newQueue() *q {
	return &q{loaded: make(map[hash.Hash]*c)}
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
	itr, err := GetDotDotRevisionsIterator(ctx, includedDB, includedHeads, excludedDB, excludedHeads, nil)
	if err != nil {
		return nil, err
	}

	var commitList []*doltdb.OptionalCommit
	for num < 0 || len(commitList) < num {
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

// GetTopologicalOrderCommitIterator returns an iterator for commits generated with the same semantics as
// GetTopologicalOrderCommits
func GetTopologicalOrderIterator(ctx context.Context, ddb *doltdb.DoltDB, startCommitHashes []hash.Hash, matchFn func(*doltdb.OptionalCommit) (bool, error)) (doltdb.CommitItr, error) {
	return newCommiterator(ctx, ddb, startCommitHashes, matchFn)
}

type commiterator struct {
	ddb               *doltdb.DoltDB
	startCommitHashes []hash.Hash
	matchFn           func(*doltdb.OptionalCommit) (bool, error)
	q                 *q
}

var _ doltdb.CommitItr = (*commiterator)(nil)

func newCommiterator(ctx context.Context, ddb *doltdb.DoltDB, startCommitHashes []hash.Hash, matchFn func(*doltdb.OptionalCommit) (bool, error)) (*commiterator, error) {
	itr := &commiterator{
		ddb:               ddb,
		startCommitHashes: startCommitHashes,
		matchFn:           matchFn,
	}

	err := itr.Reset(ctx)
	if err != nil {
		return nil, err
	}

	return itr, nil
}

// Next implements doltdb.CommitItr
func (iter *commiterator) Next(ctx context.Context) (hash.Hash, *doltdb.OptionalCommit, error) {
	if iter.q.NumVisiblePending() > 0 {
		nextC := iter.q.PopPending()

		var err error
		parents := []hash.Hash{}
		commit, ok := nextC.commit.ToCommit()
		if ok {
			parents, err = commit.ParentHashes(ctx)
			if err != nil {
				return hash.Hash{}, nil, err
			}
		}

		for _, parentID := range parents {
			if err := iter.q.AddPendingIfUnseen(ctx, nextC.ddb, parentID); err != nil {
				return hash.Hash{}, nil, err
			}
		}

		matches := true
		if iter.matchFn != nil {
			matches, err = iter.matchFn(nextC.commit)

			if err != nil {
				return hash.Hash{}, nil, err
			}
		}

		if matches {
			return nextC.hash, &doltdb.OptionalCommit{Commit: commit, Addr: nextC.hash}, nil
		}

		return iter.Next(ctx)
	}

	return hash.Hash{}, nil, io.EOF
}

// Reset implements doltdb.CommitItr
func (i *commiterator) Reset(ctx context.Context) error {
	i.q = newQueue()
	for _, startCommitHash := range i.startCommitHashes {
		if err := i.q.AddPendingIfUnseen(ctx, i.ddb, startCommitHash); err != nil {
			return err
		}
	}
	return nil
}

// GetDotDotRevisionsIterator returns an iterator for commits generated with the same semantics as
// GetDotDotRevisions
func GetDotDotRevisionsIterator(ctx context.Context, includedDdb *doltdb.DoltDB, startCommitHashes []hash.Hash, excludedDdb *doltdb.DoltDB, excludingCommitHashes []hash.Hash, matchFn func(*doltdb.OptionalCommit) (bool, error)) (doltdb.CommitItr, error) {
	return newDotDotCommiterator(ctx, includedDdb, startCommitHashes, excludedDdb, excludingCommitHashes, matchFn)
}

// GetTopNTopoOrderedCommitsMatching returns the first N commits (If N <= 0 then all commits) reachable from the commits in
// `startCommitHashes` in reverse topological order, with tiebreaking done by the height of the commit graph -- higher
// commits appear first. Remaining ties are broken by timestamp; newer commits appear first. DO NOT DELETE, USED IN DOLTHUB
func GetTopNTopoOrderedCommitsMatching(ctx context.Context, ddb *doltdb.DoltDB, startCommitHashes []hash.Hash, n int, matchFn func(commit *doltdb.OptionalCommit) (bool, error)) ([]*doltdb.Commit, error) {
	itr, err := GetTopologicalOrderIterator(ctx, ddb, startCommitHashes, matchFn)
	if err != nil {
		return nil, err
	}

	var commitList []*doltdb.Commit
	for n < 0 || len(commitList) < n {
		_, optCmt, err := itr.Next(ctx)
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

type dotDotCommiterator struct {
	includedDdb           *doltdb.DoltDB
	excludedDdb           *doltdb.DoltDB
	startCommitHashes     []hash.Hash
	excludingCommitHashes []hash.Hash
	matchFn               func(*doltdb.OptionalCommit) (bool, error)
	q                     *q
}

var _ doltdb.CommitItr = (*dotDotCommiterator)(nil)

func newDotDotCommiterator(ctx context.Context, includedDdb *doltdb.DoltDB, startCommitHashes []hash.Hash, excludedDdb *doltdb.DoltDB, excludingCommitHashes []hash.Hash, matchFn func(*doltdb.OptionalCommit) (bool, error)) (*dotDotCommiterator, error) {
	itr := &dotDotCommiterator{
		includedDdb:           includedDdb,
		excludedDdb:           excludedDdb,
		startCommitHashes:     startCommitHashes,
		excludingCommitHashes: excludingCommitHashes,
		matchFn:               matchFn,
	}

	err := itr.Reset(ctx)
	if err != nil {
		return nil, err
	}

	return itr, nil
}

// Next implements doltdb.CommitItr
func (i *dotDotCommiterator) Next(ctx context.Context) (hash.Hash, *doltdb.OptionalCommit, error) {
	if i.q.NumVisiblePending() > 0 {
		nextC := i.q.PopPending()

		commit, ok := nextC.commit.ToCommit()
		if !ok {
			return nextC.hash, nextC.commit, nil
		}

		parents, err := commit.ParentHashes(ctx)
		if err != nil {
			return hash.Hash{}, nil, err
		}

		for _, parentID := range parents {
			if nextC.invisible {
				if err := i.q.SetInvisible(ctx, nextC.ddb, parentID); err != nil {
					return hash.Hash{}, nil, err
				}
			}
			if err := i.q.AddPendingIfUnseen(ctx, nextC.ddb, parentID); err != nil {
				return hash.Hash{}, nil, err
			}
		}

		matches := true
		if i.matchFn != nil {
			matches, err = i.matchFn(nextC.commit)
			if err != nil {
				return hash.Hash{}, nil, err
			}
		}

		// If not invisible, return commit. Otherwise get next commit
		if !nextC.invisible && matches {
			return nextC.hash, nextC.commit, nil
		}
		return i.Next(ctx)
	}

	return hash.Hash{}, nil, io.EOF
}

// Reset implements doltdb.CommitItr
func (i *dotDotCommiterator) Reset(ctx context.Context) error {
	i.q = newQueue()
	for _, excludingCommitHash := range i.excludingCommitHashes {
		if err := i.q.SetInvisible(ctx, i.excludedDdb, excludingCommitHash); err != nil {
			return err
		}
		if err := i.q.AddPendingIfUnseen(ctx, i.excludedDdb, excludingCommitHash); err != nil {
			return err
		}
	}
	for _, startCommitHash := range i.startCommitHashes {
		if err := i.q.AddPendingIfUnseen(ctx, i.includedDdb, startCommitHash); err != nil {
			return err
		}
	}
	return nil
}
