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

package doltdb

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// CommitItr is an interface for iterating over a set of unique commits
type CommitItr interface {
	// Next returns the hash of the next commit, and a pointer to that commit.  Implementations of Next must handle
	// making sure the list of commits returned are unique.  When complete Next will return hash.Hash{}, nil, io.EOF
	Next(ctx context.Context) (hash.Hash, *Commit, error)

	// Reset the commit iterator back to the start
	Reset(ctx context.Context) error
}

type commitItr struct {
	ddb         *DoltDB
	rootCommits []*Commit
	currentRoot int

	added       map[hash.Hash]bool
	unprocessed []hash.Hash
	curr        *Commit
}

// CommitItrForAllBranches returns a CommitItr which will iterate over all commits in all branches in a DoltDB
func CommitItrForAllBranches(ctx context.Context, ddb *DoltDB) (CommitItr, error) {
	refs, err := ddb.GetRefs(ctx)

	if err != nil {
		return nil, err
	}

	rootCommits := make([]*Commit, 0, len(refs))
	for _, ref := range refs {
		cs, err := NewCommitSpec("HEAD", ref.String())

		if err != nil {
			return nil, err
		}

		cm, err := ddb.Resolve(ctx, cs)

		if err != nil {
			return nil, err
		}

		rootCommits = append(rootCommits, cm)
	}

	cmItr := CommitItrForRoots(ddb, rootCommits...)
	return cmItr, nil
}

// CommitItrForRoots will return a CommitItr which will iterate over all descendant commits of the provided rootCommits.
func CommitItrForRoots(ddb *DoltDB, rootCommits ...*Commit) CommitItr {
	return &commitItr{
		ddb:         ddb,
		rootCommits: rootCommits,
		added:       make(map[hash.Hash]bool, 4096),
		unprocessed: make([]hash.Hash, 0, 4096),
	}
}

func (cmItr *commitItr) Reset(ctx context.Context) error {
	cmItr.curr = nil
	cmItr.currentRoot = 0
	cmItr.added = make(map[hash.Hash]bool, 4096)
	cmItr.unprocessed = cmItr.unprocessed[:0]

	return nil
}

// Next returns the hash of the next commit, and a pointer to that commit.  It handles making sure the list of commits
// returned are unique.  When complete Next will return hash.Hash{}, nil, io.EOF
func (cmItr *commitItr) Next(ctx context.Context) (hash.Hash, *Commit, error) {
	for cmItr.curr == nil {
		if cmItr.currentRoot >= len(cmItr.rootCommits) {
			return hash.Hash{}, nil, io.EOF
		}

		cm := cmItr.rootCommits[cmItr.currentRoot]
		h, err := cm.HashOf()

		if err != nil {
			return hash.Hash{}, nil, err
		}

		if !cmItr.added[h] {
			cmItr.added[h] = true
			cmItr.curr = cm
			return h, cmItr.curr, nil
		}

		cmItr.currentRoot++
	}

	parents, err := cmItr.curr.ParentHashes(ctx)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	for _, h := range parents {
		if !cmItr.added[h] {
			cmItr.added[h] = true
			cmItr.unprocessed = append(cmItr.unprocessed, h)
		}
	}

	numUnprocessed := len(cmItr.unprocessed)

	if numUnprocessed == 0 {
		cmItr.curr = nil
		cmItr.currentRoot++
		return cmItr.Next(ctx)
	}

	next := cmItr.unprocessed[numUnprocessed-1]
	cmItr.unprocessed = cmItr.unprocessed[:numUnprocessed-1]
	cmItr.curr, err = hashToCommit(ctx, cmItr.ddb.ValueReadWriter(), next)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	return next, cmItr.curr, nil
}

func hashToCommit(ctx context.Context, vrw types.ValueReadWriter, h hash.Hash) (*Commit, error) {
	val, err := vrw.ReadValue(ctx, h)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, errors.New("failed to get commit")
	}

	cmSt := val.(types.Struct)
	return NewCommit(vrw, cmSt), nil
}

type CommitIndexingCommitItr struct {
	ddb         *DoltDB
	itr         CommitItr
	commits     []hash.Hash
	authors     []string
	commitTimes []time.Time
}

func NewCommitIndexingCommitItr(ddb *DoltDB, itr CommitItr) *CommitIndexingCommitItr {
	return &CommitIndexingCommitItr{
		ddb:         ddb,
		itr:         itr,
		commits:     make([]hash.Hash, 0, 4096),
		authors:     make([]string, 0, 4096),
		commitTimes: make([]time.Time, 0, 4096),
	}
}

func (cmItr *CommitIndexingCommitItr) Reset(ctx context.Context) error {
	cmItr.commits = make([]hash.Hash, 0, 4096)
	cmItr.authors = make([]string, 0, 4096)
	cmItr.commitTimes = make([]time.Time, 0, 4096)

	return cmItr.itr.Reset(ctx)
}

func (cmItr *CommitIndexingCommitItr) Next(ctx context.Context) (hash.Hash, *Commit, error) {
	h, cm, err := cmItr.itr.Next(ctx)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	meta, err := cm.GetCommitMeta()

	if err != nil {
		return hash.Hash{}, nil, err
	}

	commitTS := meta.Time()
	author := meta.Name

	cmItr.commits = append(cmItr.commits, h)
	cmItr.commitTimes = append(cmItr.commitTimes, commitTS)
	cmItr.authors = append(cmItr.authors, author)

	return h, cm, nil
}

type TimeRange struct {
	Min time.Time
	Max time.Time
}

func (tr *TimeRange) Contains(t time.Time) bool {
	return t.After(tr.Min) && t.Before(tr.Max)
}

func (cmItr *CommitIndexingCommitItr) Unfiltered() *CommitHashItr {
	return &CommitHashItr{cmItr.ddb.ValueReadWriter(), cmItr.commits, 0}
}

type CommitCheck func(context.Context, hash.Hash, string, time.Time) (bool, error)

func (cmItr *CommitIndexingCommitItr) Filter(ctx context.Context, check CommitCheck) (*CommitHashItr, error) {
	hashes := make([]hash.Hash, 0, len(cmItr.commits))
	for i, h := range cmItr.commits {
		author := cmItr.authors[i]
		ts := cmItr.commitTimes[i]

		passed, err := check(ctx, h, author, ts)

		if err != nil {
			return nil, err
		}

		if passed {
			hashes = append(hashes, h)
		}
	}

	return &CommitHashItr{cmItr.ddb.ValueReadWriter(), hashes, 0}, nil
}

type CommitHashItr struct {
	vrw    types.ValueReadWriter
	hashes []hash.Hash
	n      int
}

func (chItr *CommitHashItr) Next(ctx context.Context) (hash.Hash, *Commit, error) {
	if chItr.n >= len(chItr.hashes) {
		return hash.Hash{}, nil, io.EOF
	}

	h := chItr.hashes[chItr.n]

	cm, err := hashToCommit(ctx, chItr.vrw, h)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	chItr.n++
	return h, cm, nil
}

func (chItr *CommitHashItr) Reset(ctx context.Context) error {
	chItr.n = 0
	return nil
}
