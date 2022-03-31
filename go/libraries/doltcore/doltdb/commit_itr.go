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

package doltdb

import (
	"context"
	"errors"
	"io"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
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
	branchRefs, err := ddb.GetBranches(ctx)

	if err != nil {
		return nil, err
	}

	rootCommits := make([]*Commit, 0, len(branchRefs))
	for _, ref := range branchRefs {
		cm, err := ddb.ResolveCommitRef(ctx, ref)

		if err != nil {
			return nil, err
		}

		rootCommits = append(rootCommits, cm)
	}

	cmItr := CommitItrForRoots(ddb, rootCommits...)
	return cmItr, nil
}

// CommitItrForRoots will return a CommitItr which will iterate over all ancestor commits of the provided rootCommits.
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

	// TODO: Get rid of this tomfoolery.

	ref, err := types.NewRef(val, vrw.Format())
	if err != nil {
		return nil, err
	}

	dc, err := datas.LoadCommitRef(ctx, vrw, ref)
	if err != nil {
		return nil, err
	}

	return NewCommit(ctx, vrw, dc)
}

// CommitFilter is a function that returns true if a commit should be filtered out, and false if it should be kept
type CommitFilter func(context.Context, hash.Hash, *Commit) (filterOut bool, err error)

// FilteringCommitItr is a CommitItr implementation that applies a filtering function to limit the commits returned
type FilteringCommitItr struct {
	itr    CommitItr
	filter CommitFilter
}

func NewFilteringCommitItr(itr CommitItr, filter CommitFilter) FilteringCommitItr {
	return FilteringCommitItr{itr, filter}
}

// Next returns the hash of the next commit, and a pointer to that commit.  Implementations of Next must handle
// making sure the list of commits returned are unique.  When complete Next will return hash.Hash{}, nil, io.EOF
func (itr FilteringCommitItr) Next(ctx context.Context) (hash.Hash, *Commit, error) {
	// iteration will terminate on io.EOF or a commit that is !filteredOut
	for {
		h, cm, err := itr.itr.Next(ctx)

		if err != nil {
			return hash.Hash{}, nil, err
		}

		if filterOut, err := itr.filter(ctx, h, cm); err != nil {
			return hash.Hash{}, nil, err
		} else if !filterOut {
			return h, cm, nil
		}
	}
}

// Reset the commit iterator back to the
func (itr FilteringCommitItr) Reset(ctx context.Context) error {
	return itr.itr.Reset(ctx)
}
