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
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

// CommitItr is an interface for iterating over a set of unique commits
type CommitItr[C Context] interface {
	// Next returns the hash of the next commit, and a pointer to that commit.  Implementations of Next must handle
	// making sure the list of commits returned are unique.  When complete Next will return hash.Hash{}, nil, io.EOF
	Next(ctx C) (hash.Hash, *OptionalCommit, error)

	// Reset the commit iterator back to the start
	Reset(ctx context.Context) error
}

type commitItr[C Context] struct {
	ddb         *DoltDB
	rootCommits []*Commit
	currentRoot int

	added       map[hash.Hash]bool
	unprocessed []hash.Hash
	curr        *Commit
}

// CommitItrForAllBranches returns a CommitItr which will iterate over all commits in all branches in a DoltDB
func CommitItrForAllBranches[C Context](ctx context.Context, ddb *DoltDB) (CommitItr[C], error) {
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

	cmItr := CommitItrForRoots[C](ddb, rootCommits...)
	return cmItr, nil
}

// CommitItrForRoots will return a CommitItr which will iterate over all ancestor commits of the provided rootCommits.
func CommitItrForRoots[C Context](ddb *DoltDB, rootCommits ...*Commit) CommitItr[C] {
	return &commitItr[C]{
		ddb:         ddb,
		rootCommits: rootCommits,
		added:       make(map[hash.Hash]bool, 4096),
		unprocessed: make([]hash.Hash, 0, 4096),
	}
}

func (cmItr *commitItr[C]) Reset(ctx context.Context) error {
	cmItr.curr = nil
	cmItr.currentRoot = 0
	cmItr.added = make(map[hash.Hash]bool, 4096)
	cmItr.unprocessed = cmItr.unprocessed[:0]

	return nil
}

// Next returns the hash of the next commit, and a pointer to that commit.  It handles making sure the list of commits
// returned are unique.  When complete Next will return hash.Hash{}, nil, io.EOF
func (cmItr *commitItr[C]) Next(ctx C) (hash.Hash, *OptionalCommit, error) {
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
			return h, &OptionalCommit{cmItr.curr, h}, nil
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
	cmItr.curr, err = HashToCommit(ctx, cmItr.ddb.ValueReadWriter(), cmItr.ddb.ns, next)
	if err != nil && err != ErrGhostCommitEncountered {
		return hash.Hash{}, nil, err
	}
	if err == ErrGhostCommitEncountered {
		cmItr.curr = nil
	}

	return next, &OptionalCommit{cmItr.curr, next}, nil
}

func HashToCommit(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, h hash.Hash) (*Commit, error) {
	dc, err := datas.LoadCommitAddr(ctx, vrw, h)
	if err != nil {
		return nil, err
	}

	if dc.IsGhost() {
		return nil, ErrGhostCommitEncountered
	}

	return NewCommit(ctx, vrw, ns, dc)
}

type Context interface {
	context.Context
}

// CommitFilter is a function that returns true if a commit should be filtered out, and false if it should be kept
type CommitFilter[C Context] func(C, hash.Hash, *OptionalCommit) (filterOut bool, err error)

// FilteringCommitItr is a CommitItr implementation that applies a filtering function to limit the commits returned
type FilteringCommitItr[C Context] struct {
	itr    CommitItr[C]
	filter CommitFilter[C]
}

// AllCommits is a CommitFilter that matches all commits
func AllCommits(_ context.Context, _ hash.Hash, _ *Commit) (filterOut bool, err error) {
	return false, nil
}

func NewFilteringCommitItr[C Context](itr CommitItr[C], filter CommitFilter[C]) FilteringCommitItr[C] {
	return FilteringCommitItr[C]{itr, filter}
}

// Next returns the hash of the next commit, and a pointer to that commit.  Implementations of Next must handle
// making sure the list of commits returned are unique.  When complete Next will return hash.Hash{}, nil, io.EOF
func (itr FilteringCommitItr[C]) Next(ctx C) (hash.Hash, *OptionalCommit, error) {
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
func (itr FilteringCommitItr[C]) Reset(ctx context.Context) error {
	return itr.itr.Reset(ctx)
}

func NewCommitSliceIter[C Context](cm []*Commit, h []hash.Hash) *CommitSliceIter[C] {
	return &CommitSliceIter[C]{cm: cm, h: h}
}

type CommitSliceIter[C Context] struct {
	h  []hash.Hash
	cm []*Commit
	i  int
}

var _ CommitItr[context.Context] = (*CommitSliceIter[context.Context])(nil)

func (i *CommitSliceIter[C]) Next(ctx C) (hash.Hash, *OptionalCommit, error) {
	if i.i >= len(i.h) {
		return hash.Hash{}, nil, io.EOF
	}
	i.i++
	return i.h[i.i-1], &OptionalCommit{i.cm[i.i-1], i.h[i.i-1]}, nil

}

func (i *CommitSliceIter[C]) Reset(ctx context.Context) error {
	i.i = 0
	return nil
}

func NewOneCommitIter[C Context](cm *Commit, h hash.Hash, meta *datas.CommitMeta) *OneCommitIter[C] {
	return &OneCommitIter[C]{cm: &OptionalCommit{cm, h}, h: h}
}

type OneCommitIter[C Context] struct {
	h    hash.Hash
	cm   *OptionalCommit
	m    *datas.CommitMeta
	done bool
}

var _ CommitItr[context.Context] = (*OneCommitIter[context.Context])(nil)

func (i *OneCommitIter[C]) Next(_ C) (hash.Hash, *OptionalCommit, error) {
	if i.done {
		return hash.Hash{}, nil, io.EOF
	}
	i.done = true
	return i.h, i.cm, nil

}

func (i *OneCommitIter[C]) Reset(_ context.Context) error {
	i.done = false
	return nil
}

func NewCommitPart(h hash.Hash, cm *Commit, m *datas.CommitMeta) *CommitPart {
	return &CommitPart{h: h, cm: cm, m: m}
}

type CommitPart struct {
	h  hash.Hash
	m  *datas.CommitMeta
	cm *Commit
}

var _ sql.Partition = (*CommitPart)(nil)

func (c *CommitPart) Hash() hash.Hash {
	return c.h
}

func (c *CommitPart) Commit() *Commit {
	return c.cm
}

func (c *CommitPart) Meta() *datas.CommitMeta {
	return c.m
}

func (c *CommitPart) Key() []byte {
	return c.h[:]
}

func NewCommitSlicePartitionIter(h []hash.Hash, cm []*Commit, m []*datas.CommitMeta) *CommitSlicePartitionIter {
	return &CommitSlicePartitionIter{h: h, cm: cm, m: m}
}

type CommitSlicePartitionIter struct {
	h  []hash.Hash
	m  []*datas.CommitMeta
	cm []*Commit
	i  int
}

var _ sql.PartitionIter = (*CommitSlicePartitionIter)(nil)

func (i *CommitSlicePartitionIter) Next(ctx *sql.Context) (sql.Partition, error) {
	if i.i >= len(i.cm) {
		return nil, io.EOF
	}
	i.i++
	return &CommitPart{h: i.h[i.i-1], m: i.m[i.i-1], cm: i.cm[i.i-1]}, nil

}

func (i *CommitSlicePartitionIter) Close(ctx *sql.Context) error {
	return nil
}
