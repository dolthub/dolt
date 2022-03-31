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

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var errCommitHasNoMeta = errors.New("commit has no metadata")
var errHasNoRootValue = errors.New("no root value")

// Commit contains information on a commit that was written to noms
type Commit struct {
	vrw     types.ValueReadWriter
	parents []*datas.Commit
	dCommit *datas.Commit
}

func NewCommit(ctx context.Context, vrw types.ValueReadWriter, commit *datas.Commit) (*Commit, error) {
	parents, err := datas.GetCommitParents(ctx, vrw, commit.NomsValue())
	if err != nil {
		return nil, err
	}
	return &Commit{vrw, parents, commit}, nil
}

// HashOf returns the hash of the commit
func (c *Commit) HashOf() (hash.Hash, error) {
	return c.dCommit.Addr(), nil
}

// GetCommitMeta gets the metadata associated with the commit
func (c *Commit) GetCommitMeta(ctx context.Context) (*datas.CommitMeta, error) {
	return datas.GetCommitMeta(ctx, c.dCommit.NomsValue())
}

// DatasParents returns the []*datas.Commit of the commit parents.
func (c *Commit) DatasParents() []*datas.Commit {
	return c.parents
}

// ParentHashes returns the commit hashes for all parent commits.
func (c *Commit) ParentHashes(ctx context.Context) ([]hash.Hash, error) {
	hashes := make([]hash.Hash, len(c.parents))
	for i, pr := range c.parents {
		hashes[i] = pr.Addr()
	}
	return hashes, nil
}

// NumParents gets the number of parents a commit has.
func (c *Commit) NumParents() (int, error) {
	return len(c.parents), nil
}

func (c *Commit) Height() (uint64, error) {
	return c.dCommit.Height(), nil
}

// GetRootValue gets the RootValue of the commit.
func (c *Commit) GetRootValue(ctx context.Context) (*RootValue, error) {
	rootV, err := datas.GetCommittedValue(ctx, c.vrw, c.dCommit.NomsValue())
	if err != nil {
		return nil, err
	}
	if rootV == nil {
		return nil, errHasNoRootValue
	}
	// TODO: Get rid of this types.Struct assert.
	return newRootValue(c.vrw, rootV.(types.Struct))
}

func (c *Commit) GetParent(ctx context.Context, idx int) (*Commit, error) {
	return NewCommit(ctx, c.vrw, c.parents[idx])
}

var ErrNoCommonAncestor = errors.New("no common ancestor")

func GetCommitAncestor(ctx context.Context, cm1, cm2 *Commit) (*Commit, error) {
	addr, err := getCommitAncestorAddr(ctx, cm1.dCommit, cm2.dCommit, cm1.vrw, cm2.vrw)
	if err != nil {
		return nil, err
	}

	targetVal, err := cm1.vrw.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}

	// TODO: Get rid of this tomfoolery.
	targetRef, err := types.NewRef(targetVal, cm1.vrw.Format())
	if err != nil {
		return nil, err
	}
	targetCommit, err := datas.LoadCommitRef(ctx, cm1.vrw, targetRef)
	if err != nil {
		return nil, err
	}

	return NewCommit(ctx, cm1.vrw, targetCommit)
}

func getCommitAncestorAddr(ctx context.Context, c1, c2 *datas.Commit, vrw1, vrw2 types.ValueReadWriter) (hash.Hash, error) {
	ancestorAddr, ok, err := datas.FindCommonAncestor(ctx, c1, c2, vrw1, vrw2)
	if err != nil {
		return hash.Hash{}, err
	}

	if !ok {
		return hash.Hash{}, ErrNoCommonAncestor
	}

	return ancestorAddr, nil
}

func (c *Commit) CanFastForwardTo(ctx context.Context, new *Commit) (bool, error) {
	ancestor, err := GetCommitAncestor(ctx, c, new)

	if err != nil {
		return false, err
	} else if ancestor == nil {
		return false, errors.New("cannot perform fast forward merge; commits have no common ancestor")
	} else if ancestor.dCommit.Addr() == c.dCommit.Addr() {
		if ancestor.dCommit.Addr() == new.dCommit.Addr() {
			return true, ErrUpToDate
		}
		return true, nil
	} else if ancestor.dCommit.Addr() == new.dCommit.Addr() {
		return false, ErrIsAhead
	}

	return false, nil
}

func (c *Commit) CanFastReverseTo(ctx context.Context, new *Commit) (bool, error) {
	ancestor, err := GetCommitAncestor(ctx, c, new)

	if err != nil {
		return false, err
	} else if ancestor == nil {
		return false, errors.New("cannot perform fast forward merge; commits have no common ancestor")
	} else if ancestor.dCommit.Addr() == new.dCommit.Addr() {
		if ancestor.dCommit.Addr() == c.dCommit.Addr() {
			return true, ErrUpToDate
		}
		return true, nil
	} else if ancestor.dCommit.Addr() == c.dCommit.Addr() {
		return false, ErrIsBehind
	}

	return false, nil
}

func (c *Commit) GetAncestor(ctx context.Context, as *AncestorSpec) (*Commit, error) {
	if as == nil || len(as.Instructions) == 0 {
		return c, nil
	}

	cur := c

	instructions := as.Instructions
	for _, inst := range instructions {
		n, err := cur.NumParents()
		if err != nil {
			return nil, err
		}
		if inst >= n {
			return nil, ErrInvalidAncestorSpec
		}

		cur, err = cur.GetParent(ctx, inst)
		if err != nil {
			return nil, err
		}
		if cur == nil {
			return nil, ErrInvalidAncestorSpec
		}
	}

	return cur, nil
}

// PendingCommit represents a commit that hasn't yet been written to storage. It contains a root value and options to
// use when committing it. Use a PendingCommit when it's important to update the working set and HEAD together
// atomically, via doltdb.CommitWithWorkingSet
type PendingCommit struct {
	Roots         Roots
	Val           types.Value
	CommitOptions datas.CommitOptions
}

// NewPendingCommit returns a new PendingCommit object to be written with doltdb.CommitWithWorkingSet.
// |roots| are the current roots to include in the PendingCommit. roots.Staged is used as the new root to package in the
// commit, once written.
// |headRef| is the ref of the HEAD the commit will update
// |parentCommits| are any additional merge parents for this commit. The current HEAD commit is always considered a
// parent.
// |cm| is the metadata for the commit
func (ddb *DoltDB) NewPendingCommit(
	ctx context.Context,
	roots Roots,
	headRef ref.DoltRef,
	parentCommits []*Commit,
	cm *datas.CommitMeta,
) (*PendingCommit, error) {
	val, err := ddb.writeRootValue(ctx, roots.Staged)
	if err != nil {
		return nil, err
	}

	ds, err := ddb.db.GetDataset(ctx, headRef.String())
	if err != nil {
		return nil, err
	}

	nomsHeadAddr, hasHead := ds.MaybeHeadAddr()
	var parents []hash.Hash
	if hasHead {
		parents = append(parents, nomsHeadAddr)
	}

	for _, pc := range parentCommits {
		if pc.dCommit.Addr() != nomsHeadAddr {
			parents = append(parents, pc.dCommit.Addr())
		}
	}

	commitOpts := datas.CommitOptions{Parents: parents, Meta: cm}
	return &PendingCommit{
		Roots:         roots,
		Val:           val,
		CommitOptions: commitOpts,
	}, nil
}
