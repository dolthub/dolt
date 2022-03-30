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
	meta    *datas.CommitMeta
	parents []*datas.Commit
	stref   types.Ref
	rootV   types.Value
}

func NewCommit(ctx context.Context, vrw types.ValueReadWriter, commitV types.Value) (*Commit, error) {
	parents, err := datas.GetCommitParents(ctx, vrw, commitV)
	if err != nil {
		return nil, err
	}
	meta, err := datas.GetCommitMeta(ctx, commitV)
	if err != nil {
		meta = nil
	}
	cref, err := types.NewRef(commitV, vrw.Format())
	if err != nil {
		return nil, err
	}
	rootVal, err := datas.GetCommittedValue(ctx, vrw, commitV)
	if err != nil {
		return nil, err
	}
	return &Commit{vrw, meta, parents, cref, rootVal}, nil
}

// HashOf returns the hash of the commit
func (c *Commit) HashOf() (hash.Hash, error) {
	return c.stref.TargetHash(), nil
}

// GetCommitMeta gets the metadata associated with the commit
func (c *Commit) GetCommitMeta() (*datas.CommitMeta, error) {
	if c.meta == nil {
		return nil, errCommitHasNoMeta
	}
	return c.meta, nil
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
	return c.stref.Height(), nil
}

// GetRootValue gets the RootValue of the commit.
func (c *Commit) GetRootValue() (*RootValue, error) {
	if c.rootV == nil {
		return nil, errHasNoRootValue
	}
	// TODO: Get rid of this types.Struct assert.
	return newRootValue(c.vrw, c.rootV.(types.Struct))
}

// GetStRef returns a Noms Ref for this Commit's Noms commit Struct.
func (c *Commit) GetStRef() (types.Ref, error) {
	return c.stref, nil
}

func (c *Commit) GetParent(ctx context.Context, idx int) (*Commit, error) {
	p := c.parents[idx]
	v := p.NomsValue()
	return NewCommit(ctx, c.vrw, v)
}

var ErrNoCommonAncestor = errors.New("no common ancestor")

func GetCommitAncestor(ctx context.Context, cm1, cm2 *Commit) (*Commit, error) {
	ref1, err := cm1.GetStRef()
	if err != nil {
		return nil, err
	}

	ref2, err := cm2.GetStRef()
	if err != nil {
		return nil, err
	}

	addr, err := getCommitAncestorAddr(ctx, ref1, ref2, cm1.vrw, cm2.vrw)
	if err != nil {
		return nil, err
	}

	targetVal, err := cm1.vrw.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}

	return NewCommit(ctx, cm1.vrw, targetVal)
}

func getCommitAncestorAddr(ctx context.Context, ref1, ref2 types.Ref, vrw1, vrw2 types.ValueReadWriter) (hash.Hash, error) {
	ancestorAddr, ok, err := datas.FindCommonAncestor(ctx, ref1, ref2, vrw1, vrw2)

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
	} else if ancestor.stref.Equals(c.stref) {
		if ancestor.stref.Equals(new.stref) {
			return true, ErrUpToDate
		}
		return true, nil
	} else if ancestor.stref.Equals(new.stref) {
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
	} else if ancestor.stref.Equals(new.stref) {
		if ancestor.stref.Equals(c.stref) {
			return true, ErrUpToDate
		}
		return true, nil
	} else if ancestor.stref.Equals(c.stref) {
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
		if pc.stref.TargetHash() != nomsHeadAddr {
			parents = append(parents, pc.stref.TargetHash())
		}
	}

	commitOpts := datas.CommitOptions{Parents: parents, Meta: cm}
	return &PendingCommit{
		Roots:         roots,
		Val:           val,
		CommitOptions: commitOpts,
	}, nil
}
