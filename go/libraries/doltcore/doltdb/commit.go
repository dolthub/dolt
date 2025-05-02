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
	"fmt"
	"sort"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

var errCommitHasNoMeta = errors.New("commit has no metadata")
var errHasNoRootValue = errors.New("no root value")

// TODO: Include the commit id in the error. Unfortunately, this message is passed through the SQL layer. The only way we currently
// have on the client side to match an error is with string matching. We possibly need error codes as a prefix to the error message, but
// currently there is not standard for doing this in Dolt.
var ErrGhostCommitEncountered = errors.New("Commit not found. You are using a shallow clone which does not contain the requested commit. Please do a full clone.")
var ErrGhostCommitRuntimeFailure = errors.New("runtime failure: Ghost commit encountered unexpectedly. Please report bug to: https://github.com/dolthub/dolt/issues")

// Rootish is an object resolvable to a RootValue.
type Rootish interface {
	// ResolveRootValue resolves a Rootish to a RootValue.
	ResolveRootValue(ctx context.Context) (RootValue, error)

	// HashOf returns the hash.Hash of the Rootish.
	HashOf() (hash.Hash, error)
}

// Commit contains information on a commit that was written to noms
type Commit struct {
	vrw     types.ValueReadWriter
	ns      tree.NodeStore
	parents []*datas.Commit
	dCommit *datas.Commit
}

type OptionalCommit struct {
	Commit *Commit
	Addr   hash.Hash
}

// ToCommit unwraps the *Commit contained by the OptionalCommit. If the commit is invalid, it returns (nil, false).
// Otherwise, it returns (commit, true).
func (cmt *OptionalCommit) ToCommit() (*Commit, bool) {
	if cmt.Commit == nil {
		return nil, false
	}
	return cmt.Commit, true
}

var _ Rootish = &Commit{}

// NewCommit generates a new Commit object that wraps a supplied datas.Commit.
func NewCommit(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, commit *datas.Commit) (*Commit, error) {
	if commit.IsGhost() {
		return nil, ErrGhostCommitRuntimeFailure
	}

	parents, err := datas.GetCommitParents(ctx, vrw, commit.NomsValue())
	if err != nil {
		return nil, err
	}
	return &Commit{vrw, ns, parents, commit}, nil
}

// NewCommitFromValue generates a new Commit object that wraps a supplied types.Value.
func NewCommitFromValue(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, value types.Value) (*Commit, error) {
	commit, err := datas.CommitFromValue(vrw.Format(), value)
	if err != nil {
		return nil, err
	}
	return NewCommit(ctx, vrw, ns, commit)
}

// HashOf returns the hash of the commit
func (c *Commit) HashOf() (hash.Hash, error) {
	return c.dCommit.Addr(), nil
}

// Value returns the types.Value that backs the commit.
func (c *Commit) Value() types.Value {
	return c.dCommit.NomsValue()
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
	sort.Slice(hashes, func(i, j int) bool {
		return hashes[i].Less(hashes[j])
	})
	return hashes, nil
}

// NumParents gets the number of parents a commit has.
func (c *Commit) NumParents() int {
	return len(c.parents)
}

func (c *Commit) Height() (uint64, error) {
	return c.dCommit.Height(), nil
}

// GetRootValue gets the RootValue of the commit.
func (c *Commit) GetRootValue(ctx context.Context) (RootValue, error) {
	rootV, err := datas.GetCommittedValue(ctx, c.vrw, c.dCommit.NomsValue())
	if err != nil {
		return nil, err
	}
	if rootV == nil {
		return nil, errHasNoRootValue
	}
	return NewRootValue(ctx, c.vrw, c.ns, rootV)
}

func (c *Commit) GetParent(ctx context.Context, idx int) (*OptionalCommit, error) {
	parent := c.parents[idx]
	if parent.IsGhost() {
		return &OptionalCommit{nil, parent.Addr()}, nil
	}

	cmt, err := NewCommit(ctx, c.vrw, c.ns, parent)
	if err != nil {
		return nil, err
	}
	return &OptionalCommit{cmt, parent.Addr()}, nil
}

func (c *Commit) GetCommitClosure(ctx context.Context) (prolly.CommitClosure, error) {
	return getCommitClosure(ctx, c.dCommit, c.vrw, c.ns)
}

func getCommitClosure(ctx context.Context, cmt *datas.Commit, vrw types.ValueReadWriter, ns tree.NodeStore) (prolly.CommitClosure, error) {
	switch v := cmt.NomsValue().(type) {
	case types.SerialMessage:
		return datas.NewParentsClosure(ctx, cmt, v, vrw, ns)
	default:
		return prolly.CommitClosure{}, fmt.Errorf("old format lacks commit closure")
	}
}

var ErrNoCommonAncestor = errors.New("no common ancestor")

func GetCommitAncestor(ctx context.Context, cm1, cm2 *Commit) (*OptionalCommit, error) {
	addr, err := getCommitAncestorAddr(ctx, cm1.dCommit, cm2.dCommit, cm1.vrw, cm2.vrw, cm1.ns, cm2.ns)
	if err != nil {
		return nil, err
	}

	targetCommit, err := datas.LoadCommitAddr(ctx, cm1.vrw, addr)
	if err != nil {
		return nil, err
	}

	if targetCommit.IsGhost() {
		return &OptionalCommit{nil, addr}, nil
	}

	cmt, err := NewCommit(ctx, cm1.vrw, cm1.ns, targetCommit)
	if err != nil {
		return nil, err
	}
	return &OptionalCommit{cmt, addr}, nil
}

func getCommitAncestorAddr(ctx context.Context, c1, c2 *datas.Commit, vrw1, vrw2 types.ValueReadWriter, ns1, ns2 tree.NodeStore) (hash.Hash, error) {
	ancestorAddr, ok, err := datas.FindCommonAncestor(ctx, c1, c2, vrw1, vrw2, ns1, ns2)
	if err != nil {
		return hash.Hash{}, err
	}

	if !ok {
		return hash.Hash{}, ErrNoCommonAncestor
	}

	return ancestorAddr, nil
}

func (c *Commit) CanFastForwardTo(ctx context.Context, new *Commit) (bool, error) {
	optAnc, err := GetCommitAncestor(ctx, c, new)
	if err != nil {
		return false, err
	}

	ancestor, ok := optAnc.ToCommit()
	if !ok {
		return false, fmt.Errorf("Unexpected Ghost Commit")
	}
	if ancestor == nil {
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
	optAnc, err := GetCommitAncestor(ctx, c, new)
	if err != nil {
		return false, err
	}

	ancestor, ok := optAnc.ToCommit()
	if !ok {
		return false, ErrGhostCommitEncountered
	}
	if ancestor == nil {
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

func (c *Commit) GetAncestor(ctx context.Context, as *AncestorSpec) (*OptionalCommit, error) {
	addr, err := c.HashOf()
	if err != nil {
		return nil, err
	}
	optInst := &OptionalCommit{c, addr}
	if as == nil || len(as.Instructions) == 0 {
		return optInst, nil
	}

	hardInst := c
	instructions := as.Instructions
	for _, inst := range instructions {
		if inst >= hardInst.NumParents() {
			return nil, ErrInvalidAncestorSpec
		}

		var err error
		optInst, err = hardInst.GetParent(ctx, inst)
		if err != nil {
			return nil, err
		}

		var ok bool
		hardInst, ok = optInst.ToCommit()
		if !ok {
			break
		}
	}

	return optInst, nil
}

// ResolveRootValue implements Rootish.
func (c *Commit) ResolveRootValue(ctx context.Context) (RootValue, error) {
	return c.GetRootValue(ctx)
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
// |mergeParentCommits| are any merge parents for this commit
// |amend| is a flag which indicates that additional parents should not be added to the provided |mergeParentCommits|.
// |cm| is the metadata for the commit
// The current branch head will be automatically filled in as the first parent at commit time.
func (ddb *DoltDB) NewPendingCommit(
	ctx context.Context,
	roots Roots,
	mergeParentCommits []*Commit,
	amend bool,
	cm *datas.CommitMeta,
) (*PendingCommit, error) {
	newstaged, val, err := ddb.writeRootValue(ctx, roots.Staged)
	if err != nil {
		return nil, err
	}
	roots.Staged = newstaged

	var parents []hash.Hash
	for _, pc := range mergeParentCommits {
		parents = append(parents, pc.dCommit.Addr())
	}

	commitOpts := datas.CommitOptions{Parents: parents, Meta: cm, Amend: amend}
	return &PendingCommit{
		Roots:         roots,
		Val:           val,
		CommitOptions: commitOpts,
	}, nil
}
