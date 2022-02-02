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
	"bytes"
	"context"
	"errors"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	metaField        = datas.CommitMetaField
	parentsField     = datas.ParentsField
	parentsListField = datas.ParentsListField
	rootValueField   = datas.ValueField
)

var errCommitHasNoMeta = errors.New("commit has no metadata")
var errHasNoRootValue = errors.New("no root value")

// Commit contains information on a commit that was written to noms
type Commit struct {
	vrw      types.ValueReadWriter
	commitSt types.Struct
	parents  []types.Ref
}

func NewCommit(vrw types.ValueReadWriter, commitSt types.Struct) *Commit {
	parents, err := readParents(vrw, commitSt)
	if err != nil {
		panic(err)
	}
	return &Commit{vrw, commitSt, parents}
}

func readParents(vrw types.ValueReadWriter, commitSt types.Struct) ([]types.Ref, error) {
	if l, found, err := commitSt.MaybeGet(parentsListField); err != nil {
		return nil, err
	} else if found && l != nil {
		l := l.(types.List)
		parents := make([]types.Ref, 0, l.Len())
		i, err := l.Iterator(context.TODO())
		if err != nil {
			return nil, err
		}
		for {
			v, err := i.Next(context.TODO())
			if err != nil {
				return nil, err
			}
			if v == nil {
				break
			}
			parents = append(parents, v.(types.Ref))
		}
		return parents, nil
	}
	if s, found, err := commitSt.MaybeGet(parentsField); err != nil {
		return nil, err
	} else if found && s != nil {
		s := s.(types.Set)
		parents := make([]types.Ref, 0, s.Len())
		i, err := s.Iterator(context.TODO())
		if err != nil {
			return nil, err
		}
		for {
			v, err := i.Next(context.TODO())
			if err != nil {
				return nil, err
			}
			if v == nil {
				break
			}
			parents = append(parents, v.(types.Ref))
		}
		return parents, nil
	}
	return nil, nil
}

// HashOf returns the hash of the commit
func (c *Commit) HashOf() (hash.Hash, error) {
	return c.commitSt.Hash(c.vrw.Format())
}

// GetCommitMeta gets the metadata associated with the commit
func (c *Commit) GetCommitMeta() (*CommitMeta, error) {
	metaVal, found, err := c.commitSt.MaybeGet(metaField)

	if err != nil {
		return nil, err
	}

	if !found {
		return nil, errCommitHasNoMeta
	}

	if metaVal != nil {
		if metaSt, ok := metaVal.(types.Struct); ok {
			cm, err := commitMetaFromNomsSt(metaSt)

			if err == nil {
				return cm, nil
			}
		}
	}

	h, err := c.HashOf()

	if err != nil {
		return nil, errCommitHasNoMeta
	}

	return nil, errors.New(h.String() + " is a commit without the required metadata.")
}

// ParentRefs returns the noms types.Refs for the commits
func (c *Commit) ParentRefs() []types.Ref {
	return c.parents
}

// ParentHashes returns the commit hashes for all parent commits.
func (c *Commit) ParentHashes(ctx context.Context) ([]hash.Hash, error) {
	hashes := make([]hash.Hash, len(c.parents))
	for i, pr := range c.parents {
		hashes[i] = pr.TargetHash()
	}
	return hashes, nil
}

// NumParents gets the number of parents a commit has.
func (c *Commit) NumParents() (int, error) {
	return len(c.parents), nil
}

func (c *Commit) Height() (uint64, error) {
	maxHeight, err := maxChunkHeight(c.commitSt, c.vrw.Format())
	if err != nil {
		return 0, err
	}
	return maxHeight + 1, nil
}

func maxChunkHeight(v types.Value, nbf *types.NomsBinFormat) (max uint64, err error) {
	err = v.WalkRefs(nbf, func(r types.Ref) error {
		if height := r.Height(); height > max {
			max = height
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return max, nil
}

func (c *Commit) getParent(ctx context.Context, idx int) (*types.Struct, error) {
	parentRef := c.parents[idx]
	targVal, err := parentRef.TargetValue(ctx, c.vrw)
	if err != nil {
		return nil, err
	}
	parentSt := targVal.(types.Struct)
	return &parentSt, nil
}

// GetRootValue gets the RootValue of the commit.
func (c *Commit) GetRootValue() (*RootValue, error) {
	rootVal, _, err := c.commitSt.MaybeGet(rootValueField)

	if err != nil {
		return nil, err
	}

	if rootVal != nil {
		if rootSt, ok := rootVal.(types.Struct); ok {
			return newRootValue(c.vrw, rootSt)
		}
	}

	return nil, errHasNoRootValue
}

// GetStRef returns a Noms Ref for this Commit's Noms commit Struct.
func (c *Commit) GetStRef() (types.Ref, error) {
	return types.NewRef(c.commitSt, c.vrw.Format())
}

var ErrNoCommonAncestor = errors.New("no common ancestor")

func GetCommitAncestor(ctx context.Context, cm1, cm2 *Commit) (*Commit, error) {
	ref1, err := types.NewRef(cm1.commitSt, cm1.vrw.Format())

	if err != nil {
		return nil, err
	}

	ref2, err := types.NewRef(cm2.commitSt, cm2.vrw.Format())

	if err != nil {
		return nil, err
	}

	ref, err := getCommitAncestorRef(ctx, ref1, ref2, cm1.vrw, cm2.vrw)

	if err != nil {
		return nil, err
	}

	targetVal, err := ref.TargetValue(ctx, cm1.vrw)

	if err != nil {
		return nil, err
	}

	ancestorSt := targetVal.(types.Struct)
	return NewCommit(cm1.vrw, ancestorSt), nil
}

func getCommitAncestorRef(ctx context.Context, ref1, ref2 types.Ref, vrw1, vrw2 types.ValueReadWriter) (types.Ref, error) {
	ancestorRef, ok, err := datas.FindCommonAncestor(ctx, ref1, ref2, vrw1, vrw2)

	if err != nil {
		return types.Ref{}, err
	}

	if !ok {
		return types.Ref{}, ErrNoCommonAncestor
	}

	return ancestorRef, nil
}

func (c *Commit) CanFastForwardTo(ctx context.Context, new *Commit) (bool, error) {
	ancestor, err := GetCommitAncestor(ctx, c, new)

	if err != nil {
		return false, err
	} else if ancestor == nil {
		return false, errors.New("cannot perform fast forward merge; commits have no common ancestor")
	} else if ancestor.commitSt.Equals(c.commitSt) {
		if ancestor.commitSt.Equals(new.commitSt) {
			return true, ErrUpToDate
		}
		return true, nil
	} else if ancestor.commitSt.Equals(new.commitSt) {
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
	} else if ancestor.commitSt.Equals(new.commitSt) {
		if ancestor.commitSt.Equals(c.commitSt) {
			return true, ErrUpToDate
		}
		return true, nil
	} else if ancestor.commitSt.Equals(c.commitSt) {
		return false, ErrIsBehind
	}

	return false, nil
}

func (c *Commit) GetAncestor(ctx context.Context, as *AncestorSpec) (*Commit, error) {
	ancestorSt, err := getAncestor(ctx, c.vrw, c.commitSt, as)

	if err != nil {
		return nil, err
	}

	return NewCommit(c.vrw, ancestorSt), nil
}

func (c *Commit) DebugString(ctx context.Context) string {
	var buf bytes.Buffer
	err := types.WriteEncodedValue(ctx, &buf, c.commitSt)
	if err != nil {
		panic(err)
	}
	return buf.String()
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
	cm *CommitMeta,
) (*PendingCommit, error) {
	val, err := ddb.writeRootValue(ctx, roots.Staged)
	if err != nil {
		return nil, err
	}

	ds, err := ddb.db.GetDataset(ctx, headRef.String())
	if err != nil {
		return nil, err
	}

	nomsHeadRef, hasHead, err := ds.MaybeHeadRef()
	if err != nil {
		return nil, err
	}

	parents, err := types.NewList(ctx, ddb.vrw)
	if err != nil {
		return nil, err
	}

	parentEditor := parents.Edit()
	if hasHead {
		parentEditor = parentEditor.Append(nomsHeadRef)
	}

	for _, pc := range parentCommits {
		rf, err := types.NewRef(pc.commitSt, ddb.vrw.Format())
		if err != nil {
			return nil, err
		}

		parentEditor = parentEditor.Append(rf)
	}

	parents, err = parentEditor.List(ctx)
	if err != nil {
		return nil, err
	}

	st, err := cm.toNomsStruct(ddb.vrw.Format())
	if err != nil {
		return nil, err
	}

	commitOpts := datas.CommitOptions{ParentsList: parents, Meta: st}
	return &PendingCommit{
		Roots:         roots,
		Val:           val,
		CommitOptions: commitOpts,
	}, nil
}
