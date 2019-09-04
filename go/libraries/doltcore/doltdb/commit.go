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

	"github.com/liquidata-inc/dolt/go/store/datas"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	metaField      = "meta"
	parentsField   = "parents"
	rootValueField = "value"
)

var errCommitHasNoMeta = errors.New("commit has no metadata")
var errHasNoRootValue = errors.New("no root value")

// Commit contains information on a commit that was written to noms
type Commit struct {
	vrw      types.ValueReadWriter
	commitSt types.Struct
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

func (c *Commit) ParentHashes(ctx context.Context) ([]hash.Hash, error) {
	parentSet, err := c.getParents()

	if err != nil {
		return nil, err
	}

	hashes := make([]hash.Hash, 0, parentSet.Len())
	err = parentSet.IterAll(ctx, func(parentVal types.Value) error {
		parentRef := parentVal.(types.Ref)
		parentHash := parentRef.TargetHash()
		hashes = append(hashes, parentHash)

		return nil
	})

	return hashes, err
}

func (c *Commit) getParents() (types.Set, error) {
	if parVal, found, err := c.commitSt.MaybeGet(parentsField); err != nil {
		return types.EmptySet, err
	} else if found && parVal != nil {
		return parVal.(types.Set), nil
	}

	return types.EmptySet, nil
}

// NumParents gets the number of parents a commit has.
func (c *Commit) NumParents() (int, error) {
	parents, err := c.getParents()

	if err != nil {
		return 0, err
	}

	return int(parents.Len()), nil
}

func (c *Commit) Height() (uint64, error) {
	ref, err := types.NewRef(c.commitSt, c.vrw.Format())
	if err != nil {
		return 0, err
	}
	return ref.Height(), nil
}

func (c *Commit) getParent(ctx context.Context, idx int) (*types.Struct, error) {
	parentSet, err := c.getParents()

	if err != nil {
		return nil, err
	}

	itr, err := parentSet.IteratorAt(ctx, uint64(idx))

	if err != nil {
		return nil, err
	}

	parentVal, err := itr.Next(ctx)

	if err != nil {
		return nil, err
	}

	if parentVal == nil {
		return nil, nil
	}

	parentRef := parentVal.(types.Ref)
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
			return newRootValue(c.vrw, rootSt), nil
		}
	}

	return nil, errHasNoRootValue
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

	ref, err := getCommitAncestorRef(ctx, ref1, ref2, cm1.vrw)

	if err != nil {
		return nil, err
	}

	targetVal, err := ref.TargetValue(ctx, cm1.vrw)

	if err != nil {
		return nil, err
	}

	ancestorSt := targetVal.(types.Struct)

	if err != nil {
		return nil, err
	}

	return &Commit{cm1.vrw, ancestorSt}, nil
}

func getCommitAncestorRef(ctx context.Context, ref1, ref2 types.Ref, vrw types.ValueReadWriter) (types.Ref, error) {
	ancestorRef, ok, err := datas.FindCommonAncestor(ctx, ref1, ref2, vrw)

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
