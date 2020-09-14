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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"
	"sort"

	"github.com/liquidata-inc/dolt/go/store/d"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/nomdl"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	ParentsField = "parents"
	// Added in July, 2020. Commits created with versions before this was
	// added have only a Set of parents. Commits created after this was
	// added carry a List of parents, because parent order can matter.
	// `"parents"` is still written as a Set as well, so that commits
	// created with newer versions of still usable by older versions.
	ParentsListField = "parents_list"
	ValueField       = "value"
	CommitMetaField  = "meta"
	CommitName       = "Commit"
)

var commitTemplate = types.MakeStructTemplate(CommitName, []string{CommitMetaField, ParentsField, ParentsListField, ValueField})

var valueCommitType = nomdl.MustParseType(`Struct Commit {
        meta: Struct {},
        parents: Set<Ref<Cycle<Commit>>>,
        parents_list?: List<Ref<Cycle<Commit>>>,
        value: Value,
}`)

// NewCommit creates a new commit object.
//
// A commit has the following type:
//
// ```
// struct Commit {
//   meta: M,
//   parents: Set<Ref<Cycle<Commit>>>,
//   parentsList: List<Ref<Cycle<Commit>>>,
//   value: T,
// }
// ```
// where M is a struct type and T is any type.
func NewCommit(ctx context.Context, value types.Value, parentsList types.List, meta types.Struct) (types.Struct, error) {
	parentsSet, err := parentsList.ToSet(ctx)
	if err != nil {
		return types.EmptyStruct(meta.Format()), err
	}
	return commitTemplate.NewStruct(meta.Format(), []types.Value{meta, parentsSet, parentsList, value})
}

// FindCommonAncestor returns the most recent common ancestor of c1 and c2, if
// one exists, setting ok to true. If there is no common ancestor, ok is set
// to false. Refs of |c1| are dereferenced through |vr1|, while refs of |c2|
// are dereference through |vr2|.
func FindCommonAncestor(ctx context.Context, c1, c2 types.Ref, vr1, vr2 types.ValueReader) (a types.Ref, ok bool, err error) {
	t1, err := types.TypeOf(c1)

	if err != nil {
		return types.Ref{}, false, err
	}

	// precondition checks
	if !IsRefOfCommitType(c1.Format(), t1) {
		d.Panic("first reference is not a commit")
	}

	t2, err := types.TypeOf(c2)

	if err != nil {
		return types.Ref{}, false, err
	}

	if !IsRefOfCommitType(c2.Format(), t2) {
		d.Panic("second reference is not a commit")
	}

	c1Q, c2Q := &types.RefByHeight{c1}, &types.RefByHeight{c2}
	for !c1Q.Empty() && !c2Q.Empty() {
		c1Ht, c2Ht := c1Q.MaxHeight(), c2Q.MaxHeight()
		if c1Ht == c2Ht {
			c1Parents, c2Parents := c1Q.PopRefsOfHeight(c1Ht), c2Q.PopRefsOfHeight(c2Ht)
			if common, ok := findCommonRef(c1Parents, c2Parents); ok {
				return common, true, nil
			}
			parentsToQueue(ctx, c1Parents, c1Q, vr1)
			parentsToQueue(ctx, c2Parents, c2Q, vr2)
		} else if c1Ht > c2Ht {
			parentsToQueue(ctx, c1Q.PopRefsOfHeight(c1Ht), c1Q, vr1)
		} else {
			parentsToQueue(ctx, c2Q.PopRefsOfHeight(c2Ht), c2Q, vr2)
		}
	}

	return a, ok, nil
}

func parentsToQueue(ctx context.Context, refs types.RefSlice, q *types.RefByHeight, vr types.ValueReader) error {
	seen := make(map[hash.Hash]bool)
	for _, r := range refs {
		if _, ok := seen[r.TargetHash()]; ok {
			continue
		}
		seen[r.TargetHash()] = true

		v, err := r.TargetValue(ctx, vr)

		if err != nil {
			return err
		}

		c := v.(types.Struct)
		ps, ok, err := c.MaybeGet(ParentsListField)
		if err != nil {
			return err
		}
		if ok {
			p := ps.(types.List)
			err = p.IterAll(ctx, func(v types.Value, _ uint64) error {
				q.PushBack(v.(types.Ref))
				return nil
			})
		} else {
			ps, ok, err := c.MaybeGet(ParentsField)
			if err != nil {
				return err
			}
			if ok {
				p := ps.(types.Set)
				err = p.IterAll(ctx, func(v types.Value) error {
					q.PushBack(v.(types.Ref))
					return nil
				})
			}
		}
	}

	sort.Sort(q)
	return nil
}

func findCommonRef(a, b types.RefSlice) (types.Ref, bool) {
	toRefSet := func(s types.RefSlice) map[hash.Hash]types.Ref {
		out := map[hash.Hash]types.Ref{}
		for _, r := range s {
			out[r.TargetHash()] = r
		}
		return out
	}

	aSet, bSet := toRefSet(a), toRefSet(b)
	for s, r := range aSet {
		if _, present := bSet[s]; present {
			return r, true
		}
	}
	return types.Ref{}, false
}

func makeCommitStructType(metaType, parentsType, parentsListType, valueType *types.Type) (*types.Type, error) {
	return types.MakeStructType(CommitName,
		types.StructField{
			Name: CommitMetaField,
			Type: metaType,
		},
		types.StructField{
			Name: ParentsField,
			Type: parentsType,
		},
		types.StructField{
			Name: ParentsListField,
			Type: parentsListType,
		},
		types.StructField{
			Name: ValueField,
			Type: valueType,
		},
	)
}

func getRefElementType(t *types.Type) *types.Type {
	// precondition checks
	d.PanicIfFalse(t.TargetKind() == types.RefKind)

	return t.Desc.(types.CompoundDesc).ElemTypes[0]
}

func IsCommitType(nbf *types.NomsBinFormat, t *types.Type) bool {
	return types.IsSubtype(nbf, valueCommitType, t)
}

func IsCommit(v types.Value) (bool, error) {
	if s, ok := v.(types.Struct); !ok {
		return false, nil
	} else {
		return types.IsValueSubtypeOf(s.Format(), v, valueCommitType)
	}
}

func IsRefOfCommitType(nbf *types.NomsBinFormat, t *types.Type) bool {
	return t.TargetKind() == types.RefKind && IsCommitType(nbf, getRefElementType(t))
}
