// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"sort"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/nomdl"
	"github.com/attic-labs/noms/go/types"
)

const (
	ParentsField = "parents"
	ValueField   = "value"
	MetaField    = "meta"
	commitName   = "Commit"
)

var commitTemplate = types.MakeStructTemplate(commitName, []string{MetaField, ParentsField, ValueField})

var valueCommitType = nomdl.MustParseType(`Struct Commit {
        meta: Struct {},
        parents: Set<Ref<Cycle<Commit>>>,
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
//   value: T,
// }
// ```
// where M is a struct type and T is any type.
func NewCommit(value types.Value, parents types.Set, meta types.Struct) types.Struct {
	return commitTemplate.NewStruct([]types.Value{meta, parents, value})
}

// FindCommonAncestor returns the most recent common ancestor of c1 and c2, if
// one exists, setting ok to true. If there is no common ancestor, ok is set
// to false.
func FindCommonAncestor(c1, c2 types.Ref, vr types.ValueReader) (a types.Ref, ok bool) {
	if !IsRefOfCommitType(types.TypeOf(c1)) {
		d.Panic("FindCommonAncestor() called on %s", types.TypeOf(c1).Describe())
	}
	if !IsRefOfCommitType(types.TypeOf(c2)) {
		d.Panic("FindCommonAncestor() called on %s", types.TypeOf(c2).Describe())
	}

	c1Q, c2Q := &types.RefByHeight{c1}, &types.RefByHeight{c2}
	for !c1Q.Empty() && !c2Q.Empty() {
		c1Ht, c2Ht := c1Q.MaxHeight(), c2Q.MaxHeight()
		if c1Ht == c2Ht {
			c1Parents, c2Parents := c1Q.PopRefsOfHeight(c1Ht), c2Q.PopRefsOfHeight(c2Ht)
			if common, ok := findCommonRef(c1Parents, c2Parents); ok {
				return common, true
			}
			parentsToQueue(c1Parents, c1Q, vr)
			parentsToQueue(c2Parents, c2Q, vr)
		} else if c1Ht > c2Ht {
			parentsToQueue(c1Q.PopRefsOfHeight(c1Ht), c1Q, vr)
		} else {
			parentsToQueue(c2Q.PopRefsOfHeight(c2Ht), c2Q, vr)
		}
	}
	return
}

func parentsToQueue(refs types.RefSlice, q *types.RefByHeight, vr types.ValueReader) {
	for _, r := range refs {
		c := r.TargetValue(vr).(types.Struct)
		p := c.Get(ParentsField).(types.Set)
		p.IterAll(func(v types.Value) {
			q.PushBack(v.(types.Ref))
		})
	}
	sort.Sort(q)
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

func makeCommitStructType(metaType, parentsType, valueType *types.Type) *types.Type {
	return types.MakeStructType("Commit",
		types.StructField{
			Name: MetaField,
			Type: metaType,
		},
		types.StructField{
			Name: ParentsField,
			Type: parentsType,
		},
		types.StructField{
			Name: ValueField,
			Type: valueType,
		},
	)
}

func getRefElementType(t *types.Type) *types.Type {
	d.PanicIfFalse(t.TargetKind() == types.RefKind)
	return t.Desc.(types.CompoundDesc).ElemTypes[0]
}

func IsCommitType(t *types.Type) bool {
	return types.IsSubtype(valueCommitType, t)
}

func IsCommit(v types.Value) bool {
	return types.IsValueSubtypeOf(v, valueCommitType)
}

func IsRefOfCommitType(t *types.Type) bool {
	return t.TargetKind() == types.RefKind && IsCommitType(getRefElementType(t))
}
