// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"sort"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

const (
	ParentsField = "parents"
	ValueField   = "value"
	MetaField    = "meta"
)

var valueCommitType = makeCommitType(types.ValueType, nil, types.EmptyStructType, nil)

// NewCommit creates a new commit object. The type of Commit is computed based on the type of the value, the type of the meta info as well as the type of the parents.
//
// For the first commit we get:
//
// ```
// struct Commit {
//   meta: M,
//   parents: Set<Ref<Cycle<0>>>,
//   value: T,
// }
// ```
//
// As long as we continue to commit values with type T and meta of type M that type stays the same.
//
// When we later do a commit with value of type U and meta of type N we get:
//
// ```
// struct Commit {
//   meta: N,
//   parents: Set<Ref<struct Commit {
//     meta: M | N,
//     parents: Set<Ref<Cycle<0>>>,
//     value: T | U
//   }>>,
//   value: U,
// }
// ```
//
// Similarly if we do a commit with a different type for the meta info.
//
// The new type gets combined as a union type for the value/meta of the inner commit struct.
func NewCommit(value types.Value, parents types.Set, meta types.Struct) types.Struct {
	t := makeCommitType(value.Type(), valueTypesFromParents(parents, ValueField), meta.Type(), valueTypesFromParents(parents, MetaField))
	return types.NewStructWithType(t, types.ValueSlice{meta, parents, value})
}

// FindCommonAncestor returns the most recent common ancestor of c1 and c2, if
// one exists, setting ok to true. If there is no common ancestor, ok is set
// to false.
func FindCommonAncestor(c1, c2 types.Ref, vr types.ValueReader) (a types.Ref, ok bool) {
	if !IsRefOfCommitType(c1.Type()) {
		d.Panic("FindCommonAncestor() called on %s", c1.Type().Describe())
	}
	if !IsRefOfCommitType(c2.Type()) {
		d.Panic("FindCommonAncestor() called on %s", c2.Type().Describe())
	}

	c1Q, c2Q := &types.RefByHeight{c1}, &types.RefByHeight{c2}
	for !c1Q.Empty() && !c2Q.Empty() {
		c1Ht, c2Ht := c1Q.MaxHeight(), c2Q.MaxHeight()
		if c1Ht == c2Ht {
			c1Parents, c2Parents := c1Q.PopRefsOfHeight(c1Ht), c2Q.PopRefsOfHeight(c2Ht)
			if common := findCommonRef(c1Parents, c2Parents); (common != types.Ref{}) {
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

func findCommonRef(a, b types.RefSlice) types.Ref {
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
			return r
		}
	}
	return types.Ref{}
}

func makeCommitType(valueType *types.Type, parentsValueTypes []*types.Type, metaType *types.Type, parentsMetaTypes []*types.Type) *types.Type {
	tmp := make([]*types.Type, len(parentsValueTypes), len(parentsValueTypes)+1)
	copy(tmp, parentsValueTypes)
	tmp = append(tmp, valueType)
	parentsValueUnionType := types.MakeUnionType(tmp...)

	tmp2 := make([]*types.Type, len(parentsMetaTypes), len(parentsMetaTypes)+1)
	copy(tmp2, parentsMetaTypes)
	tmp2 = append(tmp2, metaType)
	parentsMetaUnionType := types.MakeUnionType(tmp2...)

	fieldNames := []string{MetaField, ParentsField, ValueField}
	var parentsType *types.Type
	if parentsValueUnionType.Equals(valueType) && parentsMetaUnionType.Equals(metaType) {
		parentsType = types.MakeSetType(types.MakeRefType(types.MakeCycleType(0)))
	} else {
		parentsType = types.MakeSetType(types.MakeRefType(
			types.MakeStructType("Commit", fieldNames, []*types.Type{
				parentsMetaUnionType,
				types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
				parentsValueUnionType,
			})))
	}
	fieldTypes := []*types.Type{
		metaType,
		parentsType,
		valueType,
	}

	return types.MakeStructType("Commit", fieldNames, fieldTypes)
}

func valueTypesFromParents(parents types.Set, fieldName string) []*types.Type {
	elemType := getSetElementType(parents.Type())
	switch elemType.Kind() {
	case types.UnionKind:
		ts := []*types.Type{}
		for _, rt := range elemType.Desc.(types.CompoundDesc).ElemTypes {
			ts = append(ts, fieldTypeFromRefOfCommit(rt, fieldName))
		}
		return ts
	default:
		return []*types.Type{fieldTypeFromRefOfCommit(elemType, fieldName)}
	}
}

func getSetElementType(t *types.Type) *types.Type {
	d.PanicIfFalse(t.Kind() == types.SetKind)
	return t.Desc.(types.CompoundDesc).ElemTypes[0]
}

func fieldTypeFromRefOfCommit(t *types.Type, fieldName string) *types.Type {
	return fieldTypeFromCommit(getRefElementType(t), fieldName)
}

func getRefElementType(t *types.Type) *types.Type {
	d.PanicIfFalse(t.Kind() == types.RefKind)
	return t.Desc.(types.CompoundDesc).ElemTypes[0]
}

func fieldTypeFromCommit(t *types.Type, fieldName string) *types.Type {
	d.PanicIfFalse(t.Kind() == types.StructKind && t.Desc.(types.StructDesc).Name == "Commit")
	return t.Desc.(types.StructDesc).Field(fieldName)
}

func IsCommitType(t *types.Type) bool {
	return types.IsSubtype(valueCommitType, t)
}

func IsRefOfCommitType(t *types.Type) bool {
	return t.Kind() == types.RefKind && IsCommitType(getRefElementType(t))
}
