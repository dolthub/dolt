// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "sort"

func makePrimitiveType(k NomsKind) *Type {
	return newType(PrimitiveDesc(k))
}

var BoolType = makePrimitiveType(BoolKind)
var NumberType = makePrimitiveType(NumberKind)
var StringType = makePrimitiveType(StringKind)
var BlobType = makePrimitiveType(BlobKind)
var TypeType = makePrimitiveType(TypeKind)
var ValueType = makePrimitiveType(ValueKind)

func makeCompoundType(kind NomsKind, elemTypes ...*Type) *Type {
	return newType(CompoundDesc{kind, elemTypes})
}

func makeStructTypeQuickly(name string, fields structFields, checkKind checkKindType) *Type {
	t := newType(StructDesc{name, fields})
	if t.HasUnresolvedCycle() {
		t, _ = toUnresolvedType(t, -1, nil)
		resolveStructCycles(t, nil)
		if !t.HasUnresolvedCycle() {
			checkStructType(t, checkKind)
		}
	}
	return t
}

func makeStructType(name string, fields structFields) *Type {
	verifyStructName(name)
	verifyFields(fields)
	return makeStructTypeQuickly(name, fields, checkKindNormalize)
}

func indexOfType(t *Type, tl []*Type) (uint32, bool) {
	for i, tt := range tl {
		if tt == t {
			return uint32(i), true
		}
	}
	return 0, false
}

// Returns a new type where cyclic pointer references are replaced with Cycle<N> types.
func toUnresolvedType(t *Type, level int, parentStructTypes []*Type) (*Type, bool) {
	i, found := indexOfType(t, parentStructTypes)
	if found {
		cycle := CycleDesc(uint32(len(parentStructTypes)) - i - 1)
		return newType(cycle), true // This type is just a placeholder. It doesn't need a real id.
	}

	switch desc := t.Desc.(type) {
	case CompoundDesc:
		ts := make(typeSlice, len(desc.ElemTypes))
		didChange := false
		for i, et := range desc.ElemTypes {
			st, changed := toUnresolvedType(et, level, parentStructTypes)
			ts[i] = st
			didChange = didChange || changed
		}

		if !didChange {
			return t, false
		}

		return newType(CompoundDesc{t.TargetKind(), ts}), true
	case StructDesc:
		fs := make(structFields, len(desc.fields))
		didChange := false
		for i, f := range desc.fields {
			st, changed := toUnresolvedType(f.Type, level+1, append(parentStructTypes, t))
			fs[i] = StructField{f.Name, st, f.Optional}
			didChange = didChange || changed
		}

		if !didChange {
			return t, false
		}

		return newType(StructDesc{desc.Name, fs}), true
	case CycleDesc:
		cycleLevel := int(desc)
		return t, cycleLevel <= level // Only cycles which can be resolved in the current struct.
	}

	return t, false
}

// ToUnresolvedType replaces cycles (by pointer comparison) in types to Cycle types.
func ToUnresolvedType(t *Type) *Type {
	t2, _ := toUnresolvedType(t, 0, nil)
	return t2
}

// Drops cycles and replaces them with pointers to parent structs
func resolveStructCycles(t *Type, parentStructTypes []*Type) *Type {
	switch desc := t.Desc.(type) {
	case CompoundDesc:
		for i, et := range desc.ElemTypes {
			desc.ElemTypes[i] = resolveStructCycles(et, parentStructTypes)
		}

	case StructDesc:
		for i, f := range desc.fields {
			desc.fields[i].Type = resolveStructCycles(f.Type, append(parentStructTypes, t))
		}

	case CycleDesc:
		idx := uint32(desc)
		if idx < uint32(len(parentStructTypes)) {
			return parentStructTypes[uint32(len(parentStructTypes))-1-idx]
		}
	}

	return t
}

// We normalize structs during their construction iff they have no unresolved
// cycles. Normalizing applies a canonical ordering to the composite types of a
// union and serializes all types under the struct. To ensure a consistent
// ordering of the composite types of a union, we generate a unique "order id"
// or OID for each of those types. The OID is the hash of a unique type
// encoding that is independent of the extant order of types within any
// subordinate unions. This encoding for most types is a straightforward
// serialization of its components; for unions the encoding is a bytewise XOR
// of the hashes of each of its composite type encodings.
//
// We require a consistent order of types within a union to ensure that
// equivalent types have a single persistent encoding and, therefore, a single
// hash. The method described above fails for "unrolled" cycles whereby two
// equivalent, but uniquely described structures, would have different OIDs.
// Consider for example the following two types that, while equivalent, do not
// yield the same OID:
//
//   Struct A { a: Cycle<0> }
//   Struct A { a: Struct A { a: Cycle<1> } }
//
// We explicitly disallow this sort of redundantly expressed type. If a
// non-Byzantine use of such a construction arises, we can attempt to simplify
// the expansive type or find another means of comparison.

type checkKindType uint8

const (
	checkKindNormalize checkKindType = iota
	checkKindNoValidate
	checkKindValidate
)

func checkStructType(t *Type, checkKind checkKindType) {
	if checkKind == checkKindNoValidate {
		return
	}

	switch checkKind {
	case checkKindNormalize:
		walkType(t, nil, sortUnions)
	case checkKindValidate:
		walkType(t, nil, validateTypes)
	default:
		panic("unreachable")
	}
}

func sortUnions(t *Type, _ []*Type) {
	if t.TargetKind() == UnionKind {
		sort.Sort(t.Desc.(CompoundDesc).ElemTypes)
	}
}

func validateTypes(t *Type, _ []*Type) {
	switch t.TargetKind() {
	case UnionKind:
		elemTypes := t.Desc.(CompoundDesc).ElemTypes
		if len(elemTypes) == 1 {
			panic("Invalid union type")
		}
		for i := 1; i < len(elemTypes); i++ {
			if !unionLess(elemTypes[i-1], elemTypes[i]) {
				panic("Invalid union order")
			}
		}
	case StructKind:
		desc := t.Desc.(StructDesc)
		verifyStructName(desc.Name)
		verifyFields(desc.fields)
	}
}

func walkType(t *Type, parentStructTypes []*Type, cb func(*Type, []*Type)) {
	if t.TargetKind() == StructKind {
		if _, found := indexOfType(t, parentStructTypes); found {
			return
		}
	}

	cb(t, parentStructTypes)

	switch desc := t.Desc.(type) {
	case CompoundDesc:
		for _, tt := range desc.ElemTypes {
			walkType(tt, parentStructTypes, cb)
		}
	case StructDesc:
		for _, f := range desc.fields {
			walkType(f.Type, append(parentStructTypes, t), cb)
		}
	}
}

// MakeUnionType creates a new union type unless the elemTypes can be folded into a single non union type.
func makeUnionType(elemTypes ...*Type) *Type {
	return makeSimplifiedType(false, elemTypes...)
}

func MakeListType(elemType *Type) *Type {
	return makeCompoundType(ListKind, elemType)
}

func MakeSetType(elemType *Type) *Type {
	return makeCompoundType(SetKind, elemType)
}

func MakeRefType(elemType *Type) *Type {
	return makeCompoundType(RefKind, elemType)
}

func MakeMapType(keyType, valType *Type) *Type {
	return makeCompoundType(MapKind, keyType, valType)
}

type FieldMap map[string]*Type

func MakeStructTypeFromFields(name string, fields FieldMap) *Type {
	fs := make(structFields, len(fields))
	i := 0
	for k, v := range fields {
		fs[i] = StructField{k, v, false}
		i++
	}
	sort.Sort(&fs)
	return makeStructType(name, fs)
}

// StructField describes a field in a struct type.
type StructField struct {
	Name     string
	Type     *Type
	Optional bool
}

type structFields []StructField

func (s structFields) Len() int           { return len(s) }
func (s structFields) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s structFields) Less(i, j int) bool { return s[i].Name < s[j].Name }

func MakeStructType(name string, fields ...StructField) *Type {
	fs := structFields(fields)
	sort.Sort(&fs)

	return makeStructType(name, fs)
}

func MakeUnionType(elemTypes ...*Type) *Type {
	return makeUnionType(elemTypes...)
}

// MakeUnionTypeIntersectStructs is a bit of strange function. It creates a
// simplified union type except for structs, where it creates interesection
// types.
// This function will go away so do not use it!
func MakeUnionTypeIntersectStructs(elemTypes ...*Type) *Type {
	return makeSimplifiedType(true, elemTypes...)
}

func MakeCycleType(level uint32) *Type {
	return newType(CycleDesc(level))
}
