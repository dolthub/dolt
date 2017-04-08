// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"

	"github.com/attic-labs/noms/go/d"
)

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

func makeStructTypeQuickly(name string, fields structTypeFields) *Type {
	return newType(StructDesc{name, fields})
}

func makeStructType(name string, fields structTypeFields) *Type {
	verifyStructName(name)
	verifyFields(fields)
	return makeStructTypeQuickly(name, fields)
}

func indexOfType(t *Type, tl []*Type) (uint32, bool) {
	for i, tt := range tl {
		if tt == t {
			return uint32(i), true
		}
	}
	return 0, false
}

func validateType(t *Type) {
	validateTypeImpl(t, map[string]struct{}{})
}

func validateTypeImpl(t *Type, seenStructs map[string]struct{}) {
	switch desc := t.Desc.(type) {
	case CompoundDesc:
		if desc.Kind() == UnionKind {
			if len(desc.ElemTypes) == 1 {
				panic("Invalid union type")
			}
			for i := 1; i < len(desc.ElemTypes); i++ {
				if !unionLess(desc.ElemTypes[i-1], desc.ElemTypes[i]) {
					panic("Invalid union order")
				}
			}
		}

		for _, et := range desc.ElemTypes {
			validateTypeImpl(et, seenStructs)
		}
	case StructDesc:
		if desc.Name != "" {
			if _, ok := seenStructs[desc.Name]; ok {
				return
			}
			seenStructs[desc.Name] = struct{}{}
		}
		verifyStructName(desc.Name)
		verifyFields(desc.fields)
		for _, f := range desc.fields {
			validateTypeImpl(f.Type, seenStructs)
		}
	}
}

// MakeUnionType creates a new union type unless the elemTypes can be folded into a single non union type.
func makeUnionType(elemTypes ...*Type) *Type {
	return simplifyType(makeCompoundType(UnionKind, elemTypes...), false)
}

func MakeListType(elemType *Type) *Type {
	return simplifyType(makeCompoundType(ListKind, elemType), false)
}

func MakeSetType(elemType *Type) *Type {
	return simplifyType(makeCompoundType(SetKind, elemType), false)
}

func MakeRefType(elemType *Type) *Type {
	return simplifyType(makeCompoundType(RefKind, elemType), false)
}

func MakeMapType(keyType, valType *Type) *Type {
	return simplifyType(makeCompoundType(MapKind, keyType, valType), false)
}

type FieldMap map[string]*Type

func MakeStructTypeFromFields(name string, fields FieldMap) *Type {
	fs := make(structTypeFields, len(fields))
	i := 0
	for k, v := range fields {
		fs[i] = StructField{k, v, false}
		i++
	}
	sort.Sort(fs)
	return simplifyType(makeStructType(name, fs), false)
}

// StructField describes a field in a struct type.
type StructField struct {
	Name     string
	Type     *Type
	Optional bool
}

type structTypeFields []StructField

func (s structTypeFields) Len() int           { return len(s) }
func (s structTypeFields) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s structTypeFields) Less(i, j int) bool { return s[i].Name < s[j].Name }

func MakeStructType(name string, fields ...StructField) *Type {
	fs := structTypeFields(fields)
	sort.Sort(fs)
	return simplifyType(makeStructType(name, fs), false)
}

func MakeUnionType(elemTypes ...*Type) *Type {
	return makeUnionType(elemTypes...)
}

// MakeUnionTypeIntersectStructs is a bit of strange function. It creates a
// simplified union type except for structs, where it creates interesection
// types.
// This function will go away so do not use it!
func MakeUnionTypeIntersectStructs(elemTypes ...*Type) *Type {
	return simplifyType(makeCompoundType(UnionKind, elemTypes...), true)
}

func MakeCycleType(name string) *Type {
	d.PanicIfTrue(name == "")
	return newType(CycleDesc(name))
}
