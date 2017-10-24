// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"

	"github.com/attic-labs/noms/go/d"
)

func MakePrimitiveType(k NomsKind) *Type {
	switch k {
	case BoolKind:
		return BoolType
	case NumberKind:
		return NumberType
	case StringKind:
		return StringType
	case BlobKind:
		return BlobType
	case ValueKind:
		return ValueType
	case TypeKind:
		return TypeType
	}
	d.Chk.Fail("invalid NomsKind: %d", k)
	return nil
}

// MakeUnionType creates a new union type unless the elemTypes can be folded into a single non union type.
func MakeUnionType(elemTypes ...*Type) *Type {
	return simplifyType(makeUnionType(elemTypes...), false)
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

func MakeStructType(name string, fields ...StructField) *Type {
	fs := structTypeFields(fields)
	sort.Sort(fs)
	return simplifyType(makeStructType(name, fs), false)
}

// MakeUnionTypeIntersectStructs is a bit of strange function. It creates a
// simplified union type except for structs, where it creates interesection
// types.
// This function will go away so do not use it!
func MakeUnionTypeIntersectStructs(elemTypes ...*Type) *Type {
	return simplifyType(makeUnionType(elemTypes...), true)
}

func MakeCycleType(name string) *Type {
	d.PanicIfTrue(name == "")
	return newType(CycleDesc(name))
}

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

func makeUnionType(elemTypes ...*Type) *Type {
	if len(elemTypes) == 1 {
		return elemTypes[0]
	}
	return makeCompoundType(UnionKind, elemTypes...)
}

func makeStructTypeQuickly(name string, fields structTypeFields) *Type {
	return newType(StructDesc{name, fields})
}

func makeStructType(name string, fields structTypeFields) *Type {
	verifyStructName(name)
	verifyFields(fields)
	return makeStructTypeQuickly(name, fields)
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
