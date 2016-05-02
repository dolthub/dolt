package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// Type defines and describes Noms types, both custom and built-in.
// Desc provides more details of the type. It may contain only a types.NomsKind, in the case of
//     primitives, or it may contain additional information -- e.g. element Types for compound type
//     specializations, field descriptions for structs, etc. Either way, checking Kind() allows code
//     to understand how to interpret the rest of the data.
// If Kind() refers to a primitive, then Desc has no more info.
// If Kind() refers to List, Map, Set or Ref, then Desc is a list of Types describing the element type(s).
// If Kind() refers to Struct, then Desc contains a []Field.

type Type struct {
	Desc TypeDesc
	ref  *ref.Ref
}

// Describe generate text that should parse into the struct being described.
func (t *Type) Describe() (out string) {
	return WriteHRS(t)
}

func (t *Type) Kind() NomsKind {
	return t.Desc.Kind()
}

func (t *Type) IsOrdered() bool {
	switch t.Desc.Kind() {
	case NumberKind, StringKind, RefKind:
		return true
	default:
		return false
	}
}

func (t *Type) Name() string {
	// TODO: Remove from Type
	d.Chk.IsType(StructDesc{}, t.Desc, "Name only works on Struct types")
	return t.Desc.(StructDesc).Name
}

func (t *Type) Ref() ref.Ref {
	return EnsureRef(t.ref, t)
}

func (t *Type) Equals(other Value) (res bool) {
	return other != nil && t.Ref() == other.Ref()
}

func (t *Type) Chunks() (chunks []Ref) {
	return
}

func (t *Type) ChildValues() (res []Value) {
	switch desc := t.Desc.(type) {
	case CompoundDesc:
		for _, t := range desc.ElemTypes {
			res = append(res, t)
		}
	case StructDesc:
		desc.IterFields(func(name string, t *Type) {
			res = append(res, t)
		})
	case PrimitiveDesc:
		// Nothing, these have no child values
	default:
		d.Chk.Fail("Unexpected type desc implementation: %#v", t)
	}
	return
}

var typeForType = makePrimitiveType(TypeKind)

func (t *Type) Type() *Type {
	return typeForType
}

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

func makePrimitiveType(k NomsKind) *Type {
	return buildType(PrimitiveDesc(k))
}

func MakePrimitiveTypeByString(p string) *Type {
	switch p {
	case "Bool":
		return BoolType
	case "Number":
		return NumberType
	case "String":
		return StringType
	case "Blob":
		return BlobType
	case "Value":
		return ValueType
	case "Type":
		return TypeType
	}
	d.Chk.Fail("invalid type string: %s", p)
	return nil
}

func makeCompoundType(kind NomsKind, elemTypes ...*Type) *Type {
	if len(elemTypes) == 1 {
		d.Chk.NotEqual(MapKind, kind, "MapKind requires 2 element types.")
		d.Chk.True(kind == RefKind || kind == ListKind || kind == SetKind)
	} else {
		d.Chk.Equal(MapKind, kind)
		d.Chk.Len(elemTypes, 2, "MapKind requires 2 element types.")
	}
	return buildType(CompoundDesc{kind, elemTypes})
}

func MakeStructType(name string, fields map[string]*Type) *Type {
	return buildType(StructDesc{name, fields})
}

func MakeListType(elemType *Type) *Type {
	return buildType(CompoundDesc{ListKind, []*Type{elemType}})
}

func MakeSetType(elemType *Type) *Type {
	return buildType(CompoundDesc{SetKind, []*Type{elemType}})
}

func MakeMapType(keyType, valType *Type) *Type {
	return buildType(CompoundDesc{MapKind, []*Type{keyType, valType}})
}

func MakeRefType(elemType *Type) *Type {
	return buildType(CompoundDesc{RefKind, []*Type{elemType}})
}

func buildType(desc TypeDesc) *Type {
	return &Type{Desc: desc, ref: &ref.Ref{}}
}

var NumberType = makePrimitiveType(NumberKind)
var BoolType = makePrimitiveType(BoolKind)
var StringType = makePrimitiveType(StringKind)
var BlobType = makePrimitiveType(BlobKind)
var TypeType = makePrimitiveType(TypeKind)
var ValueType = makePrimitiveType(ValueKind)
