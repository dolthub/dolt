package types

import (
	"regexp"
	"sort"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/hash"
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
	h    *hash.Hash
}

var typeForType = makePrimitiveType(TypeKind)

// Describe generate text that should parse into the struct being described.
func (t *Type) Describe() (out string) {
	return EncodedValue(t)
}

func (t *Type) Kind() NomsKind {
	return t.Desc.Kind()
}

func (t *Type) Name() string {
	// TODO: Remove from Type
	d.Chk.IsType(StructDesc{}, t.Desc, "Name only works on Struct types")
	return t.Desc.(StructDesc).Name
}

// Value interface
func (t *Type) Equals(other Value) (res bool) {
	return other != nil && t.Hash() == other.Hash()
}

func (t *Type) Less(other Value) (res bool) {
	return valueLess(t, other)
}

func (t *Type) Hash() hash.Hash {
	return EnsureHash(t.h, t)
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

func (t *Type) Chunks() (chunks []Ref) {
	return
}

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

func MakeStructType(name string, fields map[string]*Type) *Type {
	for fn := range fields {
		verifyFieldName(fn)
	}
	return buildType(StructDesc{name, fields})
}

var fieldNameRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)

func verifyFieldName(name string) {
	d.Exp.True(fieldNameRe.MatchString(name), "Invalid struct field name: %s", name)
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

type unionTypes []*Type

func (uts unionTypes) Len() int           { return len(uts) }
func (uts unionTypes) Less(i, j int) bool { return uts[i].Hash().Less(uts[j].Hash()) }
func (uts unionTypes) Swap(i, j int)      { uts[i], uts[j] = uts[j], uts[i] }

// MakeUnionType creates a new union type unless the elemTypes can be folded into a single non union type.
func MakeUnionType(elemTypes ...*Type) *Type {
	seenTypes := map[hash.Hash]bool{}
	ts := flattenUnionTypes(elemTypes, &seenTypes)
	if len(ts) == 1 {
		return ts[0]
	}
	sort.Sort(unionTypes(ts))
	return buildType(CompoundDesc{UnionKind, ts})
}

func flattenUnionTypes(ts []*Type, seenTypes *map[hash.Hash]bool) []*Type {
	if len(ts) == 0 {
		return ts
	}

	ts2 := make([]*Type, 0, len(ts))
	for _, t := range ts {
		if t.Kind() == UnionKind {
			ts2 = append(ts2, flattenUnionTypes(t.Desc.(CompoundDesc).ElemTypes, seenTypes)...)
		} else {
			if !(*seenTypes)[t.Hash()] {
				(*seenTypes)[t.Hash()] = true
				ts2 = append(ts2, t)
			}
		}
	}
	return ts2
}

func buildType(desc TypeDesc) *Type {
	return &Type{Desc: desc, h: &hash.Hash{}}
}

var NumberType = makePrimitiveType(NumberKind)
var BoolType = makePrimitiveType(BoolKind)
var StringType = makePrimitiveType(StringKind)
var BlobType = makePrimitiveType(BlobKind)
var TypeType = makePrimitiveType(TypeKind)
var ValueType = makePrimitiveType(ValueKind)
