package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// TypeRef structs define and describe Noms types, both custom and built-in.
// Kind and Desc collectively describe any legal Noms type. Kind captures what kind of type the instance describes, e.g. Set, Bool, Map, Struct, etc. Desc captures further information needed to flesh out the definition, such as the type of the elements in a List, or the field names and types of a Struct.
// If Kind refers to a primitive, then Desc is empty.
// If Kind refers to List, Set or Ref, then Desc is a TypeRef describing the element type.
// If Kind refers to Map, then Desc is a 2-element List(TypeRef), describing the key and value types respectively.
// If Kind refers to Struct, then Desc is {"fields": [name, type, ...], "choices": [name, type, ...]}.
// If Kind refers to Enum, then Desc is a List(String) describing the enumerated values.
// Name is optional.
// pkgRef is optional. If set, then pkgRef + name address a type defined in another package.
// TODO Merge this type with parse.TypeRef BUG 338
type TypeRef struct {
	kind   NomsKind
	pkgRef *Ref
	name   String
	desc   Value

	ref ref.Ref
}

func (t TypeRef) Kind() NomsKind {
	return t.kind
}

func (t TypeRef) PackageRef() Ref {
	if t.pkgRef == nil {
		return Ref{}
	}
	return *t.pkgRef
}

func (t TypeRef) Name() String {
	return t.name
}

func (t TypeRef) ElemDesc() TypeRef {
	return t.desc.(TypeRef)
}

// TODO: should return a ListOfString? or just a []String?
func (t TypeRef) EnumDesc() List {
	return t.desc.(List)
}

func (t TypeRef) MapElemDesc() (TypeRef, TypeRef) {
	l := t.desc.(List)
	d.Chk.Equal(uint64(2), l.Len())
	return l.Get(0).(TypeRef), l.Get(1).(TypeRef)
}

func (t TypeRef) StructDesc() (fields, choices List) {
	m := t.desc.(Map)
	f := m.Get(NewString("fields"))
	if f != nil {
		fields = f.(List)
	}
	c := m.Get(NewString("choices"))
	if c != nil {
		choices = c.(List)
	}
	return
}

func (t TypeRef) Ref() ref.Ref {
	return ensureRef(&t.ref, t)
}

func (t TypeRef) Equals(other Value) (res bool) {
	if other, ok := other.(TypeRef); ok {
		return t.Ref() == other.Ref()
	}
	return false
}

func (t TypeRef) Chunks() (out []Future) {
	if t.pkgRef != nil {
		out = append(out, futureFromRef(t.pkgRef.Ref()))
	}
	if t.desc != nil {
		out = append(out, t.desc.Chunks()...)
	}
	return
}

// NomsKind allows a TypeDesc to indicate what kind of type is described.
type NomsKind uint8

// All supported kinds of Noms types are enumerated here.
const (
	BoolKind NomsKind = iota
	UInt8Kind
	UInt16Kind
	UInt32Kind
	UInt64Kind
	Int8Kind
	Int16Kind
	Int32Kind
	Int64Kind
	Float32Kind
	Float64Kind
	StringKind
	BlobKind
	ValueKind
	ListKind
	MapKind
	RefKind
	SetKind
	EnumKind
	StructKind
	TypeRefKind
)

func IsPrimitiveKind(k NomsKind) bool {
	switch k {
	case BoolKind, Int8Kind, Int16Kind, Int32Kind, Int64Kind, Float32Kind, Float64Kind, UInt8Kind, UInt16Kind, UInt32Kind, UInt64Kind, StringKind, BlobKind, ValueKind, TypeRefKind:
		return true
	default:
		return false
	}
}

func MakePrimitiveTypeRef(k NomsKind) TypeRef {
	return buildType("", k, nil)
}

func MakeCompoundTypeRef(name string, kind NomsKind, elemTypes ...TypeRef) TypeRef {
	if len(elemTypes) == 1 {
		d.Chk.NotEqual(MapKind, kind, "MapKind requires 2 element types.")
		return buildType(name, kind, elemTypes[0])
	}
	d.Chk.Equal(MapKind, kind)
	d.Chk.Len(elemTypes, 2, "MapKind requires 2 element types.")
	return buildType(name, kind, NewList(elemTypes[0], elemTypes[1]))
}

func MakeEnumTypeRef(name string, ids ...string) TypeRef {
	vids := make([]Value, len(ids))
	for i, id := range ids {
		vids[i] = NewString(id)
	}
	return buildType(name, EnumKind, NewList(vids...))
}

func MakeStructTypeRef(name string, fields, choices []Field) TypeRef {
	listify := func(fields []Field) List {
		v := make([]Value, 2*len(fields))
		for i, f := range fields {
			v[2*i] = NewString(f.Name)
			v[2*i+1] = f.T
		}
		return NewList(v...)
	}
	desc := NewMap()
	if fields != nil {
		desc = desc.Set(NewString("fields"), listify(fields))
	}
	if choices != nil {
		desc = desc.Set(NewString("choices"), listify(choices))
	}
	return buildType(name, StructKind, desc)
}

// Field represents a Struct field or a Union choice.
// Neither Name nor T is allowed to be a zero-value, though T may be an unresolved TypeRef.
type Field struct {
	Name string
	T    TypeRef
}

func MakeTypeRef(name string, pkg Ref) TypeRef {
	return TypeRef{name: NewString(name), pkgRef: &pkg, kind: ValueKind}
}

func buildType(name string, kind NomsKind, desc Value) TypeRef {
	if IsPrimitiveKind(kind) {
		d.Chk.Nil(desc, "Primitive TypeRefs have no description.")
		return TypeRef{name: NewString(name), kind: kind}
	}
	switch kind {
	case ListKind, RefKind, SetKind, MapKind, EnumKind, StructKind:
		return TypeRef{name: NewString(name), kind: kind, desc: desc}
	default:
		d.Exp.Fail("Unrecognized Kind:", "%v", kind)
		panic("unreachable")
	}
}
