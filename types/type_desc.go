package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// TypeDesc describes a type of the kind returned by Kind(), e.g. Map, Int32, or a custom type.
type TypeDesc interface {
	Kind() NomsKind
	Equals(other TypeDesc) bool
}

// PrimitiveDesc implements TypeDesc for all primitive Noms types:
// Blob
// Bool
// Float32
// Float64
// Int16
// Int32
// Int64
// Int8
// Package
// String
// Type
// Uint16
// Uint32
// Uint64
// Uint8
// Value
type PrimitiveDesc NomsKind

func (p PrimitiveDesc) Kind() NomsKind {
	return NomsKind(p)
}

func (p PrimitiveDesc) Equals(other TypeDesc) bool {
	return p.Kind() == other.Kind()
}

var KindToString = map[NomsKind]string{
	BlobKind:    "Blob",
	BoolKind:    "Bool",
	Float32Kind: "Float32",
	Float64Kind: "Float64",
	Int16Kind:   "Int16",
	Int32Kind:   "Int32",
	Int64Kind:   "Int64",
	Int8Kind:    "Int8",
	ListKind:    "List",
	MapKind:     "Map",
	PackageKind: "Package",
	RefKind:     "Ref",
	SetKind:     "Set",
	StringKind:  "String",
	TypeKind:    "Type",
	Uint16Kind:  "Uint16",
	Uint32Kind:  "Uint32",
	Uint64Kind:  "Uint64",
	Uint8Kind:   "Uint8",
	ValueKind:   "Value",
}

func primitiveToDesc(p string) PrimitiveDesc {
	for k, v := range KindToString {
		if p == v {
			d.Chk.True(IsPrimitiveKind(k), "Kind must be primitive, not %s", KindToString[k])
			return PrimitiveDesc(k)
		}
	}
	d.Chk.Fail("Tried to create PrimitiveDesc from bad string", "%s", p)
	panic("Unreachable")
}

type UnresolvedDesc struct {
	pkgRef  ref.Ref
	ordinal int16
}

func (u UnresolvedDesc) Kind() NomsKind {
	return UnresolvedKind
}

func (u UnresolvedDesc) Equals(other TypeDesc) bool {
	if other, ok := other.(UnresolvedDesc); ok {
		return u.pkgRef == other.pkgRef && u.ordinal == other.ordinal
	}
	return false
}

// CompoundDesc describes a List, Map, Set or Ref type.
// ElemTypes indicates what type or types are in the container indicated by kind, e.g. Map key and value or Set element.
type CompoundDesc struct {
	kind      NomsKind
	ElemTypes []Type
}

func (c CompoundDesc) Kind() NomsKind {
	return c.kind
}

func (c CompoundDesc) Equals(other TypeDesc) bool {
	if c.Kind() != other.Kind() {
		return false
	}
	for i, e := range other.(CompoundDesc).ElemTypes {
		if !e.Equals(c.ElemTypes[i]) {
			return false
		}
	}
	return true
}

// StructDesc describes a custom Noms Struct.
// Structs can contain at most one anonymous union, so Union may be nil.
type StructDesc struct {
	Fields []Field
	Union  []Field
}

func (s StructDesc) Kind() NomsKind {
	return StructKind
}

func (s StructDesc) Equals(other TypeDesc) bool {
	if s.Kind() != other.Kind() || len(s.Fields) != len(other.(StructDesc).Fields) {
		return false
	}
	for i, f := range other.(StructDesc).Fields {
		if !s.Fields[i].Equals(f) {
			return false
		}
	}
	for i, f := range other.(StructDesc).Union {
		if !s.Union[i].Equals(f) {
			return false
		}
	}
	return true
}

// Field represents a Struct field or a Union choice.
// Neither Name nor T is allowed to be a zero-value, though T may be an unresolved Type.
type Field struct {
	Name     string
	T        Type
	Optional bool
}

func (f Field) Equals(other Field) bool {
	return f.Name == other.Name && f.Optional == other.Optional && f.T.Equals(other.T)
}
