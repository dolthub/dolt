package types

import (
	"fmt"
	"strings"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// TypeDesc describes a type of the kind returned by Kind(), e.g. Map, Int32, or a custom type.
type TypeDesc interface {
	Kind() NomsKind
	Equals(other TypeDesc) bool
	Describe() string // For use in tests.
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
// UInt16
// UInt32
// UInt64
// UInt8
// Value
type PrimitiveDesc NomsKind

func (p PrimitiveDesc) Kind() NomsKind {
	return NomsKind(p)
}

func (p PrimitiveDesc) Equals(other TypeDesc) bool {
	return p.Kind() == other.Kind()
}

func (p PrimitiveDesc) Describe() string {
	return KindToString[p.Kind()]
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
	TypeRefKind: "Type",
	UInt16Kind:  "UInt16",
	UInt32Kind:  "UInt32",
	UInt64Kind:  "UInt64",
	UInt8Kind:   "UInt8",
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

func (u UnresolvedDesc) Describe() string {
	panic("Not reachable.")
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

func (c CompoundDesc) Describe() string {
	descElems := func() string {
		out := make([]string, len(c.ElemTypes))
		for i, e := range c.ElemTypes {
			out[i] = e.Describe()
		}
		return strings.Join(out, ", ")
	}
	switch c.kind {
	case ListKind:
		return "List(" + descElems() + ")"
	case MapKind:
		return "Map(" + descElems() + ")"
	case RefKind:
		return "Ref(" + descElems() + ")"
	case SetKind:
		return "Set(" + descElems() + ")"
	case MetaSequenceKind:
		return "Meta(" + descElems() + ")"
	default:
		panic(fmt.Errorf("Kind is not compound: %v", c.kind))
	}
}

// EnumDesc simply lists the identifiers used in this enum.
type EnumDesc struct {
	IDs []string
}

func (e EnumDesc) Kind() NomsKind {
	return EnumKind
}

func (e EnumDesc) Equals(other TypeDesc) bool {
	if e.Kind() != other.Kind() {
		return false
	}
	e2 := other.(EnumDesc)
	if len(e.IDs) != len(e2.IDs) {
		return false
	}
	for i, id := range e2.IDs {
		if id != e.IDs[i] {
			return false
		}
	}
	return true
}

func (e EnumDesc) Describe() string {
	return "enum: { " + strings.Join(e.IDs, "\n") + "}\n"
}

// Choices represents a union, with each choice as a Field..
type Choices []Field

func (u Choices) Describe() (out string) {
	out = "union {\n"
	for _, c := range u {
		out += fmt.Sprintf("  %s :%s\n", c.Name, c.T.Describe())
	}
	return out + "  }"
}

// StructDesc describes a custom Noms Struct.
// Structs can contain at most one anonymous union, so Union may be nil.
type StructDesc struct {
	Fields []Field
	Union  Choices
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

func (s StructDesc) Describe() (out string) {
	if s.Union != nil {
		out += s.Union.Describe()
	}
	for _, f := range s.Fields {
		out += fmt.Sprintf("  %s: %s\n", f.Name, f.T.Describe())
	}
	return
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
