package types

import "fmt"

// TypeDesc describes a type of the kind returned by Kind(), e.g. Map, Number, or a custom type.
type TypeDesc interface {
	Kind() NomsKind
	Equals(other TypeDesc) bool
}

// PrimitiveDesc implements TypeDesc for all primitive Noms types:
// Blob
// Bool
// Number
// Package
// String
// Type
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
	ListKind:    "List",
	MapKind:     "Map",
	NumberKind:  "Number",
	RefKind:     "Ref",
	SetKind:     "Set",
	StringKind:  "String",
	TypeKind:    "Type",
	ValueKind:   "Value",
	BackRefKind: "BackRef",
}

// CompoundDesc describes a List, Map, Set or Ref type.
// ElemTypes indicates what type or types are in the container indicated by kind, e.g. Map key and value or Set element.
type CompoundDesc struct {
	kind      NomsKind
	ElemTypes []*Type
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
	Name   string
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
	T        *Type
	Optional bool
}

func (f Field) Equals(other Field) bool {
	return f.Name == other.Name && f.Optional == other.Optional && f.T.Equals(other.T)
}

// BackRefDesc is used to symbolize back references in recursive struct types
type BackRefDesc uint8

func (b BackRefDesc) Kind() NomsKind {
	return BackRefKind
}

func (b BackRefDesc) Equals(other TypeDesc) bool {
	return b.Kind() == other.Kind() && other.(BackRefDesc) == b
}

func (b BackRefDesc) Describe() string {
	return fmt.Sprintf("%s(%d)", KindToString[b.Kind()], b)
}
