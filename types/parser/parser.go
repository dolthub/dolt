package main

import (
	"fmt"
	"strings"
)

type NomsKind int

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
	UnionKind
	EnumKind
	NamedKind
	StructKind
)

type Type interface {
	Kind() NomsKind
	Describe() string
}

type PrimitiveType NomsKind

func (p PrimitiveType) Kind() NomsKind {
	return NomsKind(p)
}

func (p PrimitiveType) Describe() string {
	for k, v := range PrimitiveToKind {
		if p == v {
			return k
		}
	}
	panic("wha")
}

var PrimitiveToKind = map[string]PrimitiveType{
	"Bool":    PrimitiveType(BoolKind),
	"UInt64":  PrimitiveType(UInt64Kind),
	"UInt32":  PrimitiveType(UInt32Kind),
	"UInt16":  PrimitiveType(UInt16Kind),
	"UInt8":   PrimitiveType(UInt8Kind),
	"Int64":   PrimitiveType(Int64Kind),
	"Int32":   PrimitiveType(Int32Kind),
	"Int16":   PrimitiveType(Int16Kind),
	"Int8":    PrimitiveType(Int8Kind),
	"Float64": PrimitiveType(Float64Kind),
	"Float32": PrimitiveType(Float32Kind),
	"String":  PrimitiveType(StringKind),
	"Blob":    PrimitiveType(BlobKind),
	"Value":   PrimitiveType(ValueKind),
}

type Package struct {
	packageName       string
	aliases           map[string]string
	usingDeclarations []CompoundType
	structs           map[string]StructType
	enums             map[string]EnumType
}

type CompoundType struct {
	kind      NomsKind
	elemTypes []Type
}

type EnumType struct {
	name string
	ids  []string
}

type NamedType struct {
	name string
}

type UnionType struct {
	choices []Field
}

type Field struct {
	name string
	t    Type
}

type StructType struct {
	name   string
	fields []Field
	union  UnionType
}

func (e StructType) Kind() NomsKind {
	return StructKind
}

func (s StructType) Describe() (out string) {
	out = fmt.Sprintf("Custom type %s\n", s.name)
	if len(s.union.choices) != 0 {
		out += fmt.Sprintf("  anon %s\n", s.union.Describe())
	}
	for _, f := range s.fields {
		out += fmt.Sprintf("  %s: %s\n", f.name, f.t.Describe())
	}
	return
}

func (e EnumType) Kind() NomsKind {
	return EnumKind
}

func (e EnumType) Describe() string {
	return "Enum: " + strings.Join(e.ids, ", ")
}

func (n NamedType) Kind() NomsKind {
	return NamedKind
}

func (n NamedType) Describe() string {
	return "Struct " + n.name
}

func (n UnionType) Kind() NomsKind {
	return UnionKind
}

func (u UnionType) Describe() (out string) {
	out = "Union of {\n"
	for _, c := range u.choices {
		out += fmt.Sprintf("  %s, %s\n", c.name, c.t.Describe())
	}
	return out + "  }"
}

func (c CompoundType) Kind() NomsKind {
	return c.kind
}

func (c CompoundType) Describe() string {
	descElems := func() (out string) {
		for _, e := range c.elemTypes {
			out += fmt.Sprintf("%v (%T); ", e, e)
		}
		return out
	}
	switch c.kind {
	case ListKind:
		return "List of " + descElems()
	case MapKind:
		return "Map of " + descElems()
	case RefKind:
		return "Ref of " + descElems()
	case SetKind:
		return "Set of " + descElems()
	default:
		panic("ffuu")
	}
}

type Alias struct {
	name   string
	target string
}
