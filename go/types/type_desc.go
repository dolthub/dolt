// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "sort"

// TypeDesc describes a type of the kind returned by Kind(), e.g. Map, Number, or a custom type.
type TypeDesc interface {
	Kind() NomsKind
	HasUnresolvedCycle(visited []*Type) bool
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

func (p PrimitiveDesc) HasUnresolvedCycle(visited []*Type) bool {
	return false
}

var KindToString = map[NomsKind]string{
	BlobKind:   "Blob",
	BoolKind:   "Bool",
	CycleKind:  "Cycle",
	ListKind:   "List",
	MapKind:    "Map",
	NumberKind: "Number",
	RefKind:    "Ref",
	SetKind:    "Set",
	StructKind: "Struct",
	StringKind: "String",
	TypeKind:   "Type",
	UnionKind:  "Union",
	ValueKind:  "Value",
}

// CompoundDesc describes a List, Map, Set, Ref, or Union type.
// ElemTypes indicates what type or types are in the container indicated by kind, e.g. Map key and value or Set element.
type CompoundDesc struct {
	kind      NomsKind
	ElemTypes typeSlice
}

func (c CompoundDesc) Kind() NomsKind {
	return c.kind
}

func (c CompoundDesc) HasUnresolvedCycle(visited []*Type) bool {
	for _, t := range c.ElemTypes {
		if t.hasUnresolvedCycle(visited) {
			return true
		}
	}
	return false
}

type TypeMap map[string]*Type

type field struct {
	name string
	t    *Type
}

type fieldSlice []field

func (s fieldSlice) Len() int           { return len(s) }
func (s fieldSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s fieldSlice) Less(i, j int) bool { return s[i].name < s[j].name }

// StructDesc describes a custom Noms Struct.
type StructDesc struct {
	Name   string
	fields []field
}

func (s StructDesc) Kind() NomsKind {
	return StructKind
}

func (s StructDesc) HasUnresolvedCycle(visited []*Type) bool {
	for _, field := range s.fields {
		if field.t.hasUnresolvedCycle(visited) {
			return true
		}
	}
	return false
}

func (s StructDesc) IterFields(cb func(name string, t *Type)) {
	for _, field := range s.fields {
		cb(field.name, field.t)
	}
}

func (s StructDesc) Field(name string) *Type {
	f, i := s.findField(name)
	if i == -1 {
		return nil
	}
	return f.t
}

func (s StructDesc) findField(name string) (*field, int) {
	i := sort.Search(len(s.fields), func(i int) bool { return s.fields[i].name >= name })
	if i == len(s.fields) || s.fields[i].name != name {
		return nil, -1
	}
	return &s.fields[i], i
}

// Len returns the number of fields in the struct
func (s StructDesc) Len() int {
	return len(s.fields)
}

type CycleDesc uint32

func (c CycleDesc) Kind() NomsKind {
	return CycleKind
}

func (c CycleDesc) HasUnresolvedCycle(visited []*Type) bool {
	return true
}

type typeSlice []*Type

func (ts typeSlice) Len() int           { return len(ts) }
func (ts typeSlice) Less(i, j int) bool { return ts[i].oid.Less(*ts[j].oid) }
func (ts typeSlice) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
