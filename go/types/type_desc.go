// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"
)

// TypeDesc describes a type of the kind returned by Kind(), e.g. Map, Number, or a custom type.
type TypeDesc interface {
	Kind() NomsKind
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

// CompoundDesc describes a List, Map, Set, Ref, or Union type.
// ElemTypes indicates what type or types are in the container indicated by kind, e.g. Map key and value or Set element.
type CompoundDesc struct {
	kind      NomsKind
	ElemTypes typeSlice
}

func (c CompoundDesc) Kind() NomsKind {
	return c.kind
}

type TypeMap map[string]*Type

// StructDesc describes a custom Noms Struct.
type StructDesc struct {
	Name   string
	fields structTypeFields
}

func (s StructDesc) Kind() NomsKind {
	return StructKind
}

func (s StructDesc) IterFields(cb func(name string, t *Type, optional bool)) {
	for _, field := range s.fields {
		cb(field.Name, field.Type, field.Optional)
	}
}

func (s StructDesc) Field(name string) (typ *Type, optional bool) {
	f, i := s.findField(name)
	if i == -1 {
		return nil, false
	}
	return f.Type, f.Optional
}

func (s StructDesc) findField(name string) (*StructField, int) {
	i := sort.Search(len(s.fields), func(i int) bool { return s.fields[i].Name >= name })
	if i == len(s.fields) || s.fields[i].Name != name {
		return nil, -1
	}
	return &s.fields[i], i
}

// Len returns the number of fields in the struct
func (s StructDesc) Len() int {
	return len(s.fields)
}

type CycleDesc string

func (c CycleDesc) Kind() NomsKind {
	return CycleKind
}

type typeSlice []*Type

func (ts typeSlice) Len() int { return len(ts) }

func (ts typeSlice) Less(i, j int) bool {
	return unionLess(ts[i], ts[j])
}

func (ts typeSlice) Swap(i, j int) { ts[i], ts[j] = ts[j], ts[i] }

// unionLess is used for sorting union types in a predictable order as well as
// validating the order when reading union types from a chunk.
func unionLess(ti, tj *Type) bool {
	if ti == tj {
		panic("unreachable") // unions must not contain the same type twice.
	}

	ki, kj := ti.TargetKind(), tj.TargetKind()
	if ki == kj {
		switch ki {
		case StructKind:
			// Due to type simplification, the only thing that matters is the name of the struct.
			return ti.Desc.(StructDesc).Name < tj.Desc.(StructDesc).Name
		case CycleKind:
			return ti.Desc.(CycleDesc) < tj.Desc.(CycleDesc)
		default:
			panic("unreachable") // We should have folded all other types into one.
		}
	}
	return ki < kj
}
