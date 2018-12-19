// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package types contains most of the data structures available to/from Noms.
package types

import (
	"github.com/attic-labs/noms/go/hash"
)

// Type defines and describes Noms types, both built-in and user-defined.
// Desc provides the composition of the type. It may contain only a types.NomsKind, in the case of
//     primitives, or it may contain additional information -- e.g. element Types for compound type
//     specializations, field descriptions for structs, etc. Either way, checking Kind() allows code
//     to understand how to interpret the rest of the data.
// If Kind() refers to a primitive, then Desc has no more info.
// If Kind() refers to List, Map, Ref, Set, or Union, then Desc is a list of Types describing the element type(s).
// If Kind() refers to Struct, then Desc contains a []field.

type Type struct {
	Desc TypeDesc
}

func newType(desc TypeDesc) *Type {
	return &Type{desc}
}

// Describe generate text that should parse into the struct being described.
func (t *Type) Describe() (out string) {
	return EncodedValue(t)
}

func (t *Type) TargetKind() NomsKind {
	return t.Desc.Kind()
}

// Value interface
func (t *Type) Value() Value {
	return t
}

func (t *Type) Equals(other Value) (res bool) {
	// This is highly optimized to not having to encode a *Type unless we have too.
	if t == other {
		return true
	}

	if otherType, ok := other.(*Type); ok {
		return t.TargetKind() == otherType.TargetKind() && t.Hash() == other.Hash()
	}

	return false
}

func (t *Type) Less(other Value) (res bool) {
	return valueLess(t, other)
}

func (t *Type) Hash() hash.Hash {
	return getHash(t)
}

func (t *Type) writeTo(w nomsWriter) {
	TypeKind.writeTo(w)
	t.writeToAsType(w, map[string]*Type{})
}

func (t *Type) writeToAsType(w nomsWriter, seensStructs map[string]*Type) {
	t.Desc.writeTo(w, t, seensStructs)
}

func (t *Type) WalkValues(cb ValueCallback) {
	t.Desc.walkValues(cb)
}

func (t *Type) WalkRefs(cb RefCallback) {
	return
}

func (t *Type) typeOf() *Type {
	return TypeType
}

func (t *Type) Kind() NomsKind {
	return TypeKind
}

func (t *Type) valueReadWriter() ValueReadWriter {
	return nil
}

// TypeOf returns the type describing the value. This is not an exact type but
// often a simplification of the concrete type.
func TypeOf(v Value) *Type {
	return simplifyType(v.typeOf(), false)
}

// HasStructCycles determines if the type contains any struct cycles.
func HasStructCycles(t *Type) bool {
	return hasStructCycles(t, nil)
}

func hasStructCycles(t *Type, visited []*Type) bool {
	if _, found := indexOfType(t, visited); found {
		return true
	}

	switch desc := t.Desc.(type) {
	case CompoundDesc:
		for _, et := range desc.ElemTypes {
			b := hasStructCycles(et, visited)
			if b {
				return true
			}
		}

	case StructDesc:
		for _, f := range desc.fields {
			b := hasStructCycles(f.Type, append(visited, t))
			if b {
				return true
			}
		}

	case CycleDesc:
		panic("unexpected unresolved cycle")
	}

	return false
}

func indexOfType(t *Type, tl []*Type) (uint32, bool) {
	for i, tt := range tl {
		if tt == t {
			return uint32(i), true
		}
	}
	return 0, false
}
