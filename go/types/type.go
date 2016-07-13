// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"regexp"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
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
	Desc          TypeDesc
	h             *hash.Hash
	id            uint32
	serialization []byte
}

const initialTypeBufferSize = 128

func newType(desc TypeDesc, id uint32) *Type {
	t := &Type{desc, &hash.Hash{}, id, nil}
	if !t.HasUnresolvedCycle() {
		serializeType(t)
	}
	return t
}

func serializeType(t *Type) {
	w := &binaryNomsWriter{make([]byte, initialTypeBufferSize), 0}
	enc := newValueEncoder(w, nil)
	enc.writeType(t, nil)
	t.serialization = w.data()
}

// Describe generate text that should parse into the struct being described.
func (t *Type) Describe() (out string) {
	return EncodedValue(t)
}

func (t *Type) Kind() NomsKind {
	return t.Desc.Kind()
}

func (t *Type) Name() string {
	// TODO: Remove from Type
	return t.Desc.(StructDesc).Name
}

func (t *Type) hasUnresolvedCycle(visited []*Type) bool {
	_, found := indexOfType(t, visited)
	if found {
		return false
	}

	return t.Desc.HasUnresolvedCycle(append(visited, t))
}

func (t *Type) HasUnresolvedCycle() bool {
	return t.hasUnresolvedCycle(nil)
}

// Value interface
func (t *Type) Equals(other Value) (res bool) {
	return other != nil && t.Hash() == other.Hash()
}

func (t *Type) Less(other Value) (res bool) {
	return valueLess(t, other)
}

func (t *Type) Hash() hash.Hash {
	if t.h.IsEmpty() {
		*t.h = getHash(t)
	}

	return *t.h
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
	return TypeType
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

var fieldNameComponentRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*`)
var fieldNameRe = regexp.MustCompile(fieldNameComponentRe.String() + "$")

func verifyFieldNames(names []string) {
	if len(names) == 0 {
		return
	}

	last := names[0]
	verifyFieldName(last)

	for i := 1; i < len(names); i++ {
		verifyFieldName(names[i])
		if names[i] <= last {
			d.Chk.Fail("Field names must be unique and ordered alphabetically")
		}
		last = names[i]
	}
}

func verifyName(name, kind string) {
	d.PanicIfTrue(!fieldNameRe.MatchString(name), `Invalid struct%s name: "%s"`, kind, name)
}

func verifyFieldName(name string) {
	verifyName(name, " field")
}

func verifyStructName(name string) {
	if name != "" {
		verifyName(name, "")
	}
}
