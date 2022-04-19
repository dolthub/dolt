// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package types contains most of the data structures available to/from Noms.
package types

import (
	"context"

	"github.com/dolthub/dolt/go/store/d"

	"github.com/dolthub/dolt/go/store/hash"
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
func (t *Type) Describe(ctx context.Context) (string, error) {
	return EncodedValue(ctx, t)
}

func (t *Type) TargetKind() NomsKind {
	return t.Desc.Kind()
}

// Value interface
func (t *Type) Value(ctx context.Context) (Value, error) {
	if t.Kind() == UnknownKind {
		return nil, ErrUnknownType
	}

	return t, nil
}

func (t *Type) Equals(other Value) (res bool) {
	// This is highly optimized to not having to encode a *Type unless we have too.
	if t == other {
		return true
	}

	if otherType, ok := other.(*Type); ok {
		h, err := t.Hash(Format_Default)

		// TODO - fix panics
		d.PanicIfError(err)

		oh, err := other.Hash(Format_Default)

		// TODO - fix panics
		d.PanicIfError(err)

		return t.TargetKind() == otherType.TargetKind() && h == oh
	}

	return false
}

func (t *Type) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	res, err := valueCompare(nbf, t, other.(Value))
	if err != nil {
		return false, err
	}

	return res < 0, nil
}

func (t *Type) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(t, nbf)
}

func (t *Type) isPrimitive() bool {
	return true
}

func (t *Type) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := TypeKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	return t.writeToAsType(w, map[string]*Type{}, nbf)
}

func (t *Type) writeToAsType(w nomsWriter, seensStructs map[string]*Type, nbf *NomsBinFormat) error {
	return t.Desc.writeTo(w, nbf, t, seensStructs)
}

func (t *Type) WalkValues(ctx context.Context, cb ValueCallback) error {
	return t.Desc.walkValues(cb)
}

func (t *Type) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (t *Type) typeOf() (*Type, error) {
	return PrimitiveTypeMap[TypeKind], nil
}

func (t *Type) Kind() NomsKind {
	return TypeKind
}

func (t *Type) valueReadWriter() ValueReadWriter {
	return nil
}

// TypeOf returns the type describing the value. This is not an exact type but
// often a simplification of the concrete type.
func TypeOf(v Value) (*Type, error) {
	t, err := v.typeOf()

	if err != nil {
		return nil, err
	}

	return simplifyType(t, false)
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

func (t *Type) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	panic("unreachable")
}

func (t *Type) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	panic("unreachable")
}

func (t *Type) String() string {
	panic("unreachable")
}

func (t *Type) HumanReadableString() string {
	switch typedDesc := t.Desc.(type) {
	case CompoundDesc:
		str := typedDesc.kind.String() + "<"
		for i, et := range typedDesc.ElemTypes {
			if i != 0 {
				str += ","
			}

			str += et.HumanReadableString()
		}
		str += ">"

		return str

	case PrimitiveDesc:
		return typedDesc.Kind().String()

	case StructDesc:
		str := typedDesc.Name + "{"
		for i, f := range typedDesc.fields {
			if i != 0 {
				str += ","
			}
			str += f.Name + " " + f.Type.Desc.Kind().String()
		}
		str += "}"

		return str

	case CycleDesc:
		return string(typedDesc) + "(Cycle)"
	}

	panic("implement type desc in switch")
}
