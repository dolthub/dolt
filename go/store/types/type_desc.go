// Copyright 2019 Liquidata, Inc.
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

package types

import (
	"sort"
)

// TypeDesc describes a type of the kind returned by Kind(), e.g. Map, Float, or a custom type.
type TypeDesc interface {
	Kind() NomsKind
	walkValues(cb ValueCallback) error
	writeTo(w nomsWriter, nbf *NomsBinFormat, t *Type, seenStructs map[string]*Type) error

	// isSimplifiedForSure is used to determine if the type should be
	// simplified. It may contain false negatives.
	isSimplifiedForSure() bool
	isSimplifiedInner() bool
}

// PrimitiveDesc implements TypeDesc for all primitive Noms types:
// Blob
// Bool
// Float
// String
// Type
// Value
// UUID
// Int
// Uint
type PrimitiveDesc NomsKind

func (p PrimitiveDesc) Kind() NomsKind {
	return NomsKind(p)
}

func (p PrimitiveDesc) walkValues(cb ValueCallback) error {
	return nil
}

func (p PrimitiveDesc) writeTo(w nomsWriter, nbf *NomsBinFormat, t *Type, seenStructs map[string]*Type) error {
	return NomsKind(p).writeTo(w, nbf)
}

func (p PrimitiveDesc) isSimplifiedForSure() bool {
	return true
}

func (p PrimitiveDesc) isSimplifiedInner() bool {
	return true
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

func (c CompoundDesc) walkValues(cb ValueCallback) error {
	for _, t := range c.ElemTypes {
		err := cb(t)

		if err != nil {
			return err
		}
	}

	return nil
}

func (c CompoundDesc) writeTo(w nomsWriter, nbf *NomsBinFormat, t *Type, seenStructs map[string]*Type) error {
	err := c.kind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	if c.kind == UnionKind {
		w.writeCount(uint64(len(c.ElemTypes)))
	}

	for _, t := range c.ElemTypes {
		err := t.writeToAsType(w, seenStructs, nbf)

		if err != nil {
			return err
		}
	}

	return nil
}

func (c CompoundDesc) isSimplifiedForSure() bool {
	if c.kind == UnionKind {
		return len(c.ElemTypes) == 0
	}

	for _, t := range c.ElemTypes {
		if !t.Desc.isSimplifiedInner() {
			return false
		}
	}
	return true
}

func (c CompoundDesc) isSimplifiedInner() bool {
	return c.isSimplifiedForSure()
}

// StructDesc describes a custom Noms Struct.
type StructDesc struct {
	Name   string
	fields structTypeFields
}

func (s StructDesc) Kind() NomsKind {
	return StructKind
}

func (s StructDesc) walkValues(cb ValueCallback) error {
	for _, field := range s.fields {
		err := cb(field.Type)

		if err != nil {
			return err
		}
	}

	return nil
}

func (s StructDesc) writeTo(w nomsWriter, nbf *NomsBinFormat, t *Type, seenStructs map[string]*Type) error {
	name := s.Name

	if name != "" {
		if _, ok := seenStructs[name]; ok {
			err := CycleKind.writeTo(w, nbf)

			if err != nil {
				return err
			}

			w.writeString(name)
			return nil
		}
		seenStructs[name] = t
	}

	err := StructKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeString(name)
	w.writeCount(uint64(s.Len()))

	// Write all names, all types and finally all the optional flags.
	for _, field := range s.fields {
		w.writeString(field.Name)
	}
	for _, field := range s.fields {
		err := field.Type.writeToAsType(w, seenStructs, nbf)

		if err != nil {
			return err
		}
	}
	for _, field := range s.fields {
		w.writeBool(field.Optional)
	}

	return nil
}

func (s StructDesc) isSimplifiedForSure() bool {
	for _, f := range s.fields {
		if !f.Type.Desc.isSimplifiedInner() {
			return false
		}
	}
	return true
}

func (s StructDesc) isSimplifiedInner() bool {
	// We do not try to to determine if a type is simplified if it contains a struct.
	return false
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

func (c CycleDesc) walkValues(cb ValueCallback) error {
	return nil
}

func (c CycleDesc) writeTo(w nomsWriter, nbf *NomsBinFormat, t *Type, seenStruct map[string]*Type) error {
	panic("Should not write cycle types")
}

func (c CycleDesc) isSimplifiedForSure() bool {
	return false
}

func (c CycleDesc) isSimplifiedInner() bool {
	return false
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
