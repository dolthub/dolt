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

// NomsKind allows a TypeDesc to indicate what kind of type is described.
type NomsKind uint8

// All supported kinds of Noms types are enumerated here.
// The ordering of these (especially Bool, Float and String) is important for ordering of values.
const (
	BoolKind    NomsKind = iota
	FloatKind
	StringKind
	BlobKind
	ValueKind
	ListKind
	MapKind
	RefKind
	SetKind

	// Keep StructKind and CycleKind together.
	StructKind
	CycleKind

	TypeKind
	UnionKind

	// Internal to decoder
	hashKind

	UUIDKind
	IntKind
	UintKind
	NullKind
	TupleKind

	UnknownKind NomsKind = 255
)

var SupportedKinds = map[NomsKind]struct{}{
	BoolKind:   {},
	FloatKind:  {},
	StringKind: {},
	BlobKind:   {},
	ValueKind:  {},
	ListKind:   {},
	MapKind:    {},
	RefKind:    {},
	SetKind:    {},
	StructKind: {},
	CycleKind:  {},
	TypeKind:   {},
	UnionKind:  {},
	hashKind:   {},
	UUIDKind:   {},
	IntKind:    {},
	UintKind:   {},
	NullKind:   {},
	TupleKind:  {},
}

var KindToString = map[NomsKind]string{
	UnknownKind: "unknown",
	BlobKind:    "Blob",
	BoolKind:    "Bool",
	CycleKind:   "Cycle",
	ListKind:    "List",
	MapKind:     "Map",
	FloatKind:   "Float",
	RefKind:     "Ref",
	SetKind:     "Set",
	StructKind:  "Struct",
	StringKind:  "String",
	TypeKind:    "Type",
	UnionKind:   "Union",
	ValueKind:   "Value",
	UUIDKind:    "UUID",
	IntKind:     "Int",
	UintKind:    "Uint",
	NullKind:    "Null",
	TupleKind:   "Tuple",
}

// String returns the name of the kind.
func (k NomsKind) String() string {
	return KindToString[k]
}

// IsPrimitiveKind returns true if k represents a Noms primitive type, which excludes collections (List, Map, Set), Refs, Structs, Symbolic and Unresolved types.
func IsPrimitiveKind(k NomsKind) bool {
	switch k {
	case BoolKind, FloatKind, IntKind, UintKind, StringKind, BlobKind, UUIDKind, ValueKind, TypeKind, NullKind:
		return true
	default:
		return false
	}
}

// isKindOrderedByValue determines if a value is ordered by its value instead of its hash.
func isKindOrderedByValue(k NomsKind) bool {
	return k <= StringKind || k >= UUIDKind
}

func (k NomsKind) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	if k == UnknownKind {
		return ErrUnknownType
	}

	w.writeUint8(uint8(k))
	return nil
}
