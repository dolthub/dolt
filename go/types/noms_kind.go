// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

// NomsKind allows a TypeDesc to indicate what kind of type is described.
type NomsKind uint8

// All supported kinds of Noms types are enumerated here.
// The ordering of these (especially Bool, Number and String) is important for ordering of values.
const (
	BoolKind NomsKind = iota
	NumberKind
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
)

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

// String returns the name of the kind.
func (k NomsKind) String() string {
	return KindToString[k]
}

// IsPrimitiveKind returns true if k represents a Noms primitive type, which excludes collections (List, Map, Set), Refs, Structs, Symbolic and Unresolved types.
func IsPrimitiveKind(k NomsKind) bool {
	switch k {
	case BoolKind, NumberKind, StringKind, BlobKind, ValueKind, TypeKind:
		return true
	default:
		return false
	}
}

// isKindOrderedByValue determines if a value is ordered by its value instead of its hash.
func isKindOrderedByValue(k NomsKind) bool {
	return k <= StringKind
}

func (k NomsKind) writeTo(w nomsWriter) {
	w.writeUint8(uint8(k))
}
