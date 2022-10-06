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

package types

import (
	"github.com/dolthub/dolt/go/gen/fb/serial"
)

// NomsKind allows a TypeDesc to indicate what kind of type is described.
type NomsKind uint8

// All supported kinds of Noms types are enumerated here.
// The ordering of these (especially Bool, Float and String) is important for ordering of values.
const (
	BoolKind NomsKind = iota
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
	InlineBlobKind
	TimestampKind
	DecimalKind
	JSONKind
	GeometryKind
	PointKind
	LineStringKind
	PolygonKind

	SerialMessageKind

	MultiPointKind
	MultiLineStringKind
	MultiPolygonKind
	GeometryCollectionKind

	UnknownKind NomsKind = 255
)

var KindToType = make([]Value, 255)
var SupportedKinds = make([]bool, 255)

func init() {
	KindToType[BlobKind] = Blob{}
	KindToType[BoolKind] = Bool(false)
	KindToType[ListKind] = List{}
	KindToType[MapKind] = Map{}
	KindToType[FloatKind] = Float(0)
	KindToType[RefKind] = Ref{}
	KindToType[SetKind] = Set{}
	KindToType[StructKind] = Struct{}
	KindToType[StringKind] = String("")
	KindToType[TypeKind] = &Type{}
	KindToType[UUIDKind] = UUID{}
	KindToType[IntKind] = Int(0)
	KindToType[UintKind] = Uint(0)
	KindToType[NullKind] = NullValue
	KindToType[TupleKind] = Tuple{}
	KindToType[InlineBlobKind] = InlineBlob{}
	KindToType[TimestampKind] = Timestamp{}
	KindToType[DecimalKind] = Decimal{}
	KindToType[JSONKind] = JSON{}
	KindToType[GeometryKind] = Geometry{}
	KindToType[PointKind] = Point{}
	KindToType[LineStringKind] = LineString{}
	KindToType[PolygonKind] = Polygon{}
	KindToType[MultiPointKind] = MultiPoint{}
	KindToType[SerialMessageKind] = SerialMessage{}

	SupportedKinds[BlobKind] = true
	SupportedKinds[BoolKind] = true
	SupportedKinds[CycleKind] = true
	SupportedKinds[ListKind] = true
	SupportedKinds[MapKind] = true
	SupportedKinds[FloatKind] = true
	SupportedKinds[RefKind] = true
	SupportedKinds[SetKind] = true
	SupportedKinds[StructKind] = true
	SupportedKinds[StringKind] = true
	SupportedKinds[TypeKind] = true
	SupportedKinds[UnionKind] = true
	SupportedKinds[ValueKind] = true
	SupportedKinds[UUIDKind] = true
	SupportedKinds[IntKind] = true
	SupportedKinds[UintKind] = true
	SupportedKinds[NullKind] = true
	SupportedKinds[TupleKind] = true
	SupportedKinds[InlineBlobKind] = true
	SupportedKinds[TimestampKind] = true
	SupportedKinds[DecimalKind] = true
	SupportedKinds[JSONKind] = true
	SupportedKinds[GeometryKind] = true
	SupportedKinds[PointKind] = true
	SupportedKinds[LineStringKind] = true
	SupportedKinds[PolygonKind] = true
	SupportedKinds[MultiPointKind] = true
	SupportedKinds[SerialMessageKind] = true

	if serial.MessageTypesKind != int(SerialMessageKind) {
		panic("internal error: serial.MessageTypesKind != SerialMessageKind")
	}

	// assert that all kinds are the right number
	if int(BoolKind) != 0 {
		panic("internal error: BoolKind != 0")
	}
	if int(FloatKind) != 1 {
		panic("internal error: FloatKind != 1")
	}
	if int(StringKind) != 2 {
		panic("internal error: StringKind != 2")
	}
	if int(BlobKind) != 3 {
		panic("internal error: BlobKind != 3")
	}
	if int(ValueKind) != 4 {
		panic("internal error: ValueKind != 4")
	}
	if int(ListKind) != 5 {
		panic("internal error: ListKind != 5")
	}
	if int(MapKind) != 6 {
		panic("internal error: MapKind != 6")
	}
	if int(RefKind) != 7 {
		panic("internal error: RefKind != 7")
	}
	if int(SetKind) != 8 {
		panic("internal error: SetKind != 8")
	}
	if int(StructKind) != 9 {
		panic("internal error: StructKind != 9")
	}
	if int(CycleKind) != 10 {
		panic("internal error: CycleKind != 10")
	}
	if int(TypeKind) != 11 {
		panic("internal error: TypeKind != 11")
	}
	if int(UnionKind) != 12 {
		panic("internal error: UnionKind != 12")
	}
	if int(hashKind) != 13 {
		panic("internal error: hashKind != 13")
	}
	if int(UUIDKind) != 14 {
		panic("internal error: UUIDKind != 14")
	}
	if int(IntKind) != 15 {
		panic("internal error: IntKind != 15")
	}
	if int(UintKind) != 16 {
		panic("internal error: UintKind != 16")
	}
	if int(NullKind) != 17 {
		panic("internal error: NullKind != 17")
	}
	if int(TupleKind) != 18 {
		panic("internal error: TupleKind != 18")
	}
	if int(InlineBlobKind) != 19 {
		panic("internal error: InlineBlobKind != 19")
	}
	if int(TimestampKind) != 20 {
		panic("internal error: TimestampKind != 20")
	}
	if int(DecimalKind) != 21 {
		panic("internal error: DecimalKind != 21")
	}
	if int(JSONKind) != 22 {
		panic("internal error: JSONKind != 22")
	}
	if int(GeometryKind) != 23 {
		panic("internal error: GeometryKind != 23")
	}
	if int(PointKind) != 24 {
		panic("internal error: PointKind != 24")
	}
	if int(LineStringKind) != 25 {
		panic("internal error: LineStringKind != 25")
	}
	if int(PolygonKind) != 26 {
		panic("internal error: PolygonKind != 26")
	}
	if int(SerialMessageKind) != 27 {
		panic("internal error: SerialMessageKind != 27")
	}
	if int(MultiPointKind) != 28 {
		panic("internal error: MultiPointKind != 28")
	}
	if int(MultiLineStringKind) != 29 {
		panic("internal error: MultiPointKind != 29")
	}
	if int(MultiPolygonKind) != 30 {
		panic("internal error: MultiPointKind != 30")
	}
	if int(GeometryCollectionKind) != 31 {
		panic("internal error: MultiPointKind != 31")
	}
	if int(UnknownKind) != 255 {
		panic("internal error: UnknownKind != 255")
	}
}

var KindToTypeSlice []Value

var KindToString = map[NomsKind]string{
	UnknownKind:       "unknown",
	BlobKind:          "Blob",
	BoolKind:          "Bool",
	CycleKind:         "Cycle",
	ListKind:          "List",
	MapKind:           "Map",
	FloatKind:         "Float",
	RefKind:           "Ref",
	SetKind:           "Set",
	StructKind:        "Struct",
	StringKind:        "String",
	TypeKind:          "Type",
	UnionKind:         "Union",
	ValueKind:         "Value",
	UUIDKind:          "UUID",
	IntKind:           "Int",
	UintKind:          "Uint",
	NullKind:          "Null",
	TupleKind:         "Tuple",
	InlineBlobKind:    "InlineBlob",
	TimestampKind:     "Timestamp",
	DecimalKind:       "Decimal",
	JSONKind:          "JSON",
	GeometryKind:      "Geometry",
	PointKind:         "Point",
	LineStringKind:    "LineString",
	PolygonKind:       "Polygon",
	MultiPointKind:    "MultiPoint",
	SerialMessageKind: "SerialMessage",
}

// String returns the name of the kind.
func (k NomsKind) String() string {
	return KindToString[k]
}

// IsPrimitiveKind returns true if k represents a Noms primitive type, which excludes collections (List, Map, Set), Refs, Structs, Symbolic and Unresolved types.
func IsPrimitiveKind(k NomsKind) bool {
	i := int(k)
	return i < len(PrimitiveKindMask) && PrimitiveKindMask[i]
}

// isKindOrderedByValue determines if a value is ordered by its value instead of its hash.
func isKindOrderedByValue(k NomsKind) bool {
	return k <= StringKind || k >= UUIDKind
}

func IsGeometryKind(k NomsKind) bool {
	switch k {
	case PointKind,
		LineStringKind,
		PolygonKind,
		MultiPointKind,
		GeometryKind:
		return true
	default:
		return false
	}
}

func (k NomsKind) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	if k == UnknownKind {
		return ErrUnknownType
	}

	w.writeUint8(uint8(k))
	return nil
}
