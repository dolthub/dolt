// Copyright 2021 Dolthub, Inc.
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

package val

import (
	"bytes"
	"encoding/binary"
	"math"
)

type Type struct {
	Enc      Encoding
	Coll     Collation
	Nullable bool
}

type ByteSize uint16

const (
	int8Size    ByteSize = 1
	uint8Size   ByteSize = 1
	int16Size   ByteSize = 2
	uint16Size  ByteSize = 2
	int24Size   ByteSize = 3
	uint24Size  ByteSize = 3
	int32Size   ByteSize = 4
	uint32Size  ByteSize = 4
	int64Size   ByteSize = 8
	uint64Size  ByteSize = 8
	float32Size ByteSize = 4
	float64Size ByteSize = 8
)

type Collation uint16

const (
	ByteOrderCollation Collation = 0
)

type Encoding uint8

// Constant Size Encodings
const (
	NullEnc    Encoding = 0
	Int8Enc    Encoding = 1
	Uint8Enc   Encoding = 2
	Int16Enc   Encoding = 3
	Uint16Enc  Encoding = 4
	Int24Enc   Encoding = 5
	Uint24Enc  Encoding = 6
	Int32Enc   Encoding = 7
	Uint32Enc  Encoding = 8
	Int64Enc   Encoding = 9
	Uint64Enc  Encoding = 10
	Float32Enc Encoding = 11
	Float64Enc Encoding = 12

	// TODO
	//  TimeEnc
	//  TimestampEnc
	//  DateEnc
	//  TimeEnc
	//  DatetimeEnc
	//  YearEnc

	sentinel Encoding = 127
)

// Variable Size Encodings
const (
	StringEnc Encoding = 128
	BytesEnc  Encoding = 129

	// TODO
	//  DecimalEnc
	//  BitEnc
	//  CharEnc
	//  VarCharEnc
	//  TextEnc
	//  BinaryEnc
	//  VarBinaryEnc
	//  BlobEnc
	//  JSONEnc
	//  EnumEnc
	//  SetEnc
	//  ExpressionEnc
	//  GeometryEnc
)

func readBool(val []byte) bool {
	expectSize(val, int8Size)
	return val[0] == 1
}
func readInt8(val []byte) int8 {
	expectSize(val, int8Size)
	return int8(val[0])
}

func readUint8(val []byte) uint8 {
	expectSize(val, uint8Size)
	return val[0]
}

func readInt16(val []byte) int16 {
	expectSize(val, int16Size)
	return int16(binary.LittleEndian.Uint16(val))
}

func readUint16(val []byte) uint16 {
	expectSize(val, uint16Size)
	return binary.LittleEndian.Uint16(val)
}

func readInt32(val []byte) int32 {
	expectSize(val, int32Size)
	return int32(binary.LittleEndian.Uint32(val))
}

func readUint32(val []byte) uint32 {
	expectSize(val, uint32Size)
	return binary.LittleEndian.Uint32(val)
}

func readInt64(val []byte) int64 {
	expectSize(val, int64Size)
	return int64(binary.LittleEndian.Uint64(val))
}

func readUint64(val []byte) uint64 {
	expectSize(val, uint64Size)
	return binary.LittleEndian.Uint64(val)
}

func readFloat32(val []byte) float32 {
	expectSize(val, float32Size)
	return math.Float32frombits(readUint32(val))
}

func readFloat64(val []byte) float64 {
	expectSize(val, float64Size)
	return math.Float64frombits(readUint64(val))
}

func readString(val []byte, coll Collation) string {
	// todo(andy): fix allocation
	return string(val)
}

func readBytes(val []byte, coll Collation) []byte {
	return val
}

func writeBool(buf []byte, val bool) {
	expectSize(buf, 1)
	if val {
		buf[0] = byte(1)
	} else {
		buf[0] = byte(0)
	}
}

func writeInt8(buf []byte, val int8) {
	expectSize(buf, int8Size)
	buf[0] = byte(val)
}

func writeUint8(buf []byte, val uint8) {
	expectSize(buf, uint8Size)
	buf[0] = byte(val)
}

func writeInt16(buf []byte, val int16) {
	expectSize(buf, int16Size)
	binary.LittleEndian.PutUint16(buf, uint16(val))
}

func writeUint16(buf []byte, val uint16) {
	expectSize(buf, uint16Size)
	binary.LittleEndian.PutUint16(buf, val)
}

func writeInt32(buf []byte, val int32) {
	expectSize(buf, int32Size)
	binary.LittleEndian.PutUint32(buf, uint32(val))
}

func writeUint32(buf []byte, val uint32) {
	expectSize(buf, uint32Size)
	binary.LittleEndian.PutUint32(buf, val)
}

func writeInt64(buf []byte, val int64) {
	expectSize(buf, int64Size)
	binary.LittleEndian.PutUint64(buf, uint64(val))
}

func writeUint64(buf []byte, val uint64) {
	expectSize(buf, uint64Size)
	binary.LittleEndian.PutUint64(buf, val)
}

func writeFloat32(buf []byte, val float32) {
	expectSize(buf, float32Size)
	binary.LittleEndian.PutUint32(buf, math.Float32bits(val))
}

func writeFloat64(buf []byte, val float64) {
	expectSize(buf, float64Size)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(val))
}

func writeString(buf []byte, val string, coll Collation) {
	expectSize(buf, ByteSize(len(val)))
	copy(buf, val)
}

func writeBytes(buf, val []byte, coll Collation) {
	expectSize(buf, ByteSize(len(val)))
	copy(buf, val)
}

func expectSize(buf []byte, sz ByteSize) {
	if ByteSize(len(buf)) != sz {
		panic("byte slice is not of expected size")
	}
}

func compare(typ Type, left, right []byte) int {
	// order NULLs last
	if left == nil {
		if right == nil {
			return 0
		} else {
			return 1
		}
	} else if right == nil {
		if left == nil {
			return 0
		} else {
			return -1
		}
	}

	switch typ.Enc {
	case Int8Enc:
		return compareInt8(readInt8(left), readInt8(right))
	case Uint8Enc:
		return compareUint8(readUint8(left), readUint8(right))
	case Int16Enc:
		return compareInt16(readInt16(left), readInt16(right))
	case Uint16Enc:
		return compareUint16(readUint16(left), readUint16(right))
	case Int24Enc:
		panic("24 bit")
	case Uint24Enc:
		panic("24 bit")
	case Int32Enc:
		return compareInt32(readInt32(left), readInt32(right))
	case Uint32Enc:
		return compareUint32(readUint32(left), readUint32(right))
	case Int64Enc:
		return compareInt64(readInt64(left), readInt64(right))
	case Uint64Enc:
		return compareUint64(readUint64(left), readUint64(right))
	case Float32Enc:
		return compareFloat32(readFloat32(left), readFloat32(right))
	case Float64Enc:
		return compareFloat64(readFloat64(left), readFloat64(right))
	case StringEnc:
		return compareString(readString(left, typ.Coll), readString(right, typ.Coll), typ.Coll)
	case BytesEnc:
		return compareBytes(readBytes(left, typ.Coll), readBytes(right, typ.Coll), typ.Coll)
	default:
		panic("unknown encoding")
	}
}

// false is less that true
func compareBool(l, r bool) int {
	if l == r {
		return 0
	}
	if !l && r {
		return -1
	}
	return 1
}

func compareInt8(l, r int8) int {
	if l == r {
		return 0
	} else if l < r {
		return -1
	} else {
		return 1
	}
}

func compareUint8(l, r uint8) int {
	if l == r {
		return 0
	} else if l < r {
		return -1
	} else {
		return 1
	}
}

func compareInt16(l, r int16) int {
	if l == r {
		return 0
	} else if l < r {
		return -1
	} else {
		return 1
	}
}

func compareUint16(l, r uint16) int {
	if l == r {
		return 0
	} else if l < r {
		return -1
	} else {
		return 1
	}
}

func compareInt32(l, r int32) int {
	if l == r {
		return 0
	} else if l < r {
		return -1
	} else {
		return 1
	}
}

func compareUint32(l, r uint32) int {
	if l == r {
		return 0
	} else if l < r {
		return -1
	} else {
		return 1
	}
}

func compareInt64(l, r int64) int {
	if l == r {
		return 0
	} else if l < r {
		return -1
	} else {
		return 1
	}
}

func compareUint64(l, r uint64) int {
	if l == r {
		return 0
	} else if l < r {
		return -1
	} else {
		return 1
	}
}

func compareFloat32(l, r float32) int {
	if l == r {
		return 0
	} else if l < r {
		return -1
	} else {
		return 1
	}
}

func compareFloat64(l, r float64) int {
	if l == r {
		return 0
	} else if l < r {
		return -1
	} else {
		return 1
	}
}

func compareString(l, r string, coll Collation) int {
	// todo(andy): collations
	return bytes.Compare([]byte(l), []byte(r))
}

func compareBytes(l, r []byte, coll Collation) int {
	// todo(andy): collations
	return bytes.Compare(l, r)
}

// rawCmp is an array of indexes used to perform raw Tuple comparisons.
// Under certain conditions, Tuple comparisons can be optimized by
// directly comparing Tuples as byte slices, rather than accessing
// and deserializing each field.
// If each of these conditions is met, raw comparisons can be used:
//   (1) All fields in the Tuple must be non-nullable.
//   (2) All fields in the Tuple must be of constant size
//  	  (eg Ints, Uints, Floats, Time types, etc.)
//
type rawCmp []int

var rawCmpLookup = map[Encoding]rawCmp{
	Int8Enc:   {0},
	Uint8Enc:  {0},
	Int16Enc:  {1, 0},
	Uint16Enc: {1, 0},
	Int24Enc:  {2, 1, 0},
	Uint24Enc: {2, 1, 0},
	Int32Enc:  {3, 2, 1, 0},
	Uint32Enc: {3, 2, 1, 0},
	Int64Enc:  {7, 6, 5, 4, 3, 2, 1, 0},
	Uint64Enc: {7, 6, 5, 4, 3, 2, 1, 0},
}

func compareRaw(left, right Tuple, mapping rawCmp) int {
	var l, r byte
	for _, idx := range mapping {
		l, r = left[idx], right[idx]
		if l != r {
			break
		}
	}
	if l > r {
		return 1
	} else if l < r {
		return -1
	}
	return 0
}

func maybeGetRawComparison(types ...Type) rawCmp {
	var raw []int
	offset := 0
	for _, typ := range types {
		if typ.Nullable {
			return nil
		}

		mapping, ok := rawCmpLookup[typ.Enc]
		if !ok {
			return nil
		}

		for i := range mapping {
			mapping[i] += offset
		}
		raw = append(raw, mapping...)
		offset += len(mapping)
	}
	return raw
}
