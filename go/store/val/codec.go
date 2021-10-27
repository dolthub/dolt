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
	"fmt"
	"math"
)

type Type struct {
	Enc      Encoding
	Coll     Collation
	Nullable bool
}

type ByteSize uint16

type Collation uint16

const (
	ByteOrderCollation Collation = 0
)

type Encoding uint8

// Fixed Width Encodings
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

	// TimeEnc    Encoding = 13
	// TimestampEnc
	// DateEnc
	// TimeEnc
	// DatetimeEnc
	// YearEnc

	sentinel Encoding = 127
)

// Variable Width Encodings
const (
	StringEnc Encoding = 128
	BytesEnc  Encoding = 129

	// DecimalEnc
	// BitEnc
	// CharEnc
	// VarCharEnc
	// TextEnc
	// BinaryEnc
	// VarBinaryEnc
	// BlobEnc
	// JSONEnc
	// EnumEnc
	// SetEnc
	// ExpressionEnc
	// GeometryEnc
)

func FixedWidth(t Encoding) bool {
	return t >= sentinel
}

func readBool(val []byte) bool {
	return val[0] == 1
}
func readInt8(val []byte) int8 {
	return int8(val[0])
}

func readUint8(val []byte) uint8 {
	return val[0]
}

func readInt16(val []byte) int16 {
	return int16(binary.LittleEndian.Uint16(val))
}

func readUint16(val []byte) uint16 {
	return binary.LittleEndian.Uint16(val)
}

func readInt32(val []byte) int32 {
	return int32(binary.LittleEndian.Uint32(val))
}

func readUint32(val []byte) uint32 {
	return binary.LittleEndian.Uint32(val)
}

func readInt64(val []byte) int64 {
	return int64(binary.LittleEndian.Uint64(val))
}

func readUint64(val []byte) uint64 {
	return binary.LittleEndian.Uint64(val)
}

func readFloat32(val []byte) float32 {
	return math.Float32frombits(readUint32(val))
}

func readFloat64(val []byte) float64 {
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
	if val {
		buf[0] = byte(1)
	} else {
		buf[0] = byte(0)
	}
}

func writeInt8(buf []byte, val int8) {
	buf[0] = byte(val)
}

func writeUint8(buf []byte, val uint8) {
	buf[0] = byte(val)
}

func writeInt16(buf []byte, val int16) {
	binary.LittleEndian.PutUint16(buf, uint16(val))
}

func writeUint16(buf []byte, val uint16) {
	binary.LittleEndian.PutUint16(buf, val)
}

func writeInt32(buf []byte, val int32) {
	binary.LittleEndian.PutUint32(buf, uint32(val))
}

func writeUint32(buf []byte, val uint32) {
	binary.LittleEndian.PutUint32(buf, val)
}

func writeInt64(buf []byte, val int64) {
	binary.LittleEndian.PutUint64(buf, uint64(val))
}

func writeUint64(buf []byte, val uint64) {
	binary.LittleEndian.PutUint64(buf, val)
}

func writeFloat32(buf []byte, val float32) {
	binary.LittleEndian.PutUint32(buf, math.Float32bits(val))
}

func writeFloat64(buf []byte, val float64) {
	binary.LittleEndian.PutUint64(buf, math.Float64bits(val))
}

func writeString(buf []byte, val string, coll Collation) {
	copy(buf, val)
}

func writeBytes(buf, val []byte, coll Collation) {
	copy(buf, val)
}

func compare(typ Type, left, right []byte) (cmp int) {
	// todo(andy): handle NULLs

	switch typ.Enc {
	case Int8Enc:
		cmp = compareInt8(readInt8(left), readInt8(right))
	case Uint8Enc:
		cmp = compareUint8(readUint8(left), readUint8(right))
	case Int16Enc:
		cmp = compareInt16(readInt16(left), readInt16(right))
	case Uint16Enc:
		cmp = compareUint16(readUint16(left), readUint16(right))
	case Int24Enc:
		panic("unimplemented")
	case Uint24Enc:
		panic("unimplemented")
	case Int32Enc:
		cmp = compareInt32(readInt32(left), readInt32(right))
	case Uint32Enc:
		cmp = compareUint32(readUint32(left), readUint32(right))
	case Int64Enc:
		cmp = compareInt64(readInt64(left), readInt64(right))
	case Uint64Enc:
		cmp = compareUint64(readUint64(left), readUint64(right))
	case Float32Enc:
		cmp = compareFloat32(readFloat32(left), readFloat32(right))
	case Float64Enc:
		cmp = compareFloat64(readFloat64(left), readFloat64(right))
	case StringEnc:
		cmp = compareString(readString(left, typ.Coll), readString(right, typ.Coll), typ.Coll)
	case BytesEnc:
		cmp = compareBytes(readBytes(left, typ.Coll), readBytes(right, typ.Coll), typ.Coll)
	default:
		panic(fmt.Sprintf("unknown encoding %d", typ.Enc))
	}
	return
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

type comparisonMapping []int

// compareRaw compares Tuples without accessing and decoding fields.
func compareRaw(left, right Tuple, mapping comparisonMapping) Comparison {
	var l, r byte
	for _, idx := range mapping {
		l, r = left[idx], right[idx]
		if l != r {
			break
		}
	}
	if l > r {
		return 1
	}
	if l < r {
		return -1
	}
	return 0
}

func maybeGetRawComparison(types ...Type) comparisonMapping {
	// todo(andy): add back
	return nil

	//var raw []int
	//offset := 0
	//for _, typ := range Types {
	//	mapping, ok := rawComparisonMap(typ.Enc)
	//	if !ok {
	//		// every type in |Types| must be
	//		// raw-comparable to use a mapping
	//		return nil
	//	}
	//	for i := range mapping {
	//		mapping[i] += offset
	//	}
	//	raw = append(raw, mapping...)
	//	offset += len(mapping)
	//}
	//return raw
}

func rawComparisonMap(enc Encoding) (mapping []int, ok bool) {
	// todo(andy): add fixed width char and byte encodings
	lookup := map[Encoding][]int{
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

	mapping, ok = lookup[enc]
	return
}
