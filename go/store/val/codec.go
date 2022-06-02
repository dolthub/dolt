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
	"math/big"
	"math/bits"
	"time"
	"unsafe"

	"github.com/dolthub/dolt/go/gen/fb/serial"

	"github.com/shopspring/decimal"
)

type Type struct {
	Enc      Encoding
	Nullable bool
}

const (
	strTerm = byte(0)
)

type ByteSize uint16

const (
	int8Size     ByteSize = 1
	uint8Size    ByteSize = 1
	int16Size    ByteSize = 2
	uint16Size   ByteSize = 2
	int32Size    ByteSize = 4
	uint32Size   ByteSize = 4
	int64Size    ByteSize = 8
	uint64Size   ByteSize = 8
	float32Size  ByteSize = 4
	float64Size  ByteSize = 8
	bit64Size    ByteSize = 8
	hash128Size  ByteSize = 16
	yearSize     ByteSize = 1
	dateSize     ByteSize = 4
	timeSize     ByteSize = 8
	datetimeSize ByteSize = 8
	enumSize     ByteSize = 2
	setSize      ByteSize = 8
)

type Encoding byte

// Fixed Width Encodings
const (
	NullEnc     = Encoding(serial.EncodingNull)
	Int8Enc     = Encoding(serial.EncodingInt8)
	Uint8Enc    = Encoding(serial.EncodingUint8)
	Int16Enc    = Encoding(serial.EncodingInt16)
	Uint16Enc   = Encoding(serial.EncodingUint16)
	Int32Enc    = Encoding(serial.EncodingInt32)
	Uint32Enc   = Encoding(serial.EncodingUint32)
	Int64Enc    = Encoding(serial.EncodingInt64)
	Uint64Enc   = Encoding(serial.EncodingUint64)
	Float32Enc  = Encoding(serial.EncodingFloat32)
	Float64Enc  = Encoding(serial.EncodingFloat64)
	Bit64Enc    = Encoding(serial.EncodingBit64)
	Hash128Enc  = Encoding(serial.EncodingHash128)
	YearEnc     = Encoding(serial.EncodingYear)
	DateEnc     = Encoding(serial.EncodingDate)
	TimeEnc     = Encoding(serial.EncodingTime)
	DatetimeEnc = Encoding(serial.EncodingDatetime)
	EnumEnc     = Encoding(serial.EncodingEnum)
	SetEnc      = Encoding(serial.EncodingSet)

	sentinel Encoding = 127
)

// Variable Width Encodings
const (
	StringEnc     = Encoding(serial.EncodingString)
	ByteStringEnc = Encoding(serial.EncodingBytes)
	DecimalEnc    = Encoding(serial.EncodingDecimal)
	JSONEnc       = Encoding(serial.EncodingJSON)
	GeometryEnc   = Encoding(serial.EncodingGeometry)

	// TODO
	//  CharEnc
	//  BinaryEnc
	//  TextEnc
	//  BlobEnc
	//  EnumEnc
	//  SetEnc
	//  ExpressionEnc
)

func sizeFromType(t Type) (ByteSize, bool) {
	switch t.Enc {
	case Int8Enc:
		return int8Size, true
	case Uint8Enc:
		return uint8Size, true
	case Int16Enc:
		return int16Size, true
	case Uint16Enc:
		return uint16Size, true
	case Int32Enc:
		return int32Size, true
	case Uint32Enc:
		return uint32Size, true
	case Int64Enc:
		return int64Size, true
	case Uint64Enc:
		return uint64Size, true
	case Float32Enc:
		return float32Size, true
	case Float64Enc:
		return float64Size, true
	case Hash128Enc:
		return hash128Size, true
	case YearEnc:
		return yearSize, true
	case DateEnc:
		return dateSize, true
	case TimeEnc:
		return timeSize, true
	case DatetimeEnc:
		return datetimeSize, true
	case EnumEnc:
		return enumSize, true
	case SetEnc:
		return setSize, true
	case Bit64Enc:
		return bit64Size, true
	default:
		return 0, false
	}
}

func readBool(val []byte) bool {
	expectSize(val, int8Size)
	return val[0] == 1
}

func writeBool(buf []byte, val bool) {
	expectSize(buf, 1)
	if val {
		buf[0] = byte(1)
	} else {
		buf[0] = byte(0)
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

func readInt8(val []byte) int8 {
	expectSize(val, int8Size)
	return int8(val[0])
}

func writeInt8(buf []byte, val int8) {
	expectSize(buf, int8Size)
	buf[0] = byte(val)
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

func readUint8(val []byte) uint8 {
	expectSize(val, uint8Size)
	return val[0]
}

func writeUint8(buf []byte, val uint8) {
	expectSize(buf, uint8Size)
	buf[0] = byte(val)
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

func readInt16(val []byte) int16 {
	expectSize(val, int16Size)
	return int16(binary.LittleEndian.Uint16(val))
}

func writeInt16(buf []byte, val int16) {
	expectSize(buf, int16Size)
	binary.LittleEndian.PutUint16(buf, uint16(val))
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

func readUint16(val []byte) uint16 {
	expectSize(val, uint16Size)
	return binary.LittleEndian.Uint16(val)
}

func writeUint16(buf []byte, val uint16) {
	expectSize(buf, uint16Size)
	binary.LittleEndian.PutUint16(buf, val)
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

func readInt32(val []byte) int32 {
	expectSize(val, int32Size)
	return int32(binary.LittleEndian.Uint32(val))
}

func writeInt32(buf []byte, val int32) {
	expectSize(buf, int32Size)
	binary.LittleEndian.PutUint32(buf, uint32(val))
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

func readUint32(val []byte) uint32 {
	expectSize(val, uint32Size)
	return binary.LittleEndian.Uint32(val)
}

func writeUint32(buf []byte, val uint32) {
	expectSize(buf, uint32Size)
	binary.LittleEndian.PutUint32(buf, val)
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

func readInt64(val []byte) int64 {
	expectSize(val, int64Size)
	return int64(binary.LittleEndian.Uint64(val))
}

func writeInt64(buf []byte, val int64) {
	expectSize(buf, int64Size)
	binary.LittleEndian.PutUint64(buf, uint64(val))
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

func readUint64(val []byte) uint64 {
	expectSize(val, uint64Size)
	return binary.LittleEndian.Uint64(val)
}

func writeUint64(buf []byte, val uint64) {
	expectSize(buf, uint64Size)
	binary.LittleEndian.PutUint64(buf, val)
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

func readFloat32(val []byte) float32 {
	expectSize(val, float32Size)
	return math.Float32frombits(readUint32(val))
}

func writeFloat32(buf []byte, val float32) {
	expectSize(buf, float32Size)
	binary.LittleEndian.PutUint32(buf, math.Float32bits(val))
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

func readFloat64(val []byte) float64 {
	expectSize(val, float64Size)
	return math.Float64frombits(readUint64(val))
}

func writeFloat64(buf []byte, val float64) {
	expectSize(buf, float64Size)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(val))
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

func readBit64(val []byte) uint64 {
	return readUint64(val)
}

func writeBit64(buf []byte, val uint64) {
	writeUint64(buf, val)
}

func compareBit64(l, r uint64) int {
	return compareUint64(l, r)
}

func readDecimal(val []byte) decimal.Decimal {
	e := readInt32(val[:int32Size])
	s := readInt8(val[int32Size : int32Size+int8Size])
	b := big.NewInt(0).SetBytes(val[int32Size+int8Size:])
	if s < 0 {
		b = b.Neg(b)
	}
	return decimal.NewFromBigInt(b, e)
}

func writeDecimal(buf []byte, val decimal.Decimal) {
	expectSize(buf, sizeOfDecimal(val))
	writeInt32(buf[:int32Size], val.Exponent())
	b := val.Coefficient()
	writeInt8(buf[int32Size:int32Size+int8Size], int8(b.Sign()))
	b.FillBytes(buf[int32Size+int8Size:])
}

func sizeOfDecimal(val decimal.Decimal) ByteSize {
	bsz := len(val.Coefficient().Bits()) * (bits.UintSize / 8)
	return int32Size + int8Size + ByteSize(bsz)
}

func compareDecimal(l, r decimal.Decimal) int {
	return l.Cmp(r)
}

const minYear int16 = 1901

func readYear(val []byte) int16 {
	expectSize(val, yearSize)
	return int16(readUint8(val)) + minYear
}

func writeYear(buf []byte, val int16) {
	expectSize(buf, yearSize)
	writeUint8(buf, uint8(val-minYear))
}

func compareYear(l, r int16) int {
	return compareInt16(l, r)
}

// adapted from:
// https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
const (
	yearShift  uint32 = 16
	monthShift uint32 = 8
	monthMask  uint32 = 255 << monthShift
	dayMask    uint32 = 255
)

func readDate(val []byte) (date time.Time) {
	expectSize(val, dateSize)
	t := readUint32(val)
	y := t >> yearShift
	m := (t & monthMask) >> monthShift
	d := (t & dayMask)
	return time.Date(int(y), time.Month(m), int(d), 0, 0, 0, 0, time.UTC)
}

func writeDate(buf []byte, val time.Time) {
	expectSize(buf, dateSize)
	t := uint32(val.Year() << yearShift)
	t += uint32(val.Month() << monthShift)
	t += uint32(val.Day())
	writeUint32(buf, t)
}

func compareDate(l, r time.Time) int {
	return compareDatetime(l, r)
}

func readTime(val []byte) int64 {
	expectSize(val, timeSize)
	return readInt64(val)
}

func writeTime(buf []byte, val int64) {
	expectSize(buf, timeSize)
	writeInt64(buf, val)
}

func compareTime(l, r int64) int {
	return compareInt64(l, r)
}

func readDatetime(buf []byte) (t time.Time) {
	expectSize(buf, datetimeSize)
	t = time.UnixMicro(readInt64(buf)).UTC()
	return
}

func writeDatetime(buf []byte, val time.Time) {
	expectSize(buf, datetimeSize)
	writeInt64(buf, val.UnixMicro())
}

func compareDatetime(l, r time.Time) int {
	if l.Equal(r) {
		return 0
	} else if l.Before(r) {
		return -1
	} else {
		return 1
	}
}

func readEnum(val []byte) uint16 {
	return readUint16(val)
}

func writeEnum(buf []byte, val uint16) {
	writeUint16(buf, val)
}

func compareEnum(l, r uint16) int {
	return compareUint16(l, r)
}

func readSet(val []byte) uint64 {
	return readUint64(val)
}

func writeSet(buf []byte, val uint64) {
	writeUint64(buf, val)
}

func compareSet(l, r uint64) int {
	return compareUint64(l, r)
}

func readString(val []byte) string {
	return stringFromBytes(readByteString(val))
}

func writeString(buf []byte, val string) {
	writeByteString(buf, []byte(val))
}

func compareString(l, r string) int {
	return bytes.Compare([]byte(l), []byte(r))
}

func readByteString(val []byte) []byte {
	length := len(val) - 1
	return val[:length]
}

func writeByteString(buf, val []byte) {
	expectSize(buf, ByteSize(len(val))+1)
	copy(buf, val)
	buf[len(val)] = strTerm
}

func compareByteString(l, r []byte) int {
	return bytes.Compare(l, r)
}

func readHash128(val []byte) []byte {
	expectSize(val, hash128Size)
	return val
}

func writeHash128(buf, val []byte) {
	expectSize(buf, hash128Size)
	copy(buf, val)
}

func compareHash128(l, r []byte) int {
	return bytes.Compare(l, r)
}

func writeRaw(buf, val []byte) {
	expectSize(buf, ByteSize(len(val)))
	copy(buf, val)
}

func expectSize(buf []byte, sz ByteSize) {
	if ByteSize(len(buf)) != sz {
		panic("byte slice is not of expected size")
	}
}

// stringFromBytes converts a []byte to string without a heap allocation.
func stringFromBytes(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
