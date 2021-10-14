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
	"encoding/binary"
	"time"
)

type byteSize uint16

type Encoding uint8

func FixedWidth(t Encoding) bool {
	return t >= sentinel
}

func SizeOf(enc Encoding) byteSize {
	if !FixedWidth(enc) {
		panic("cannot size variable width Encoding")
	}
	return encodingSize[enc]
}

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
	TimeEnc    Encoding = 13

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

var encodingSize = [14]byteSize{
	1, // NullEnc
	1, // Int8Enc
	1, // Uint8Enc
	2, // Int16Enc
	2, // Uint16Enc
	3, // Int24Enc
	3, // Uint24Enc
	4, // Int32Enc
	4, // Uint32Enc
	8, // Int64Enc
	8, // Uint64Enc
	4, // Float32Enc
	8, // Float64Enc
	4, // TimeEnc
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
	panic("unimplemented")

}
func readFloat64(val []byte) float64 {
	panic("unimplemented")
}

func readTime(val []byte) time.Time {
	panic("unimplemented")
}

func readString(val []byte) string {
	// todo(andy): fix allocation
	return string(val)
}

func readBytes(val []byte) []byte {
	return val
}
