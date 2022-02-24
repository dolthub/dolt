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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	tests := []struct {
		typ  Type
		l, r []byte
		cmp  int
	}{
		// ints
		{
			typ: Type{Enc: Int64Enc},
			l:   encInt(0), r: encInt(0),
			cmp: 0,
		},
		{
			typ: Type{Enc: Int64Enc},
			l:   encInt(-1), r: encInt(0),
			cmp: -1,
		},
		{
			typ: Type{Enc: Int64Enc},
			l:   encInt(1), r: encInt(0),
			cmp: 1,
		},
		// uints
		{
			typ: Type{Enc: Uint64Enc},
			l:   encUint(0), r: encUint(0),
			cmp: 0,
		},
		{
			typ: Type{Enc: Uint64Enc},
			l:   encUint(0), r: encUint(1),
			cmp: -1,
		},
		{
			typ: Type{Enc: Uint64Enc},
			l:   encUint(1), r: encUint(0),
			cmp: 1,
		},
		// floats
		{
			typ: Type{Enc: Float64Enc},
			l:   encFloat(0), r: encFloat(0),
			cmp: 0,
		},
		{
			typ: Type{Enc: Float64Enc},
			l:   encFloat(-1), r: encFloat(0),
			cmp: -1,
		},
		{
			typ: Type{Enc: Float64Enc},
			l:   encFloat(1), r: encFloat(0),
			cmp: 1,
		},
		// strings
		{
			typ: Type{Enc: StringEnc},
			l:   encStr(""), r: encStr(""),
			cmp: 0,
		},
		{
			typ: Type{Enc: StringEnc},
			l:   encStr(""), r: encStr("a"),
			cmp: -1,
		},
		{
			typ: Type{Enc: StringEnc},
			l:   encStr("a"), r: encStr(""),
			cmp: 1,
		},
		{
			typ: Type{Enc: StringEnc},
			l:   encStr("a"), r: encStr("a"),
			cmp: 0,
		},
		{
			typ: Type{Enc: StringEnc},
			l:   encStr("a"), r: encStr("b"),
			cmp: -1,
		},
		{
			typ: Type{Enc: StringEnc},
			l:   encStr("b"), r: encStr("a"),
			cmp: 1,
		},
	}

	for _, test := range tests {
		act := compare(test.typ, test.l, test.r)
		assert.Equal(t, test.cmp, act)
	}
}

func encInt(i int64) []byte {
	buf := make([]byte, 8)
	writeInt64(buf, i)
	return buf
}

func encUint(u uint64) []byte {
	buf := make([]byte, 8)
	writeUint64(buf, u)
	return buf
}

func encFloat(f float64) []byte {
	buf := make([]byte, 8)
	writeFloat64(buf, f)
	return buf
}

func encStr(s string) []byte {
	buf := make([]byte, len(s)+1)
	writeString(buf, s)
	return buf
}

func TestCodecRoundTrip(t *testing.T) {
	t.Run("round trip bool", func(t *testing.T) {
		roundTripBools(t)
	})
	t.Run("round trip ints", func(t *testing.T) {
		roundTripInts(t)
	})
	t.Run("round trip uints", func(t *testing.T) {
		roundTripUints(t)
	})
	t.Run("round trip floats", func(t *testing.T) {
		roundTripFloats(t)
	})
}

func roundTripBools(t *testing.T) {
	buf := make([]byte, 1)
	integers := []bool{true, false}
	for _, exp := range integers {
		writeBool(buf, exp)
		assert.Equal(t, exp, readBool(buf))
		zero(buf)
	}
}

func roundTripInts(t *testing.T) {
	buf := make([]byte, int8Size)
	integers := []int64{-1, 0, -1, math.MaxInt8, math.MinInt8}
	for _, value := range integers {
		exp := int8(value)
		writeInt8(buf, exp)
		assert.Equal(t, exp, readInt8(buf))
		zero(buf)
	}

	buf = make([]byte, int16Size)
	integers = append(integers, math.MaxInt16, math.MaxInt16)
	for _, value := range integers {
		exp := int16(value)
		writeInt16(buf, exp)
		assert.Equal(t, exp, readInt16(buf))
		zero(buf)
	}

	buf = make([]byte, int32Size)
	integers = append(integers, math.MaxInt32, math.MaxInt32)
	for _, value := range integers {
		exp := int32(value)
		writeInt32(buf, exp)
		assert.Equal(t, exp, readInt32(buf))
		zero(buf)
	}

	buf = make([]byte, int64Size)
	integers = append(integers, math.MaxInt64, math.MaxInt64)
	for _, value := range integers {
		exp := int64(value)
		writeInt64(buf, exp)
		assert.Equal(t, exp, readInt64(buf))
		zero(buf)
	}
}

func roundTripUints(t *testing.T) {
	buf := make([]byte, uint8Size)
	uintegers := []uint64{0, 1, math.MaxUint8}
	for _, value := range uintegers {
		exp := uint8(value)
		writeUint8(buf, exp)
		assert.Equal(t, exp, readUint8(buf))
		zero(buf)
	}

	buf = make([]byte, uint16Size)
	uintegers = append(uintegers, math.MaxUint16)
	for _, value := range uintegers {
		exp := uint16(value)
		writeUint16(buf, exp)
		assert.Equal(t, exp, readUint16(buf))
		zero(buf)
	}

	buf = make([]byte, uint32Size)
	uintegers = append(uintegers, math.MaxUint32)
	for _, value := range uintegers {
		exp := uint32(value)
		writeUint32(buf, exp)
		assert.Equal(t, exp, readUint32(buf))
		zero(buf)
	}

	buf = make([]byte, uint64Size)
	uintegers = append(uintegers, math.MaxUint64)
	for _, value := range uintegers {
		exp := uint64(value)
		writeUint64(buf, exp)
		assert.Equal(t, exp, readUint64(buf))
		zero(buf)
	}
}

func roundTripFloats(t *testing.T) {
	buf := make([]byte, float32Size)
	floats := []float64{-1, 0, 1, math.MaxFloat32, math.SmallestNonzeroFloat32}
	for _, value := range floats {
		exp := float32(value)
		writeFloat32(buf, exp)
		assert.Equal(t, exp, readFloat32(buf))
		zero(buf)
	}

	buf = make([]byte, float64Size)
	floats = append(floats, math.MaxFloat64, math.SmallestNonzeroFloat64)
	for _, value := range floats {
		exp := float64(value)
		writeFloat64(buf, exp)
		assert.Equal(t, exp, readFloat64(buf))
		zero(buf)
	}
}

func zero(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}
