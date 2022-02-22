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

func TestTupleBuilder(t *testing.T) {
	t.Run("smoke test", func(t *testing.T) {
		smokeTestTupleBuilder(t)
	})
	t.Run("round trip ints", func(t *testing.T) {
		testRoundTripInts(t)
	})
}

func smokeTestTupleBuilder(t *testing.T) {
	desc := NewTupleDescriptor(
		Type{Enc: Int8Enc},
		Type{Enc: Int16Enc},
		Type{Enc: Int32Enc},
		Type{Enc: Int64Enc},
		Type{Enc: Uint8Enc},
		Type{Enc: Uint16Enc},
		Type{Enc: Uint32Enc},
		Type{Enc: Uint64Enc},
		Type{Enc: Float32Enc},
		Type{Enc: Float64Enc},
		Type{Enc: StringEnc},
		Type{Enc: ByteStringEnc},
	)

	tb := NewTupleBuilder(desc)
	tb.PutInt8(0, math.MaxInt8)
	tb.PutInt16(1, math.MaxInt16)
	tb.PutInt32(2, math.MaxInt32)
	tb.PutInt64(3, math.MaxInt64)
	tb.PutUint8(4, math.MaxUint8)
	tb.PutUint16(5, math.MaxUint16)
	tb.PutUint32(6, math.MaxUint32)
	tb.PutUint64(7, math.MaxUint64)
	tb.PutFloat32(8, math.MaxFloat32)
	tb.PutFloat64(9, math.MaxFloat64)
	tb.PutString(10, "123")
	tb.PutByteString(11, []byte("abc"))

	tup := tb.Build(testPool)
	i8, ok := desc.GetInt8(0, tup)
	assert.True(t, ok)
	assert.Equal(t, int8(math.MaxInt8), i8)
	i16, ok := desc.GetInt16(1, tup)
	assert.True(t, ok)
	assert.Equal(t, int16(math.MaxInt16), i16)
	i32, ok := desc.GetInt32(2, tup)
	assert.True(t, ok)
	assert.Equal(t, int32(math.MaxInt32), i32)
	i64, ok := desc.GetInt64(3, tup)
	assert.True(t, ok)
	assert.Equal(t, int64(math.MaxInt64), i64)
	u8, ok := desc.GetUint8(4, tup)
	assert.True(t, ok)
	assert.Equal(t, uint8(math.MaxUint8), u8)
	u16, ok := desc.GetUint16(5, tup)
	assert.True(t, ok)
	assert.Equal(t, uint16(math.MaxUint16), u16)
	u32, ok := desc.GetUint32(6, tup)
	assert.True(t, ok)
	assert.Equal(t, uint32(math.MaxUint32), u32)
	u64, ok := desc.GetUint64(7, tup)
	assert.True(t, ok)
	assert.Equal(t, uint64(math.MaxUint64), u64)
	f32, ok := desc.GetFloat32(8, tup)
	assert.True(t, ok)
	assert.Equal(t, float32(math.MaxFloat32), f32)
	f64, ok := desc.GetFloat64(9, tup)
	assert.True(t, ok)
	assert.Equal(t, float64(math.MaxFloat64), f64)
	str, ok := desc.GetString(10, tup)
	assert.True(t, ok)
	assert.Equal(t, "123", str)
	byts, ok := desc.GetBytes(11, tup)
	assert.True(t, ok)
	assert.Equal(t, []byte("abc"), byts)
}

func testRoundTripInts(t *testing.T) {
	typ := Type{Enc: Int64Enc, Nullable: true}

	tests := []struct {
		desc TupleDesc
		data map[int]int64
	}{
		{
			desc: NewTupleDescriptor(typ),
			data: map[int]int64{
				0: 0,
			},
		},
		{
			desc: NewTupleDescriptor(typ, typ, typ),
			data: map[int]int64{
				0: 0,
				1: 1,
				2: 2,
			},
		},
		{
			desc: NewTupleDescriptor(typ),
			data: map[int]int64{
				// 0: NULL,
			},
		},
		{
			desc: NewTupleDescriptor(typ, typ, typ),
			data: map[int]int64{
				// 0: NULL,
				// 1: NULL,
				2: 2,
			},
		},
	}

	for _, test := range tests {
		// build
		bld := NewTupleBuilder(test.desc)
		for idx, value := range test.data {
			bld.PutInt64(idx, value)
		}
		tup := bld.Build(testPool)

		// verify
		n := test.desc.Count()
		for idx := 0; idx < n; idx++ {
			exp, ok := test.data[idx]

			if !ok {
				null := test.desc.IsNull(idx, tup)
				assert.True(t, null)
			} else {
				act, ok := test.desc.GetInt64(idx, tup)
				assert.True(t, ok)
				assert.Equal(t, exp, act)
			}
		}
	}
}

type testCompare struct{}

func (tc testCompare) Compare(left, right Tuple, desc TupleDesc) (cmp int) {
	for i, typ := range desc.Types {
		cmp = compare(typ, left.GetField(i), right.GetField(i))
		if cmp != 0 {
			break
		}
	}
	return
}

var _ TupleComparator = testCompare{}
