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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/pool"
)

var shared = pool.NewBuffPool()

func TestNewTuple(t *testing.T) {
	t.Run("test tuple round trip", func(t *testing.T) {
		roundTripBytes(t)
	})
}

func roundTripBytes(t *testing.T) {
	randomBytes := func(t *testing.T) (fields [][]byte) {
		fields = make([][]byte, (rand.Uint32()%19)+1)
		assert.True(t, len(fields) > 0)
		for i := range fields {
			if rand.Uint32()%4 == 0 {
				// 25% NULL
				continue
			}
			fields[i] = make([]byte, rand.Uint32()%20)
			rand.Read(fields[i])
		}
		return
	}

	for n := 0; n < 100; n++ {
		fields := randomBytes(t)
		tup := NewTuple(shared, fields...)
		for i, field := range fields {
			assert.Equal(t, field, tup.GetField(i))
		}
	}
}

func TestTupleBuilder(t *testing.T) {
	t.Run("smoke test", func(t *testing.T) {
		smokeTestTupleBuilder(t)
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
		Type{Enc: BytesEnc},
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
	tb.PutBytes(11, []byte("abc"))

	tup := tb.Tuple(shared)
	assert.Equal(t, int8(math.MaxInt8), desc.GetInt8(0, tup))
	assert.Equal(t, int16(math.MaxInt16), desc.GetInt16(1, tup))
	assert.Equal(t, int32(math.MaxInt32), desc.GetInt32(2, tup))
	assert.Equal(t, int64(math.MaxInt64), desc.GetInt64(3, tup))
	assert.Equal(t, uint8(math.MaxUint8), desc.GetUint8(4, tup))
	assert.Equal(t, uint16(math.MaxUint16), desc.GetUint16(5, tup))
	assert.Equal(t, uint32(math.MaxUint32), desc.GetUint32(6, tup))
	assert.Equal(t, uint64(math.MaxUint64), desc.GetUint64(7, tup))
	assert.Equal(t, float32(math.MaxFloat32), desc.GetFloat32(8, tup))
	assert.Equal(t, float64(math.MaxFloat64), desc.GetFloat64(9, tup))
	assert.Equal(t, "123", desc.GetString(10, tup))
	assert.Equal(t, []byte("abc"), desc.GetBytes(11, tup))
}
