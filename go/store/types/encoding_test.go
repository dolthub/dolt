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
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

func toBinaryNomsReaderData(nbf *NomsBinFormat, data []interface{}) []byte {
	w := newBinaryNomsWriter()
	for i, v := range data {
		switch v := v.(type) {
		case uint8:
			w.writeUint8(v)
		case string:
			w.writeString(v)
		case Float:
			w.writeFloat(v, nbf)
		case uint64:
			w.writeCount(v)
		case bool:
			w.writeBool(v)
		case hash.Hash:
			w.writeHash(v)
		case []byte:
			w.writeCount(uint64(len(v)))
			w.writeRaw(v)
		case NomsKind:
			w.writeUint8(uint8(v))
		default:
			panic("unreachable at index " + strconv.FormatInt(int64(i), 10))
		}
	}
	return w.data()
}

func assertEncoding(t *testing.T, expect []interface{}, v Value) {
	vs := newTestValueStore()
	expectedAsByteSlice := toBinaryNomsReaderData(vs.Format(), expect)
	w := newBinaryNomsWriter()
	err := v.writeTo(&w, vs.Format())
	require.NoError(t, err)
	assert.EqualValues(t, expectedAsByteSlice, w.data())

	dec := newValueDecoder(expectedAsByteSlice, vs)
	v2, err := dec.readValue(vs.Format())
	require.NoError(t, err)
	assert.True(t, v.Equals(v2))
}

func TestRoundTrips(t *testing.T) {
	vs := newTestValueStore()

	assertRoundTrips := func(v Value) {
		chnk, err := EncodeValue(v, vs.Format())
		require.NoError(t, err)
		out, err := DecodeValue(chnk, vs)
		require.NoError(t, err)
		assert.True(t, v.Equals(out))
	}

	assertRoundTrips(Bool(false))
	assertRoundTrips(Bool(true))

	assertRoundTrips(Float(0))
	assertRoundTrips(Float(-0))
	assertRoundTrips(Float(math.Copysign(0, -1)))

	intTest := []int64{1, 2, 3, 7, 15, 16, 17,
		127, 128, 129,
		254, 255, 256, 257,
		1023, 1024, 1025,
		2048, 4096, 8192, 32767, 32768, 65535, 65536,
		4294967295, 4294967296,
		9223372036854779,
		92233720368547760,
	}
	for _, v := range intTest {
		f := float64(v)
		assertRoundTrips(Float(f))
		f = math.Copysign(f, -1)
		assertRoundTrips(Float(f))
	}
	floatTest := []float64{1.01, 1.001, 1.0001, 1.00001, 1.000001, 100.01, 1000.000001, 122.411912027329, 0.42}
	for _, f := range floatTest {
		assertRoundTrips(Float(f))
		f = math.Copysign(f, -1)
		assertRoundTrips(Float(f))
	}

	// JS Float.MAX_SAFE_INTEGER
	assertRoundTrips(Float(9007199254740991))
	// JS Float.MIN_SAFE_INTEGER
	assertRoundTrips(Float(-9007199254740991))
	assertRoundTrips(Float(math.MaxFloat64))
	assertRoundTrips(Float(math.Nextafter(1, 2) - 1))

	assertRoundTrips(String(""))
	assertRoundTrips(String("foo"))
	assertRoundTrips(String("AINT NO THANG"))
	assertRoundTrips(String("💩"))

}

func TestWritePrimitives(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			BoolKind, true,
		},
		Bool(true))

	assertEncoding(t,
		[]interface{}{
			BoolKind, false,
		},
		Bool(false))

	assertEncoding(t,
		[]interface{}{
			FloatKind, Float(0),
		},
		Float(0))

	assertEncoding(t,
		[]interface{}{
			FloatKind, Float(1000000000000000000),
		},
		Float(1e18))

	assertEncoding(t,
		[]interface{}{
			FloatKind, Float(10000000000000000000),
		},
		Float(1e19))

	assertEncoding(t,
		[]interface{}{
			FloatKind, Float(1e+20),
		},
		Float(1e20))

	assertEncoding(t,
		[]interface{}{
			StringKind, "hi",
		},
		String("hi"))
}

func TestWriteTuple(t *testing.T) {
	vrw := newTestValueStore()
	assertEncoding(t,
		[]interface{}{
			TupleKind, uint64(4) /* len */, FloatKind, Float(0), FloatKind, Float(1), FloatKind, Float(2), FloatKind, Float(3),
		},
		mustValue(NewTuple(vrw.Format(), Float(0), Float(1), Float(2), Float(3))),
	)
}

func TestWriteEmptyTuple(t *testing.T) {
	vrw := newTestValueStore()
	assertEncoding(t,
		[]interface{}{
			TupleKind, uint64(0), /* len */
		},
		mustValue(NewTuple(vrw.Format())),
	)
}

func TestWriteRef(t *testing.T) {
	r := hash.Parse("0123456789abcdefghijklmnopqrstuv")
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			RefKind, r, FloatKind, uint64(4),
		},
		mustValue(constructRef(vrw.Format(), r, PrimitiveTypeMap[FloatKind], 4)),
	)
}
