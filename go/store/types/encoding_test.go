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
	"bytes"
	"context"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/constants"
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

	st, err := NewStruct(vs.Format(), "", StructData{"a": Bool(true), "b": String("foo"), "c": Float(2.3)})
	require.NoError(t, err)
	assertRoundTrips(st)

	ll, err := newListLeafSequence(vs, Float(4), Float(5), Float(6), Float(7))
	require.NoError(t, err)
	listLeaf := newList(ll)
	assertRoundTrips(listLeaf)

	ref, err := NewRef(listLeaf, vs.Format())
	require.NoError(t, err)
	k10, err := orderedKeyFromInt(10, vs.Format())
	require.NoError(t, err)
	k20, err := orderedKeyFromInt(20, vs.Format())
	require.NoError(t, err)
	mt1, err := newMetaTuple(ref, k10, 10)
	require.NoError(t, err)
	mt2, err := newMetaTuple(ref, k20, 20)
	require.NoError(t, err)
	mseq, err := newListMetaSequence(1, []metaTuple{mt1, mt2}, vs)
	require.NoError(t, err)
	assertRoundTrips(newList(mseq))
}

func TestNonFiniteNumbers(tt *testing.T) {
	vs := newTestValueStore_7_18()

	t := func(f float64) (err error) {
		defer func() {
			if r := recover(); r != nil {
				if err == nil {
					err = r.(error)
				}
			}
		}()

		v := Float(f)
		_, err = EncodeValue(v, vs.Format())
		return
	}

	err := t(math.NaN())
	assert.Error(tt, err)
	assert.Contains(tt, err.Error(), "NaN is not a supported number")

	err = t(math.Inf(1))
	assert.Error(tt, err)
	assert.Contains(tt, err.Error(), "+Inf is not a supported number")

	err = t(math.Inf(-1))
	assert.Error(tt, err)
	assert.Contains(tt, err.Error(), "-Inf is not a supported number")
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

func TestWriteSimpleBlob(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			BlobKind, uint64(0), []byte{0x00, 0x01},
		},
		mustValue(NewBlob(context.Background(), vrw, bytes.NewBuffer([]byte{0x00, 0x01}))),
	)
}

func TestWriteList(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0), uint64(4) /* len */, FloatKind, Float(0), FloatKind, Float(1), FloatKind, Float(2), FloatKind, Float(3),
		},
		mustValue(NewList(context.Background(), vrw, Float(0), Float(1), Float(2), Float(3))),
	)
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

func TestWriteListOfList(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0),
			uint64(2), // len
			ListKind, uint64(0), uint64(1) /* len */, FloatKind, Float(0),
			ListKind, uint64(0), uint64(3) /* len */, FloatKind, Float(1), FloatKind, Float(2), FloatKind, Float(3),
		},
		mustValue(NewList(context.Background(), vrw,
			mustList(NewList(context.Background(), vrw, Float(0))),
			mustList(NewList(context.Background(), vrw, Float(1), Float(2), Float(3))))),
	)
}

func TestWriteSet(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			SetKind, uint64(0), uint64(4), /* len */
			FloatKind, Float(0), FloatKind, Float(1), FloatKind, Float(2), FloatKind, Float(3),
		},
		mustValue(NewSet(context.Background(), vrw, Float(3), Float(1), Float(2), Float(0))),
	)
}

func TestWriteSetOfSet(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			SetKind, uint64(0), uint64(2), // len
			SetKind, uint64(0), uint64(3) /* len */, FloatKind, Float(1), FloatKind, Float(2), FloatKind, Float(3),
			SetKind, uint64(0), uint64(1) /* len */, FloatKind, Float(0),
		},
		mustValue(NewSet(context.Background(), vrw,
			mustValue(NewSet(context.Background(), vrw, Float(0))),
			mustValue(NewSet(context.Background(), vrw, Float(1), Float(2), Float(3))))),
	)
}

func TestWriteMap(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			MapKind, uint64(0), uint64(3), /* len */
			StringKind, "a", BoolKind, false,
			StringKind, "b", BoolKind, true,
			StringKind, "c", TupleKind, uint64(1), FloatKind, Float(1.0),
		},
		mustValue(NewMap(context.Background(), vrw, String("a"), Bool(false), String("b"), Bool(true), String("c"), mustValue(NewTuple(vrw.Format(), Float(1.0))))),
	)
}

func TestWriteMapOfMap(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			MapKind, uint64(0), uint64(1), // len
			MapKind, uint64(0), uint64(1) /* len */, StringKind, "a", FloatKind, Float(0),
			SetKind, uint64(0), uint64(1) /* len */, BoolKind, true,
		},
		mustValue(NewMap(context.Background(), vrw,
			mustValue(NewMap(context.Background(), vrw, String("a"), Float(0))),
			mustValue(NewSet(context.Background(), vrw, Bool(true))))),
	)
}

func TestWriteCompoundBlob(t *testing.T) {
	r1 := hash.Parse("00000000000000000000000000000001")
	r2 := hash.Parse("00000000000000000000000000000002")
	r3 := hash.Parse("00000000000000000000000000000003")
	vrw := newTestValueStore()
	assertEncoding(t,
		[]interface{}{
			BlobKind, uint64(1),
			uint64(3), // len
			RefKind, r1, BlobKind, uint64(11), FloatKind, Float(20), uint64(20),
			RefKind, r2, BlobKind, uint64(22), FloatKind, Float(40), uint64(40),
			RefKind, r3, BlobKind, uint64(33), FloatKind, Float(60), uint64(60),
		},
		newBlob(mustSeq(newBlobMetaSequence(1, []metaTuple{
			mustMetaTuple(newMetaTuple(mustRef(constructRef(vrw.Format(), r1, PrimitiveTypeMap[BlobKind], 11)), mustOrdKey(orderedKeyFromInt(20, vrw.Format())), 20)),
			mustMetaTuple(newMetaTuple(mustRef(constructRef(vrw.Format(), r2, PrimitiveTypeMap[BlobKind], 22)), mustOrdKey(orderedKeyFromInt(40, vrw.Format())), 40)),
			mustMetaTuple(newMetaTuple(mustRef(constructRef(vrw.Format(), r3, PrimitiveTypeMap[BlobKind], 33)), mustOrdKey(orderedKeyFromInt(60, vrw.Format())), 60)),
		}, vrw))),
	)
}

func TestWriteEmptyStruct(t *testing.T) {
	vrw := newTestValueStore()
	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(0), /* len */
		},
		mustValue(NewStruct(vrw.Format(), "S", nil)),
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

func TestWriteStruct(t *testing.T) {
	vrw := newTestValueStore()
	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(2), /* len */
			"b", BoolKind, true, "x", FloatKind, Float(42),
		},
		mustValue(NewStruct(vrw.Format(), "S", StructData{"x": Float(42), "b": Bool(true)})),
	)
}

func TestWriteStructTooMuchData(t *testing.T) {
	vrw := newTestValueStore()
	s, err := NewStruct(vrw.Format(), "S", StructData{"x": Float(42), "b": Bool(true)})
	require.NoError(t, err)
	c, err := EncodeValue(s, vrw.Format())
	require.NoError(t, err)
	data := c.Data()
	buff := make([]byte, len(data)+1)
	copy(buff, data)
	buff[len(data)] = 5 // Add a bogus extrabyte
	assert.Panics(t, func() {
		_, err := decodeFromBytes(buff, vrw)
		require.NoError(t, err)
	})
}

func TestWriteStructWithList(t *testing.T) {
	vrw := newTestValueStore()

	// struct S {l: List<String>}({l: ["a", "b"]})
	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(1), /* len */
			"l", ListKind, uint64(0), uint64(2) /* len */, StringKind, "a", StringKind, "b",
		},
		mustValue(NewStruct(vrw.Format(), "S", StructData{"l": mustValue(NewList(context.Background(), vrw, String("a"), String("b")))})),
	)

	// struct S {l: List<>}({l: []})
	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(1), /* len */
			"l", ListKind, uint64(0), uint64(0), /* len */
		},
		mustValue(NewStruct(vrw.Format(), "S", StructData{"l": mustList(NewList(context.Background(), vrw))})),
	)
}

func TestWriteStructWithTuple(t *testing.T) {
	vrw := newTestValueStore()
	// struct S {l: List<String>}({l: ["a", "b"]})
	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(1), /* len */
			"t", TupleKind, uint64(2) /* len */, StringKind, "a", StringKind, "b",
		},
		mustValue(NewStruct(vrw.Format(), "S", StructData{"t": mustValue(NewTuple(vrw.Format(), String("a"), String("b")))})),
	)

	// struct S {l: List<>}({l: []})
	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(1), /* len */
			"t", TupleKind, uint64(0), /* len */
		},
		mustValue(NewStruct(vrw.Format(), "S", StructData{"t": mustValue(NewTuple(vrw.Format()))})),
	)
}

func TestWriteStructWithStruct(t *testing.T) {
	vrw := newTestValueStore()
	// struct S2 {
	//   x: Float
	// }
	// struct S {
	//   s: S2
	// }
	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(1), // len
			"s", StructKind, "S2", uint64(1), /* len */
			"x", FloatKind, Float(42),
		},
		// {s: {x: 42}}
		mustValue(NewStruct(vrw.Format(), "S", StructData{"s": mustValue(NewStruct(vrw.Format(), "S2", StructData{"x": Float(42)}))})),
	)
}

func TestWriteStructWithBlob(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(1), /* len */
			"b", BlobKind, uint64(0), []byte{0x00, 0x01},
		},
		mustValue(NewStruct(vrw.Format(), "S", StructData{"b": mustBlob(NewBlob(context.Background(), vrw, bytes.NewBuffer([]byte{0x00, 0x01})))})),
	)
}

func TestWriteCompoundList(t *testing.T) {
	vrw := newTestValueStore()

	list1 := newList(mustSeq(newListLeafSequence(vrw, Float(0))))
	list2 := newList(mustSeq(newListLeafSequence(vrw, Float(1), Float(2), Float(3))))
	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(1), uint64(2), // len,
			RefKind, mustHash(list1.Hash(vrw.Format())), ListKind, FloatKind, uint64(1), FloatKind, Float(1), uint64(1),
			RefKind, mustHash(list2.Hash(vrw.Format())), ListKind, FloatKind, uint64(1), FloatKind, Float(3), uint64(3),
		},
		newList(mustSeq(newListMetaSequence(1, []metaTuple{
			mustMetaTuple(newMetaTuple(mustRef(NewRef(list1, vrw.Format())), mustOrdKey(orderedKeyFromInt(1, vrw.Format())), 1)),
			mustMetaTuple(newMetaTuple(mustRef(NewRef(list2, vrw.Format())), mustOrdKey(orderedKeyFromInt(3, vrw.Format())), 3)),
		}, vrw))),
	)
}

func TestWriteCompoundSet(t *testing.T) {
	vrw := newTestValueStore()

	sls, err := newSetLeafSequence(vrw, Float(0), Float(1))
	require.NoError(t, err)
	sls2, err := newSetLeafSequence(vrw, Float(2), Float(3), Float(4))
	require.NoError(t, err)
	set1 := newSet(sls)
	set2 := newSet(sls2)

	assertEncoding(t,
		[]interface{}{
			SetKind, uint64(1), uint64(2), // len,
			RefKind, mustHash(set1.Hash(vrw.Format())), SetKind, FloatKind, uint64(1), FloatKind, Float(1), uint64(2),
			RefKind, mustHash(set2.Hash(vrw.Format())), SetKind, FloatKind, uint64(1), FloatKind, Float(4), uint64(3),
		},
		newSet(mustOrdSeq(newSetMetaSequence(1, []metaTuple{
			mustMetaTuple(newMetaTuple(mustRef(NewRef(set1, vrw.Format())), mustOrdKey(orderedKeyFromInt(1, vrw.Format())), 2)),
			mustMetaTuple(newMetaTuple(mustRef(NewRef(set2, vrw.Format())), mustOrdKey(orderedKeyFromInt(4, vrw.Format())), 3)),
		}, vrw))),
	)
}

func TestWriteCompoundSetOfBlobs(t *testing.T) {
	vrw := newTestValueStore()

	// Blobs are interesting because unlike the numbers used in TestWriteCompondSet, refs are sorted by their hashes, not their value.
	newBlobOfInt := func(i int) Blob {
		return mustBlob(NewBlob(context.Background(), vrw, strings.NewReader(strconv.Itoa(i))))
	}

	blob0 := newBlobOfInt(0)
	blob1 := newBlobOfInt(1)
	blob2 := newBlobOfInt(2)
	blob3 := newBlobOfInt(3)
	blob4 := newBlobOfInt(4)

	set1 := newSet(mustOrdSeq(newSetLeafSequence(vrw, blob0, blob1)))
	set2 := newSet(mustOrdSeq(newSetLeafSequence(vrw, blob2, blob3, blob4)))

	assertEncoding(t,
		[]interface{}{
			SetKind, uint64(1), uint64(2), // len,
			RefKind, mustHash(set1.Hash(vrw.Format())), SetKind, BlobKind, uint64(1), hashKind, mustHash(blob1.Hash(vrw.Format())), uint64(2),
			RefKind, mustHash(set2.Hash(vrw.Format())), SetKind, BlobKind, uint64(1), hashKind, mustHash(blob4.Hash(vrw.Format())), uint64(3),
		},
		newSet(mustOrdSeq(newSetMetaSequence(1, []metaTuple{
			mustMetaTuple(newMetaTuple(mustRef(NewRef(set1, vrw.Format())), mustOrdKey(newOrderedKey(blob1, vrw.Format())), 2)),
			mustMetaTuple(newMetaTuple(mustRef(NewRef(set2, vrw.Format())), mustOrdKey(newOrderedKey(blob4, vrw.Format())), 3)),
		}, vrw))),
	)
}

func TestWriteListOfUnion(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		// Note that the order of members in a union is determined based on a hash computation; the particular ordering of Float, Bool, String was determined empirically. This must not change unless deliberately and explicitly revving the persistent format.
		[]interface{}{
			ListKind, uint64(0),
			uint64(4) /* len */, StringKind, "0", FloatKind, Float(1), StringKind, "2", BoolKind, true,
		},
		mustList(NewList(context.Background(), vrw,
			String("0"),
			Float(1),
			String("2"),
			Bool(true),
		)),
	)
}

func TestWriteListOfStruct(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0), uint64(1), /* len */
			StructKind, "S", uint64(1) /* len */, "x", FloatKind, Float(42),
		},
		mustValue(NewList(context.Background(), vrw, mustValue(NewStruct(vrw.Format(), "S", StructData{"x": Float(42)})))),
	)
}

func TestWriteListOfUnionWithType(t *testing.T) {
	vrw := newTestValueStore()

	structType, err := MakeStructType("S", StructField{"x", PrimitiveTypeMap[FloatKind], false})
	require.NoError(t, err)

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0), uint64(4), /* len */
			BoolKind, true,
			TypeKind, FloatKind,
			TypeKind, TypeKind,
			TypeKind, StructKind, "S", uint64(1) /* len */, "x", FloatKind, false,
		},
		mustList(NewList(context.Background(), vrw,
			Bool(true),
			PrimitiveTypeMap[FloatKind],
			PrimitiveTypeMap[TypeKind],
			structType,
		)),
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

func TestWriteListOfTypes(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0), uint64(2), /* len */
			TypeKind, BoolKind, TypeKind, StringKind,
		},
		mustValue(NewList(context.Background(), vrw, PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind])),
	)
}

func TestWriteUnionList(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0), uint64(3), /* len */
			FloatKind, Float(23), StringKind, "hi", FloatKind, Float(42),
		},
		mustValue(NewList(context.Background(), vrw, Float(23), String("hi"), Float(42))),
	)
}

func TestWriteEmptyUnionList(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0), uint64(0), /* len */
		},
		mustValue(NewList(context.Background(), vrw)),
	)
}

func TestNomsBinFormat(t *testing.T) {
	v, _ := os.LookupEnv("DOLT_DEFAULT_BIN_FORMAT")
	switch v {
	case constants.Format718String:
		assert.Equal(t, Format_7_18, Format_Default)
	case constants.FormatLD1String:
		assert.Equal(t, Format_LD_1, Format_Default)
	case constants.FormatDolt1String:
		assert.Equal(t, Format_DOLT_1, Format_Default)
	case constants.FormatDoltDevString:
		assert.Equal(t, Format_DOLT_DEV, Format_Default)
	}
}
