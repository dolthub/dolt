// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/stretchr/testify/assert"
)

func toBinaryNomsReaderData(data []interface{}) []byte {
	w := newBinaryNomsWriter()
	for i, v := range data {
		switch v := v.(type) {
		case uint8:
			w.writeUint8(v)
		case string:
			w.writeString(v)
		case Float:
			w.writeFloat(v, Format_7_18)
		case uint64:
			w.writeCount(v)
		case bool:
			w.writeBool(v)
		case hash.Hash:
			w.writeHash(v)
		case []byte:
			w.writeCount(uint64(len(v)))
			w.writeBytes(v)
		case NomsKind:
			w.writeUint8(uint8(v))
		default:
			panic("unreachable at index " + strconv.FormatInt(int64(i), 10))
		}
	}
	return w.data()
}

func assertEncoding(t *testing.T, expect []interface{}, v Value) {
	expectedAsByteSlice := toBinaryNomsReaderData(expect)
	vs := newTestValueStore()
	w := newBinaryNomsWriter()
	// TODO(binformat)
	v.writeTo(&w, Format_7_18)
	assert.EqualValues(t, expectedAsByteSlice, w.data())

	dec := newValueDecoder(expectedAsByteSlice, vs)
	// TODO(binformat)
	v2 := dec.readValue(Format_7_18)
	assert.True(t, v.Equals(v2))
}

func TestRoundTrips(t *testing.T) {
	vs := newTestValueStore()

	assertRoundTrips := func(v Value) {
		out := DecodeValue(EncodeValue(v, Format_7_18), vs)
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

	assertRoundTrips(NewStruct(Format_7_18, "", StructData{"a": Bool(true), "b": String("foo"), "c": Float(2.3)}))

	listLeaf := newList(newListLeafSequence(vs, Float(4), Float(5), Float(6), Float(7)))
	assertRoundTrips(listLeaf)

	assertRoundTrips(newList(newListMetaSequence(1, []metaTuple{
		newMetaTuple(NewRef(listLeaf, Format_7_18), orderedKeyFromInt(10, Format_7_18), 10),
		newMetaTuple(NewRef(listLeaf, Format_7_18), orderedKeyFromInt(20, Format_7_18), 20),
	}, vs)))
}

func TestNonFiniteNumbers(tt *testing.T) {
	t := func(f float64) (err error) {
		// TODO: fix panics
		defer func() {
			if r := recover(); r != nil {
				err = r.(error)
			}
		}()

		v := Float(f)
		err := d.Try(func() {
			// TODO(binformat)
			EncodeValue(v, Format_7_18)
		})
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), s)
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
		NewBlob(context.Background(), vrw, bytes.NewBuffer([]byte{0x00, 0x01})),
	)
}

func TestWriteList(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0), uint64(4) /* len */, FloatKind, Float(0), FloatKind, Float(1), FloatKind, Float(2), FloatKind, Float(3),
		},
		// TODO(binformat)
		NewList(context.Background(), vrw, Float(0), Float(1), Float(2), Float(3)),
	)
}
func TestWriteTuple(t *testing.T) {

	assertEncoding(t,
		[]interface{}{
			TupleKind, uint64(4) /* len */, FloatKind, Float(0), FloatKind, Float(1), FloatKind, Float(2), FloatKind, Float(3),
		},
		NewTuple(Format_7_18, Float(0), Float(1), Float(2), Float(3)),
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
		// TODO(binformat)
		NewList(context.Background(), vrw,
			NewList(context.Background(), vrw, Float(0)),
			NewList(context.Background(), vrw, Float(1), Float(2), Float(3))),
	)
}

func TestWriteSet(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			SetKind, uint64(0), uint64(4), /* len */
			FloatKind, Float(0), FloatKind, Float(1), FloatKind, Float(2), FloatKind, Float(3),
		},
		NewSet(context.Background(), vrw, Float(3), Float(1), Float(2), Float(0)),
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
		NewSet(context.Background(), vrw,
			NewSet(context.Background(), vrw, Float(0)),
			NewSet(context.Background(), vrw, Float(1), Float(2), Float(3))),
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
		NewMap(context.Background(), vrw, String("a"), Bool(false), String("b"), Bool(true), String("c"), NewTuple(Format_7_18, Float(1.0))),
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
		NewMap(context.Background(), vrw,
			NewMap(context.Background(), vrw, String("a"), Float(0)),
			NewSet(context.Background(), vrw, Bool(true))),
	)
}

func TestWriteCompoundBlob(t *testing.T) {
	r1 := hash.Parse("00000000000000000000000000000001")
	r2 := hash.Parse("00000000000000000000000000000002")
	r3 := hash.Parse("00000000000000000000000000000003")

	assertEncoding(t,
		[]interface{}{
			BlobKind, uint64(1),
			uint64(3), // len
			RefKind, r1, BlobKind, uint64(11), FloatKind, Float(20), uint64(20),
			RefKind, r2, BlobKind, uint64(22), FloatKind, Float(40), uint64(40),
			RefKind, r3, BlobKind, uint64(33), FloatKind, Float(60), uint64(60),
		},
		newBlob(newBlobMetaSequence(1, []metaTuple{
			newMetaTuple(constructRef(Format_7_18, r1, BlobType, 11), orderedKeyFromInt(20, Format_7_18), 20),
			newMetaTuple(constructRef(Format_7_18, r2, BlobType, 22), orderedKeyFromInt(40, Format_7_18), 40),
			newMetaTuple(constructRef(Format_7_18, r3, BlobType, 33), orderedKeyFromInt(60, Format_7_18), 60),
		}, newTestValueStore())),
	)
}

func TestWriteEmptyStruct(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(0), /* len */
		},
		NewStruct(Format_7_18, "S", nil),
	)
}

func TestWriteEmptyTuple(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			TupleKind, uint64(0), /* len */
		},
		NewTuple(Format_7_18),
	)
}

func TestWriteStruct(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(2), /* len */
			"b", BoolKind, true, "x", FloatKind, Float(42),
		},
		NewStruct(Format_7_18, "S", StructData{"x": Float(42), "b": Bool(true)}),
	)
}

func TestWriteStructTooMuchData(t *testing.T) {
	s := NewStruct(Format_7_18, "S", StructData{"x": Float(42), "b": Bool(true)})
	c := EncodeValue(s, Format_7_18)
	data := c.Data()
	buff := make([]byte, len(data)+1)
	copy(buff, data)
	buff[len(data)] = 5 // Add a bogus extrabyte
	assert.Panics(t, func() {
		// TODO(binformat)
		decodeFromBytes(buff, nil, Format_7_18)
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
		// TODO(binformat)
		NewStruct(Format_7_18, "S", StructData{"l": NewList(context.Background(), vrw, String("a"), String("b"))}),
	)

	// struct S {l: List<>}({l: []})
	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(1), /* len */
			"l", ListKind, uint64(0), uint64(0), /* len */
		},
		// TODO(binformat)
		NewStruct(Format_7_18, "S", StructData{"l": NewList(context.Background(), vrw)}),
	)
}

func TestWriteStructWithTuple(t *testing.T) {
	// struct S {l: List<String>}({l: ["a", "b"]})
	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(1), /* len */
			"t", TupleKind, uint64(2) /* len */, StringKind, "a", StringKind, "b",
		},
		NewStruct(Format_7_18, "S", StructData{"t": NewTuple(Format_7_18, String("a"), String("b"))}),
	)

	// struct S {l: List<>}({l: []})
	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(1), /* len */
			"t", TupleKind, uint64(0), /* len */
		},
		NewStruct(Format_7_18, "S", StructData{"t": NewTuple(Format_7_18)}),
	)
}

func TestWriteStructWithStruct(t *testing.T) {
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
		NewStruct(Format_7_18, "S", StructData{"s": NewStruct(Format_7_18, "S2", StructData{"x": Float(42)})}),
	)
}

func TestWriteStructWithBlob(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			StructKind, "S", uint64(1), /* len */
			"b", BlobKind, uint64(0), []byte{0x00, 0x01},
		},
		NewStruct(Format_7_18, "S", StructData{"b": NewBlob(context.Background(), vrw, bytes.NewBuffer([]byte{0x00, 0x01}))}),
	)
}

func TestWriteCompoundList(t *testing.T) {
	vrw := newTestValueStore()

	// TODO(binformat)
	list1 := newList(newListLeafSequence(vrw, Float(0)))
	list2 := newList(newListLeafSequence(vrw, Float(1), Float(2), Float(3)))
	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(1), uint64(2), // len,
			RefKind, list1.Hash(Format_7_18), ListKind, FloatKind, uint64(1), FloatKind, Float(1), uint64(1),
			RefKind, list2.Hash(Format_7_18), ListKind, FloatKind, uint64(1), FloatKind, Float(3), uint64(3),
		},
		newList(newListMetaSequence(1, []metaTuple{
			newMetaTuple(NewRef(list1, Format_7_18), orderedKeyFromInt(1, Format_7_18), 1),
			newMetaTuple(NewRef(list2, Format_7_18), orderedKeyFromInt(3, Format_7_18), 3),
		}, vrw)),
	)
}

func TestWriteCompoundSet(t *testing.T) {
	vrw := newTestValueStore()

	set1 := newSet(newSetLeafSequence(vrw, Float(0), Float(1)))
	set2 := newSet(newSetLeafSequence(vrw, Float(2), Float(3), Float(4)))

	assertEncoding(t,
		[]interface{}{
			SetKind, uint64(1), uint64(2), // len,
			// TODO(binformat)
			RefKind, set1.Hash(Format_7_18), SetKind, FloatKind, uint64(1), FloatKind, Float(1), uint64(2),
			RefKind, set2.Hash(Format_7_18), SetKind, FloatKind, uint64(1), FloatKind, Float(4), uint64(3),
		},
		newSet(newSetMetaSequence(1, []metaTuple{
			newMetaTuple(NewRef(set1, Format_7_18), orderedKeyFromInt(1, Format_7_18), 2),
			newMetaTuple(NewRef(set2, Format_7_18), orderedKeyFromInt(4, Format_7_18), 3),
		}, vrw)),
	)
}

func TestWriteCompoundSetOfBlobs(t *testing.T) {
	vrw := newTestValueStore()

	// Blobs are interesting because unlike the numbers used in TestWriteCompondSet, refs are sorted by their hashes, not their value.
	newBlobOfInt := func(i int) Blob {
		return NewBlob(context.Background(), vrw, strings.NewReader(strconv.Itoa(i)))
	}

	blob0 := newBlobOfInt(0)
	blob1 := newBlobOfInt(1)
	blob2 := newBlobOfInt(2)
	blob3 := newBlobOfInt(3)
	blob4 := newBlobOfInt(4)

	set1 := newSet(newSetLeafSequence(vrw, blob0, blob1))
	set2 := newSet(newSetLeafSequence(vrw, blob2, blob3, blob4))

	assertEncoding(t,
		[]interface{}{
			SetKind, uint64(1), uint64(2), // len,
			// TODO(binformat)
			RefKind, set1.Hash(Format_7_18), SetKind, BlobKind, uint64(1), hashKind, blob1.Hash(Format_7_18), uint64(2),
			RefKind, set2.Hash(Format_7_18), SetKind, BlobKind, uint64(1), hashKind, blob4.Hash(Format_7_18), uint64(3),
		},
		newSet(newSetMetaSequence(1, []metaTuple{
			newMetaTuple(NewRef(set1, Format_7_18), newOrderedKey(blob1, Format_7_18), 2),
			newMetaTuple(NewRef(set2, Format_7_18), newOrderedKey(blob4, Format_7_18), 3),
		}, vrw)),
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
		// TODO(binformat)
		NewList(context.Background(), vrw,
			String("0"),
			Float(1),
			String("2"),
			Bool(true),
		),
	)
}

func TestWriteListOfStruct(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0), uint64(1), /* len */
			StructKind, "S", uint64(1) /* len */, "x", FloatKind, Float(42),
		},
		// TODO(binformat)
		NewList(context.Background(), vrw, NewStruct(Format_7_18, "S", StructData{"x": Float(42)})),
	)
}

func TestWriteListOfUnionWithType(t *testing.T) {
	vrw := newTestValueStore()

	structType := MakeStructType("S", StructField{"x", FloaTType, false})

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0), uint64(4), /* len */
			BoolKind, true,
			TypeKind, FloatKind,
			TypeKind, TypeKind,
			TypeKind, StructKind, "S", uint64(1) /* len */, "x", FloatKind, false,
		},
		// TODO(binformat)
		NewList(context.Background(), vrw,
			Bool(true),
			FloaTType,
			TypeType,
			structType,
		),
	)
}

func TestWriteRef(t *testing.T) {
	r := hash.Parse("0123456789abcdefghijklmnopqrstuv")

	assertEncoding(t,
		[]interface{}{
			RefKind, r, FloatKind, uint64(4),
		},
		constructRef(Format_7_18, r, FloaTType, 4),
	)
}

func TestWriteListOfTypes(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0), uint64(2), /* len */
			TypeKind, BoolKind, TypeKind, StringKind,
		},
		// TODO(binformat)
		NewList(context.Background(), vrw, BoolType, StringType),
	)
}

func TestWriteUnionList(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0), uint64(3), /* len */
			FloatKind, Float(23), StringKind, "hi", FloatKind, Float(42),
		},
		// TODO(binformat)
		NewList(context.Background(), vrw, Float(23), String("hi"), Float(42)),
	)
}

func TestWriteEmptyUnionList(t *testing.T) {
	vrw := newTestValueStore()

	assertEncoding(t,
		[]interface{}{
			ListKind, uint64(0), uint64(0), /* len */
		},
		// TODO(binformat)
		NewList(context.Background(), vrw),
	)
}

type bogusType int

func (bg bogusType) Value(ctx context.Context) Value                  { return bg }
func (bg bogusType) Equals(other Value) bool                          { return false }
func (bg bogusType) Less(f *Format, other LesserValuable) bool        { return false }
func (bg bogusType) Hash(*Format) hash.Hash                           { return hash.Hash{} }
func (bg bogusType) WalkValues(ctx context.Context, cb ValueCallback) {}
func (bg bogusType) WalkRefs(f *Format, cb RefCallback)               {}
func (bg bogusType) Kind() NomsKind {
	return CycleKind
}
func (bg bogusType) typeOf() *Type {
	return MakeCycleType("ABC")
}
func (bg bogusType) writeTo(w nomsWriter, f *Format) {
	panic("abc")
}

func TestBogusValueWithUnresolvedCycle(t *testing.T) {
	g := bogusType(1)
	assert.Panics(t, func() {
		// TODO(binformat)
		EncodeValue(g, Format_7_18)
	})
}
