// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

type nomsTestReader struct {
	a []interface{}
	i int
}

func (r *nomsTestReader) read() interface{} {
	v := r.a[r.i]
	r.i++
	return v
}

func (r *nomsTestReader) atEnd() bool {
	return r.i >= len(r.a)
}

func (r *nomsTestReader) readString() string {
	return r.read().(string)
}

func (r *nomsTestReader) readBool() bool {
	return r.read().(bool)
}

func (r *nomsTestReader) readUint8() uint8 {
	return r.read().(uint8)
}

func (r *nomsTestReader) readUint32() uint32 {
	return r.read().(uint32)
}

func (r *nomsTestReader) readUint64() uint64 {
	return r.read().(uint64)
}

func (r *nomsTestReader) readFloat64() float64 {
	return r.read().(float64)
}

func (r *nomsTestReader) readBytes() []byte {
	return r.read().([]byte)
}

func (r *nomsTestReader) readHash() hash.Hash {
	return hash.Parse(r.readString())
}

type nomsTestWriter struct {
	a []interface{}
}

func (w *nomsTestWriter) write(v interface{}) {
	w.a = append(w.a, v)
}

func (w *nomsTestWriter) writeString(s string) {
	w.write(s)
}

func (w *nomsTestWriter) writeBool(b bool) {
	w.write(b)
}

func (w *nomsTestWriter) writeUint8(v uint8) {
	w.write(v)
}

func (w *nomsTestWriter) writeUint32(v uint32) {
	w.write(v)
}

func (w *nomsTestWriter) writeUint64(v uint64) {
	w.write(v)
}

func (w *nomsTestWriter) writeFloat64(v float64) {
	w.write(v)
}

func (w *nomsTestWriter) writeBytes(v []byte) {
	w.write(v)
}

func (w *nomsTestWriter) writeHash(h hash.Hash) {
	w.writeString(h.String())
}

func assertEncoding(t *testing.T, expect []interface{}, v Value) {
	vs := NewTestValueStore()
	tw := &nomsTestWriter{}
	enc := valueEncoder{tw, vs}
	enc.writeValue(v)
	assert.EqualValues(t, expect, tw.a)

	ir := &nomsTestReader{expect, 0}
	dec := valueDecoder{ir, vs}
	v2 := dec.readValue()
	assert.True(t, ir.atEnd())
	assert.True(t, v.Equals(v2))
}

func TestRoundTrips(t *testing.T) {
	assertRoundTrips := func(v Value) {
		vs := NewTestValueStore()
		out := DecodeValue(EncodeValue(v, vs), vs)
		assert.True(t, v.Equals(out))
	}

	assertRoundTrips(Bool(false))
	assertRoundTrips(Bool(true))

	assertRoundTrips(Number(1))
	assertRoundTrips(Number(-0))
	assertRoundTrips(Number(0))
	assertRoundTrips(Number(1))

	assertRoundTrips(String(""))
	assertRoundTrips(String("foo"))
	assertRoundTrips(String("AINT NO THANG"))
	assertRoundTrips(String("💩"))

	assertRoundTrips(NewStruct("", structData{"a": Bool(true), "b": String("foo"), "c": Number(2.3)}))

	listLeaf := newList(newListLeafSequence(nil, Number(4), Number(5), Number(6), Number(7)))
	assertRoundTrips(listLeaf)

	assertRoundTrips(newList(newListMetaSequence([]metaTuple{
		newMetaTuple(NewRef(listLeaf), orderedKeyFromInt(10), 10, nil),
		newMetaTuple(NewRef(listLeaf), orderedKeyFromInt(20), 20, nil),
	}, nil)))
}

func TestWritePrimitives(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(BoolKind), true,
		},
		Bool(true))

	assertEncoding(t,
		[]interface{}{
			uint8(BoolKind), false,
		},
		Bool(false))

	assertEncoding(t,
		[]interface{}{
			uint8(NumberKind), float64(0),
		},
		Number(0))

	assertEncoding(t,
		[]interface{}{
			uint8(NumberKind), float64(1000000000000000000),
		},
		Number(1e18))

	assertEncoding(t,
		[]interface{}{
			uint8(NumberKind), float64(10000000000000000000),
		},
		Number(1e19))

	assertEncoding(t,
		[]interface{}{
			uint8(NumberKind), float64(1e+20),
		},
		Number(1e20))

	assertEncoding(t,
		[]interface{}{
			uint8(StringKind), "hi",
		},
		String("hi"))
}

func TestWriteSimpleBlob(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(BlobKind), false, []byte{0x00, 0x01},
		},
		NewBlob(bytes.NewBuffer([]byte{0x00, 0x01})),
	)
}

func TestWriteList(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(ListKind), uint8(NumberKind), false, uint32(4) /* len */, uint8(NumberKind), float64(0), uint8(NumberKind), float64(1), uint8(NumberKind), float64(2), uint8(NumberKind), float64(3),
		},
		NewList(Number(0), Number(1), Number(2), Number(3)),
	)
}

func TestWriteListOfList(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(ListKind), uint8(ListKind), uint8(NumberKind), false,
			uint32(2), // len
			uint8(ListKind), uint8(NumberKind), false, uint32(1) /* len */, uint8(NumberKind), float64(0),
			uint8(ListKind), uint8(NumberKind), false, uint32(3) /* len */, uint8(NumberKind), float64(1), uint8(NumberKind), float64(2), uint8(NumberKind), float64(3),
		},
		NewList(NewList(Number(0)), NewList(Number(1), Number(2), Number(3))),
	)
}

func TestWriteSet(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(SetKind), uint8(NumberKind), false, uint32(4) /* len */, uint8(NumberKind), float64(0), uint8(NumberKind), float64(1), uint8(NumberKind), float64(2), uint8(NumberKind), float64(3),
		},
		NewSet(Number(3), Number(1), Number(2), Number(0)),
	)
}

func TestWriteSetOfSet(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(SetKind), uint8(SetKind), uint8(NumberKind), false,
			uint32(2), // len
			uint8(SetKind), uint8(NumberKind), false, uint32(1) /* len */, uint8(NumberKind), float64(0),
			uint8(SetKind), uint8(NumberKind), false, uint32(3) /* len */, uint8(NumberKind), float64(1), uint8(NumberKind), float64(2), uint8(NumberKind), float64(3),
		},
		NewSet(NewSet(Number(0)), NewSet(Number(1), Number(2), Number(3))),
	)
}

func TestWriteMap(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(MapKind), uint8(StringKind), uint8(BoolKind), false, uint32(2) /* len */, uint8(StringKind), "a", uint8(BoolKind), false, uint8(StringKind), "b", uint8(BoolKind), true,
		},
		NewMap(String("a"), Bool(false), String("b"), Bool(true)),
	)
}

func TestWriteMapOfMap(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(MapKind), uint8(MapKind), uint8(StringKind), uint8(NumberKind), uint8(SetKind), uint8(BoolKind), false,
			uint32(1), // len
			uint8(MapKind), uint8(StringKind), uint8(NumberKind), false, uint32(1) /* len */, uint8(StringKind), "a", uint8(NumberKind), float64(0),
			uint8(SetKind), uint8(BoolKind), false, uint32(1) /* len */, uint8(BoolKind), true,
		},
		NewMap(NewMap(String("a"), Number(0)), NewSet(Bool(true))),
	)
}

func TestWriteCompoundBlob(t *testing.T) {
	r1 := hash.Parse("sha1-0000000000000000000000000000000000000001")
	r2 := hash.Parse("sha1-0000000000000000000000000000000000000002")
	r3 := hash.Parse("sha1-0000000000000000000000000000000000000003")

	assertEncoding(t,
		[]interface{}{
			uint8(BlobKind), true,
			uint32(3), // len
			uint8(RefKind), uint8(BlobKind), r1.String(), uint64(11), uint8(NumberKind), float64(20), uint64(20),
			uint8(RefKind), uint8(BlobKind), r2.String(), uint64(22), uint8(NumberKind), float64(40), uint64(40),
			uint8(RefKind), uint8(BlobKind), r3.String(), uint64(33), uint8(NumberKind), float64(60), uint64(60),
		},
		newBlob(newBlobMetaSequence([]metaTuple{
			newMetaTuple(constructRef(RefOfBlobType, r1, 11), orderedKeyFromInt(20), 20, nil),
			newMetaTuple(constructRef(RefOfBlobType, r2, 22), orderedKeyFromInt(40), 40, nil),
			newMetaTuple(constructRef(RefOfBlobType, r3, 33), orderedKeyFromInt(60), 60, nil),
		}, NewTestValueStore())),
	)
}

func TestWriteEmptyStruct(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(StructKind), "S", uint32(0), /* len */
		},
		NewStruct("S", nil),
	)
}

func TestWriteStruct(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(StructKind), "S", uint32(2) /* len */, "b", uint8(BoolKind), "x", uint8(NumberKind),
			uint8(BoolKind), true, uint8(NumberKind), float64(42),
		},
		NewStruct("S", structData{"x": Number(42), "b": Bool(true)}),
	)
}

func TestWriteStructWithList(t *testing.T) {
	// struct S {l: List<String>}({l: ["a", "b"]})
	assertEncoding(t,
		[]interface{}{
			uint8(StructKind), "S", uint32(1) /* len */, "l", uint8(ListKind), uint8(StringKind),
			uint8(ListKind), uint8(StringKind), false, uint32(2) /* len */, uint8(StringKind), "a", uint8(StringKind), "b",
		},
		NewStruct("S", structData{"l": NewList(String("a"), String("b"))}),
	)

	// struct S {l: List<>}({l: []})
	assertEncoding(t,
		[]interface{}{
			uint8(StructKind), "S", uint32(1) /* len */, "l", uint8(ListKind), uint8(UnionKind), uint32(0),
			uint8(ListKind), uint8(UnionKind), uint32(0), false, uint32(0), /* len */
		},
		NewStruct("S", structData{"l": NewList()}),
	)
}

func TestWriteStructWithStruct(t *testing.T) {
	// struct S2 {
	//   x: Number
	// }
	// struct S {
	//   s: S2
	// }
	assertEncoding(t,
		[]interface{}{
			uint8(StructKind), "S",
			uint32(1), // len
			"s", uint8(StructKind), "S2", uint32(1) /* len */, "x", uint8(NumberKind),
			uint8(StructKind), "S2", uint32(1) /* len */, "x", uint8(NumberKind),
			uint8(NumberKind), float64(42),
		},
		// {s: {x: 42}}
		NewStruct("S", structData{"s": NewStruct("S2", structData{"x": Number(42)})}),
	)
}

func TestWriteStructWithBlob(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(StructKind), "S", uint32(1) /* len */, "b", uint8(BlobKind), uint8(BlobKind), false, []byte{0x00, 0x01},
		},
		NewStruct("S", structData{"b": NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))}),
	)
}

func TestWriteCompoundList(t *testing.T) {
	list1 := newList(newListLeafSequence(nil, Number(0)))
	list2 := newList(newListLeafSequence(nil, Number(1), Number(2), Number(3)))
	assertEncoding(t,
		[]interface{}{
			uint8(ListKind), uint8(NumberKind), true,
			uint32(2), // len,
			uint8(RefKind), uint8(ListKind), uint8(NumberKind), list1.Hash().String(), uint64(1), uint8(NumberKind), float64(1), uint64(1),
			uint8(RefKind), uint8(ListKind), uint8(NumberKind), list2.Hash().String(), uint64(1), uint8(NumberKind), float64(3), uint64(3),
		},
		newList(newListMetaSequence([]metaTuple{
			newMetaTuple(NewRef(list1), orderedKeyFromInt(1), 1, list1),
			newMetaTuple(NewRef(list2), orderedKeyFromInt(3), 3, list2),
		}, nil)),
	)
}

func TestWriteCompoundSet(t *testing.T) {
	set1 := newSet(newSetLeafSequence(nil, Number(0), Number(1)))
	set2 := newSet(newSetLeafSequence(nil, Number(2), Number(3), Number(4)))

	assertEncoding(t,
		[]interface{}{
			uint8(SetKind), uint8(NumberKind), true,
			uint32(2), // len,
			uint8(RefKind), uint8(SetKind), uint8(NumberKind), set1.Hash().String(), uint64(1), uint8(NumberKind), float64(1), uint64(2),
			uint8(RefKind), uint8(SetKind), uint8(NumberKind), set2.Hash().String(), uint64(1), uint8(NumberKind), float64(4), uint64(3),
		},
		newSet(newSetMetaSequence([]metaTuple{
			newMetaTuple(NewRef(set1), orderedKeyFromInt(1), 2, set1),
			newMetaTuple(NewRef(set2), orderedKeyFromInt(4), 3, set2),
		}, nil)),
	)
}

func TestWriteCompoundSetOfBlobs(t *testing.T) {
	// Blobs are interesting because unlike the numbers used in TestWriteCompondSet, refs are sorted by their hashes, not their value.
	newBlobOfInt := func(i int) Blob {
		return NewBlob(strings.NewReader(strconv.Itoa(i)))
	}

	blob0 := newBlobOfInt(0)
	blob1 := newBlobOfInt(1)
	blob2 := newBlobOfInt(2)
	blob3 := newBlobOfInt(3)
	blob4 := newBlobOfInt(4)

	set1 := newSet(newSetLeafSequence(nil, blob0, blob1))
	set2 := newSet(newSetLeafSequence(nil, blob2, blob3, blob4))

	assertEncoding(t,
		[]interface{}{
			uint8(SetKind), uint8(BlobKind), true,
			uint32(2), // len,
			// See https://github.com/attic-labs/noms/issues/1688#issuecomment-227528987
			uint8(RefKind), uint8(SetKind), uint8(BlobKind), set1.Hash().String(), uint64(1), uint8(RefKind), uint8(BoolKind), blob1.Hash().String(), uint64(0), uint64(2),
			uint8(RefKind), uint8(SetKind), uint8(BlobKind), set2.Hash().String(), uint64(1), uint8(RefKind), uint8(BoolKind), blob4.Hash().String(), uint64(0), uint64(3),
		},
		newSet(newSetMetaSequence([]metaTuple{
			newMetaTuple(NewRef(set1), newOrderedKey(blob1), 2, set1),
			newMetaTuple(NewRef(set2), newOrderedKey(blob4), 3, set2),
		}, nil)),
	)
}

func TestWriteListOfUnion(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(ListKind), uint8(UnionKind), uint32(3) /* len */, uint8(BoolKind), uint8(StringKind), uint8(NumberKind), false,
			uint32(4) /* len */, uint8(StringKind), "0", uint8(NumberKind), float64(1), uint8(StringKind), "2", uint8(BoolKind), true,
		},
		NewList(
			String("0"),
			Number(1),
			String("2"),
			Bool(true),
		),
	)
}

func TestWriteListOfStruct(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(ListKind), uint8(StructKind), "S", uint32(1) /* len */, "x", uint8(NumberKind), false,
			uint32(1) /* len */, uint8(StructKind), "S", uint32(1) /* len */, "x", uint8(NumberKind), uint8(NumberKind), float64(42),
		},
		NewList(NewStruct("S", structData{"x": Number(42)})),
	)
}

func TestWriteListOfUnionWithType(t *testing.T) {
	structType := MakeStructType("S", TypeMap{
		"x": NumberType,
	})

	assertEncoding(t,
		[]interface{}{
			uint8(ListKind), uint8(UnionKind), uint32(2) /* len */, uint8(BoolKind), uint8(TypeKind), false,
			uint32(4) /* len */, uint8(BoolKind), true, uint8(TypeKind), uint8(NumberKind), uint8(TypeKind), uint8(TypeKind), uint8(TypeKind), uint8(StructKind), "S", uint32(1) /* len */, "x", uint8(NumberKind),
		},
		NewList(
			Bool(true),
			NumberType,
			TypeType,
			structType,
		),
	)
}

func nomsTestWriteRef(t *testing.T) {
	typ := MakeRefType(NumberType)
	r := hash.Parse("sha1-0123456789abcdef0123456789abcdef01234567")

	assertEncoding(t,
		[]interface{}{
			uint8(RefKind), uint8(NumberKind), r.String(), uint64(4),
		},
		constructRef(typ, r, 4),
	)
}

func TestWriteListOfTypes(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(ListKind), uint8(TypeKind), false, uint32(2) /* len */, uint8(TypeKind), uint8(BoolKind), uint8(TypeKind), uint8(StringKind),
		},
		NewList(BoolType, StringType),
	)
}

func nomsTestWriteRecursiveStruct(t *testing.T) {
	// struct A6 {
	//   cs: List<A6>
	//   v: Number
	// }

	structType := MakeStructType("A6", TypeMap{
		"v":  NumberType,
		"cs": nil,
	})
	listType := MakeListType(structType)
	// Mutate...

	structType.Desc.(StructDesc).SetField("cs", listType)

	assertEncoding(t,
		[]interface{}{
			uint8(StructKind), "A6", uint32(2) /* len */, "cs", uint8(ListKind), uint8(CycleKind), uint32(0), "v", uint8(NumberKind),
			uint8(ListKind), uint8(UnionKind), uint32(0) /* len */, false, uint32(0), /* len */
			uint8(NumberKind), float64(42),
		},
		// {v: 42, cs: [{v: 555, cs: []}]}
		NewStructWithType(structType, structData{
			"v":  Number(42),
			"cs": NewList(),
		}),
	)
}

func TestWriteUnionList(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(ListKind), uint8(UnionKind), uint32(2) /* len */, uint8(StringKind), uint8(NumberKind),
			false, uint32(2) /* len */, uint8(StringKind), "hi", uint8(NumberKind), float64(42),
		},
		NewList(String("hi"), Number(42)),
	)
}

func TestWriteEmptyUnionList(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(ListKind), uint8(UnionKind), uint32(0) /* len */, false, uint32(0), /* len */
		},
		NewList(),
	)
}
