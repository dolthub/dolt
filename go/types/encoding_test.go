// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

type nomsTestReader struct {
	a []interface{}
	i int
}

func (r *nomsTestReader) pos() uint32 {
	return uint32(r.i)
}

func (r *nomsTestReader) seek(pos uint32) {
	r.i = int(pos)
}

func (r *nomsTestReader) readIdent(tc *TypeCache) uint32 {
	s := r.readString()
	id, ok := tc.identTable.entries[s]
	if !ok {
		id = tc.identTable.GetId(s)
	}

	return id
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

func (r *nomsTestReader) readNumber() Number {
	return r.read().(Number)
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

func (w *nomsTestWriter) writeNumber(v Number) {
	w.write(v)
}

func (w *nomsTestWriter) writeBytes(v []byte) {
	w.write(v)
}

func (w *nomsTestWriter) writeHash(h hash.Hash) {
	w.writeString(h.String())
}

func (w *nomsTestWriter) appendType(t *Type) {
	enc := valueEncoder{w, nil}
	enc.writeType(t, nil)
}

func assertEncoding(t *testing.T, expect []interface{}, v Value) {
	vs := NewTestValueStore()
	tw := &nomsTestWriter{}
	enc := valueEncoder{tw, vs}
	enc.writeValue(v)
	assert.EqualValues(t, expect, tw.a)

	ir := &nomsTestReader{expect, 0}
	dec := valueDecoder{ir, vs, staticTypeCache}
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

	assertRoundTrips(Number(0))
	assertRoundTrips(Number(-0))
	assertRoundTrips(Number(math.Copysign(0, -1)))

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
		assertRoundTrips(Number(f))
		f = math.Copysign(f, -1)
		assertRoundTrips(Number(f))
	}
	floatTest := []float64{1.01, 1.001, 1.0001, 1.00001, 1.000001, 100.01, 1000.000001, 122.411912027329, 0.42}
	for _, f := range floatTest {
		assertRoundTrips(Number(f))
		f = math.Copysign(f, -1)
		assertRoundTrips(Number(f))
	}

	// JS Number.MAX_SAFE_INTEGER
	assertRoundTrips(Number(9007199254740991))
	// JS Number.MIN_SAFE_INTEGER
	assertRoundTrips(Number(-9007199254740991))
	assertRoundTrips(Number(math.MaxFloat64))
	assertRoundTrips(Number(math.Nextafter(1, 2) - 1))

	assertRoundTrips(String(""))
	assertRoundTrips(String("foo"))
	assertRoundTrips(String("AINT NO THANG"))
	assertRoundTrips(String("💩"))

	assertRoundTrips(NewStruct("", StructData{"a": Bool(true), "b": String("foo"), "c": Number(2.3)}))

	listLeaf := newList(newListLeafSequence(nil, Number(4), Number(5), Number(6), Number(7)))
	assertRoundTrips(listLeaf)

	assertRoundTrips(newList(newListMetaSequence([]metaTuple{
		newMetaTuple(NewRef(listLeaf), orderedKeyFromInt(10), 10, nil),
		newMetaTuple(NewRef(listLeaf), orderedKeyFromInt(20), 20, nil),
	}, nil)))
}

func TestNonFiniteNumbers(tt *testing.T) {
	vs := NewTestValueStore()
	t := func(f float64, s string) {
		v := Number(f)
		err := d.Try(func() {
			EncodeValue(v, vs)
		})
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), s)
	}
	t(math.NaN(), "NaN is not a supported number")
	t(math.Inf(1), "+Inf is not a supported number")
	t(math.Inf(-1), "-Inf is not a supported number")
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
			uint8(NumberKind), Number(0),
		},
		Number(0))

	assertEncoding(t,
		[]interface{}{
			uint8(NumberKind), Number(1000000000000000000),
		},
		Number(1e18))

	assertEncoding(t,
		[]interface{}{
			uint8(NumberKind), Number(10000000000000000000),
		},
		Number(1e19))

	assertEncoding(t,
		[]interface{}{
			uint8(NumberKind), Number(1e+20),
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
			uint8(ListKind), uint8(NumberKind), false, uint32(4) /* len */, uint8(NumberKind), Number(0), uint8(NumberKind), Number(1), uint8(NumberKind), Number(2), uint8(NumberKind), Number(3),
		},
		NewList(Number(0), Number(1), Number(2), Number(3)),
	)
}

func TestWriteListOfList(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(ListKind), uint8(ListKind), uint8(NumberKind), false,
			uint32(2), // len
			uint8(ListKind), uint8(NumberKind), false, uint32(1) /* len */, uint8(NumberKind), Number(0),
			uint8(ListKind), uint8(NumberKind), false, uint32(3) /* len */, uint8(NumberKind), Number(1), uint8(NumberKind), Number(2), uint8(NumberKind), Number(3),
		},
		NewList(NewList(Number(0)), NewList(Number(1), Number(2), Number(3))),
	)
}

func TestWriteSet(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(SetKind), uint8(NumberKind), false, uint32(4) /* len */, uint8(NumberKind), Number(0), uint8(NumberKind), Number(1), uint8(NumberKind), Number(2), uint8(NumberKind), Number(3),
		},
		NewSet(Number(3), Number(1), Number(2), Number(0)),
	)
}

func TestWriteSetOfSet(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(SetKind), uint8(SetKind), uint8(NumberKind), false,
			uint32(2), // len
			uint8(SetKind), uint8(NumberKind), false, uint32(3) /* len */, uint8(NumberKind), Number(1), uint8(NumberKind), Number(2), uint8(NumberKind), Number(3),
			uint8(SetKind), uint8(NumberKind), false, uint32(1) /* len */, uint8(NumberKind), Number(0),
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
			uint8(MapKind), uint8(StringKind), uint8(NumberKind), false, uint32(1) /* len */, uint8(StringKind), "a", uint8(NumberKind), Number(0),
			uint8(SetKind), uint8(BoolKind), false, uint32(1) /* len */, uint8(BoolKind), true,
		},
		NewMap(NewMap(String("a"), Number(0)), NewSet(Bool(true))),
	)
}

func TestWriteCompoundBlob(t *testing.T) {
	r1 := hash.Parse("00000000000000000000000000000001")
	r2 := hash.Parse("00000000000000000000000000000002")
	r3 := hash.Parse("00000000000000000000000000000003")

	assertEncoding(t,
		[]interface{}{
			uint8(BlobKind), true,
			uint32(3), // len
			uint8(RefKind), uint8(BlobKind), r1.String(), uint64(11), uint8(NumberKind), Number(20), uint64(20),
			uint8(RefKind), uint8(BlobKind), r2.String(), uint64(22), uint8(NumberKind), Number(40), uint64(40),
			uint8(RefKind), uint8(BlobKind), r3.String(), uint64(33), uint8(NumberKind), Number(60), uint64(60),
		},
		newBlob(newBlobMetaSequence([]metaTuple{
			newMetaTuple(constructRef(MakeRefType(BlobType), r1, 11), orderedKeyFromInt(20), 20, nil),
			newMetaTuple(constructRef(MakeRefType(BlobType), r2, 22), orderedKeyFromInt(40), 40, nil),
			newMetaTuple(constructRef(MakeRefType(BlobType), r3, 33), orderedKeyFromInt(60), 60, nil),
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
			uint8(BoolKind), true, uint8(NumberKind), Number(42),
		},
		NewStruct("S", StructData{"x": Number(42), "b": Bool(true)}),
	)
}

func TestWriteStructTooMuchData(t *testing.T) {
	s := NewStruct("S", StructData{"x": Number(42), "b": Bool(true)})
	c := EncodeValue(s, nil)
	data := c.Data()
	buff := make([]byte, len(data)+1)
	copy(buff, data)
	buff[len(data)] = 5 // Add a bogus extrabyte
	assert.Panics(t, func() {
		DecodeFromBytes(buff, nil, staticTypeCache)
	})
}

func TestWriteStructWithList(t *testing.T) {
	// struct S {l: List<String>}({l: ["a", "b"]})
	assertEncoding(t,
		[]interface{}{
			uint8(StructKind), "S", uint32(1) /* len */, "l", uint8(ListKind), uint8(StringKind),
			uint8(ListKind), uint8(StringKind), false, uint32(2) /* len */, uint8(StringKind), "a", uint8(StringKind), "b",
		},
		NewStruct("S", StructData{"l": NewList(String("a"), String("b"))}),
	)

	// struct S {l: List<>}({l: []})
	assertEncoding(t,
		[]interface{}{
			uint8(StructKind), "S", uint32(1) /* len */, "l", uint8(ListKind), uint8(UnionKind), uint32(0),
			uint8(ListKind), uint8(UnionKind), uint32(0), false, uint32(0), /* len */
		},
		NewStruct("S", StructData{"l": NewList()}),
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
			uint8(NumberKind), Number(42),
		},
		// {s: {x: 42}}
		NewStruct("S", StructData{"s": NewStruct("S2", StructData{"x": Number(42)})}),
	)
}

func TestWriteStructWithBlob(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(StructKind), "S", uint32(1) /* len */, "b", uint8(BlobKind), uint8(BlobKind), false, []byte{0x00, 0x01},
		},
		NewStruct("S", StructData{"b": NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))}),
	)
}

func TestWriteCompoundList(t *testing.T) {
	list1 := newList(newListLeafSequence(nil, Number(0)))
	list2 := newList(newListLeafSequence(nil, Number(1), Number(2), Number(3)))
	assertEncoding(t,
		[]interface{}{
			uint8(ListKind), uint8(NumberKind), true,
			uint32(2), // len,
			uint8(RefKind), uint8(ListKind), uint8(NumberKind), list1.Hash().String(), uint64(1), uint8(NumberKind), Number(1), uint64(1),
			uint8(RefKind), uint8(ListKind), uint8(NumberKind), list2.Hash().String(), uint64(1), uint8(NumberKind), Number(3), uint64(3),
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
			uint8(RefKind), uint8(SetKind), uint8(NumberKind), set1.Hash().String(), uint64(1), uint8(NumberKind), Number(1), uint64(2),
			uint8(RefKind), uint8(SetKind), uint8(NumberKind), set2.Hash().String(), uint64(1), uint8(NumberKind), Number(4), uint64(3),
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
		// Note that the order of members in a union is determined based on a hash computation; the particular ordering of Number, Bool, String was determined empirically. This must not change unless deliberately and explicitly revving the persistent format.
		[]interface{}{
			uint8(ListKind), uint8(UnionKind), uint32(3) /* len */, uint8(NumberKind), uint8(BoolKind), uint8(StringKind), false,
			uint32(4) /* len */, uint8(StringKind), "0", uint8(NumberKind), Number(1), uint8(StringKind), "2", uint8(BoolKind), true,
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
			uint32(1) /* len */, uint8(StructKind), "S", uint32(1) /* len */, "x", uint8(NumberKind), uint8(NumberKind), Number(42),
		},
		NewList(NewStruct("S", StructData{"x": Number(42)})),
	)
}

func TestWriteListOfUnionWithType(t *testing.T) {
	structType := MakeStructType("S", []string{"x"}, []*Type{NumberType})

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

func TestWriteRef(t *testing.T) {
	typ := MakeRefType(NumberType)
	r := hash.Parse("0123456789abcdefghijklmnopqrstuv")

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

	structType := MakeStructType("A6",
		[]string{"cs", "v"},
		[]*Type{
			MakeListType(MakeCycleType(0)),
			NumberType,
		})

	assertEncoding(t,
		[]interface{}{
			uint8(StructKind), "A6", uint32(2) /* len */, "cs", uint8(ListKind), uint8(CycleKind), uint32(0), "v", uint8(NumberKind),
			uint8(ListKind), uint8(UnionKind), uint32(0) /* len */, false, uint32(0), /* len */
			uint8(NumberKind), Number(42),
		},
		// {v: 42, cs: [{v: 555, cs: []}]}
		NewStructWithType(structType, ValueSlice{NewList(), Number(42)}),
	)
}

func TestWriteUnionList(t *testing.T) {
	assertEncoding(t,
		[]interface{}{
			uint8(ListKind), uint8(UnionKind), uint32(2) /* len */, uint8(NumberKind), uint8(StringKind),
			false, uint32(3) /* len */, uint8(NumberKind), Number(23), uint8(StringKind), "hi", uint8(NumberKind), Number(42),
		},
		NewList(Number(23), String("hi"), Number(42)),
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

type bogusType int

func (bg bogusType) Equals(other Value) bool     { return false }
func (bg bogusType) Less(other Value) bool       { return false }
func (bg bogusType) Hash() hash.Hash             { return hash.Hash{} }
func (bg bogusType) WalkValues(cb ValueCallback) {}
func (bg bogusType) WalkRefs(cb RefCallback)     {}
func (bg bogusType) Type() *Type {
	return MakeCycleType(0)
}

func TestBogusValueWithUnresolvedCycle(t *testing.T) {
	g := bogusType(1)
	assert.Panics(t, func() {
		EncodeValue(g, nil)
	})
}
