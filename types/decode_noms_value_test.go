// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/hash"
	"github.com/attic-labs/testify/assert"
)

func TestRead(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	a := []interface{}{Number(1), "hi", true}
	r := newJSONArrayReader(a, vs)

	assert.Equal(Number(1), r.read().(Number))
	assert.False(r.atEnd())

	assert.Equal("hi", r.readString())
	assert.False(r.atEnd())

	assert.Equal(true, r.readBool())
	assert.True(r.atEnd())
}

var replaceMap = map[string]NomsKind{
	"BoolKind":   BoolKind,
	"NumberKind": NumberKind,
	"StringKind": StringKind,
	"BlobKind":   BlobKind,
	"ValueKind":  ValueKind,
	"ListKind":   ListKind,
	"MapKind":    MapKind,
	"RefKind":    RefKind,
	"SetKind":    SetKind,
	"StructKind": StructKind,
	"TypeKind":   TypeKind,
	"ParentKind": ParentKind,
	"UnionKind":  UnionKind,
}
var kindRe = regexp.MustCompile(`(\w+Kind)`)

func parseJSON(s string, vs ...interface{}) (v []interface{}) {
	s = kindRe.ReplaceAllStringFunc(s, func(word string) string {
		i := replaceMap[word]
		return strconv.Itoa(int(i))
	})
	dec := json.NewDecoder(strings.NewReader(fmt.Sprintf(s, vs...)))
	dec.Decode(&v)
	d.Chk.NotEmpty(v, "Failed to parse JSON: %s", s)
	return
}

func TestReadType(t *testing.T) {
	vs := NewTestValueStore()

	test := func(expected *Type, s string, ks ...interface{}) {
		a := parseJSON(s, ks...)
		r := newJSONArrayReader(a, vs)
		tr := r.readType(nil)
		assert.True(t, expected.Equals(tr))
	}

	test(BoolType, "[%d, true]", BoolKind)
	test(TypeType, "[%d, %d]", TypeKind, BoolKind)
	test(MakeListType(BoolType), "[%d, %d, true, false]", ListKind, BoolKind)
}

func TestReadPrimitives(t *testing.T) {
	assert := assert.New(t)

	vs := NewTestValueStore()

	test := func(expected Value, s string) {
		a := parseJSON(s)
		r := newJSONArrayReader(a, vs)
		v := r.readValue()
		assert.True(expected.Equals(v))
	}

	test(Bool(true), "[BoolKind, true]")
	test(Bool(false), "[BoolKind, false]")
	test(Number(0), `[NumberKind, "0"]`)
	test(NewString("hi"), `[StringKind, "hi"]`)
	test(DecodeChunk(EncodeValue(NewString("hi"), vs), vs), `[StringKind, "hi"]`)

	blob := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	test(blob, `[BlobKind, false, "AAE="]`)
	test(DecodeChunk(EncodeValue(blob, vs), vs), `[BlobKind, false, "AAE="]`)
}

func TestReadListOfNumber(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	a := parseJSON(`[ListKind, NumberKind, false, [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`)
	r := newJSONArrayReader(a, vs)

	l := r.readValue()
	l2 := NewList(Number(0), Number(1), Number(2), Number(3))
	assert.True(l2.Equals(l))
	assert.True(l.Equals(DecodeChunk(EncodeValue(l2, vs), vs)))
}

func TestReadListOfMixedTypes(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	a := parseJSON(`[
		ListKind, UnionKind, 3, BoolKind, NumberKind, StringKind, false, [
			NumberKind, "1", StringKind, "hi", BoolKind, true
		]
	]`)
	r := newJSONArrayReader(a, vs)
	v := r.readValue().(List)

	tr := MakeListType(MakeUnionType(BoolType, NumberType, StringType))
	assert.True(v.Type().Equals(tr))
	assert.Equal(Number(1), v.Get(0))
	assert.Equal(NewString("hi"), v.Get(1))
	assert.Equal(Bool(true), v.Get(2))

	assert.True(v.Equals(DecodeChunk(EncodeValue(v, vs), vs)))
}

func TestReadSetOfMixedTypes(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	a := parseJSON(`[
		SetKind, UnionKind, 3, BoolKind, NumberKind, StringKind, false, [
			BoolKind, true, NumberKind, "1", StringKind, "hi"
		]
	]`)
	r := newJSONArrayReader(a, vs)
	v := r.readValue().(Set)

	tr := MakeSetType(MakeUnionType(BoolType, NumberType, StringType))
	assert.True(v.Type().Equals(tr))
	assert.True(v.Has(Number(1)))
	assert.True(v.Has(NewString("hi")))
	assert.True(v.Has(Bool(true)))

	assert.True(v.Equals(DecodeChunk(EncodeValue(v, vs), vs)))
}

func TestReadMapOfMixedTypes(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	a := parseJSON(`[
		MapKind, UnionKind, 2, BoolKind, NumberKind,
		UnionKind, 2, NumberKind, StringKind, false, [
			BoolKind, true, NumberKind, "1",
			NumberKind, "2", StringKind, "hi"
		]
	]`)
	r := newJSONArrayReader(a, vs)
	v := r.readValue().(Map)

	tr := MakeMapType(MakeUnionType(BoolType, NumberType), MakeUnionType(NumberType, StringType))
	assert.True(v.Type().Equals(tr))
	assert.True(v.Get(Bool(true)).Equals(Number(1)))
	assert.True(v.Get(Number(2)).Equals(NewString("hi")))

	assert.True(v.Equals(DecodeChunk(EncodeValue(v, vs), vs)))
}

func TestReadCompoundList(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	list1 := newList(newListLeafSequence(vs, Number(0)))
	list2 := newList(newListLeafSequence(vs, Number(1), Number(2), Number(3)))
	l2 := newList(newListMetaSequence([]metaTuple{
		newMetaTuple(Number(1), list1, NewRef(list1), 1),
		newMetaTuple(Number(4), list2, NewRef(list2), 4),
	}, vs))

	a := parseJSON(`[
		ListKind, NumberKind, true, [
			RefKind, ListKind, NumberKind, "%s", "1", NumberKind, "1", "1",
			RefKind, ListKind, NumberKind, "%s", "1", NumberKind, "4", "4"
		]
	]`, list1.Hash(), list2.Hash())
	r := newJSONArrayReader(a, vs)
	l := r.readValue()

	assert.True(l2.Equals(l))

	assert.True(l2.Equals(DecodeChunk(EncodeValue(l, vs), vs)))
}

func TestReadCompoundSet(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	set1 := newSet(newSetLeafSequence(vs, Number(0), Number(1)))
	set2 := newSet(newSetLeafSequence(vs, Number(2), Number(3), Number(4)))
	s2 := newSet(newSetMetaSequence([]metaTuple{
		newMetaTuple(Number(1), set1, NewRef(set1), 2),
		newMetaTuple(Number(4), set2, NewRef(set2), 3),
	}, vs))

	a := parseJSON(`[
		SetKind, NumberKind, true, [
			RefKind, SetKind, NumberKind, "%s", "1", NumberKind, "1", "2",
			RefKind, SetKind, NumberKind, "%s", "1", NumberKind, "4", "3"
		]
	]`, set1.Hash(), set2.Hash())
	r := newJSONArrayReader(a, vs)
	s := r.readValue()

	assert.True(s2.Equals(s))

	assert.True(s2.Equals(DecodeChunk(EncodeValue(s, vs), vs)))
}

func TestReadMapOfNumberToNumber(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	a := parseJSON(`[MapKind, NumberKind, NumberKind, false, [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`)
	r := newJSONArrayReader(a, vs)

	m := r.readValue()
	m2 := NewMap(Number(0), Number(1), Number(2), Number(3))
	assert.True(m2.Equals(m))

	assert.True(m2.Equals(DecodeChunk(EncodeValue(m, vs), vs)))
}

func TestReadSetOfNumber(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	a := parseJSON(`[SetKind, NumberKind, false, [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`)
	r := newJSONArrayReader(a, vs)

	s := r.readValue()
	s2 := NewSet(Number(0), Number(1), Number(2), Number(3))
	assert.True(s2.Equals(s))

	assert.True(s2.Equals(DecodeChunk(EncodeValue(s, vs), vs)))
}

func TestReadCompoundBlob(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	// Arbitrary valid refs.
	r1 := Number(1).Hash()
	r2 := Number(2).Hash()
	r3 := Number(3).Hash()
	a := parseJSON(`[
		BlobKind, true, [
			RefKind, BlobKind, "%s", "1", NumberKind, "20", "20",
			RefKind, BlobKind, "%s", "1", NumberKind, "40", "40",
			RefKind, BlobKind, "%s", "1", NumberKind, "60", "60"
		]
	]`, r1, r2, r3)
	r := newJSONArrayReader(a, vs)

	b := r.readValue()
	_, ok := b.(Blob)
	assert.True(ok)
	b2 := newBlob(newBlobMetaSequence([]metaTuple{
		newMetaTuple(Number(20), nil, constructRef(RefOfBlobType, r1, 1), 20),
		newMetaTuple(Number(40), nil, constructRef(RefOfBlobType, r2, 1), 40),
		newMetaTuple(Number(60), nil, constructRef(RefOfBlobType, r3, 1), 60),
	}, vs))

	assert.True(b.Type().Equals(b2.Type()))
	assert.Equal(b.Hash().String(), b2.Hash().String())

	assert.True(b2.Equals(DecodeChunk(EncodeValue(b, vs), vs)))
}

func TestReadStruct(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	typ := MakeStructType("A1", TypeMap{
		"x": NumberType,
		"s": StringType,
		"b": BoolType,
	})

	a := parseJSON(`[StructKind, "A1", ["b", BoolKind, "s", StringKind, "x", NumberKind], BoolKind, true, StringKind, "hi", NumberKind, "42"]`)
	r := newJSONArrayReader(a, vs)
	v := r.readValue().(Struct)

	assert.True(v.Type().Equals(typ))
	assert.True(v.Get("x").Equals(Number(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))
	assert.True(v.Get("b").Equals(Bool(true)))

	assert.True(v.Equals(DecodeChunk(EncodeValue(v, vs), vs)))
}

func TestReadStructWithList(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	// struct A4 {
	//   b: Bool
	//   l: List(Number)
	//   s: String
	// }

	typ := MakeStructType("A4", TypeMap{
		"b": BoolType,
		"l": MakeListType(NumberType),
		"s": StringType,
	})

	a := parseJSON(`[StructKind, "A4", ["b", BoolKind, "l", ListKind, NumberKind, "s", StringKind], BoolKind, true, ListKind, NumberKind, false, [NumberKind, "0", NumberKind, "1", NumberKind, "2"], StringKind, "hi"]`)
	r := newJSONArrayReader(a, vs)
	v := r.readValue().(Struct)

	assert.True(v.Type().Equals(typ))
	assert.True(v.Get("b").Equals(Bool(true)))
	l := NewList(Number(0), Number(1), Number(2))
	assert.True(v.Get("l").Equals(l))
	assert.True(v.Get("s").Equals(NewString("hi")))

	assert.True(v.Equals(DecodeChunk(EncodeValue(v, vs), vs)))
}

func TestReadStructWithValue(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	// struct A5 {
	//   b: Bool
	//   v: Value
	//   s: String
	// }

	typ := MakeStructType("A5", TypeMap{
		"b": BoolType,
		"v": ValueType,
		"s": StringType,
	})

	a := parseJSON(`[StructKind, "A5", ["b", BoolKind, "s", StringKind, "v", ValueKind], BoolKind, true, StringKind, "hi", NumberKind, "42"]`)
	r := newJSONArrayReader(a, vs)
	v := r.readValue().(Struct)

	assert.True(v.Type().Equals(typ))
	assert.True(v.Get("b").Equals(Bool(true)))
	assert.True(v.Get("v").Equals(Number(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))

	assert.True(v.Equals(DecodeChunk(EncodeValue(v, vs), vs)))
}

func TestReadRef(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	r := hash.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	a := parseJSON(`[RefKind, NumberKind, "%s", "42"]`, r.String())
	reader := newJSONArrayReader(a, vs)
	v := reader.readValue()
	tr := MakeRefType(NumberType)
	assert.True(constructRef(tr, r, 42).Equals(v))
}

func TestReadStructWithBlob(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	// struct A5 {
	//   b: Blob
	// }

	typ := MakeStructType("A5", TypeMap{
		"b": BlobType,
	})

	a := parseJSON(`[StructKind, "A5", ["b", BlobKind], BlobKind, false, "AAE="]`)
	r := newJSONArrayReader(a, vs)
	v := r.readValue().(Struct)
	assert.True(v.Type().Equals(typ))
	blob := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	assert.True(v.Get("b").Equals(blob))

	assert.True(v.Equals(DecodeChunk(EncodeValue(v, vs), vs)))
}

func TestReadRecursiveStruct(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	// struct A {
	//   b: struct B {
	//     a: List<A>
	//     b: List<B>
	//   }
	// }

	at := MakeStructType("A", TypeMap{
		"b": nil,
	})
	bt := MakeStructType("B", TypeMap{
		"a": MakeListType(at),
		"b": nil,
	})
	at.Desc.(StructDesc).Fields["b"] = bt
	bt.Desc.(StructDesc).Fields["b"] = MakeListType(bt)

	a := parseJSON(`[
		StructKind, "A", [
			"b", StructKind, "B", [
				"a", ListKind, ParentKind, 1,
				"b", ListKind, ParentKind, 0
			]
		],
		StructKind, "B", [
			"a", ListKind, StructKind, "A", [
				"b", ParentKind, 1
			],
			"b", ListKind, ParentKind, 0
		],
		ListKind, StructKind, "A", [
			"b", StructKind, "B", [
				"a", ListKind, ParentKind, 1,
				"b", ListKind, ParentKind, 0
			]
		], false, [],
		ListKind, StructKind, "B", [
			"a", ListKind, StructKind, "A", [
				"b", ParentKind, 1
			],
			"b", ListKind, ParentKind, 0
		], false, []]`)

	r := newJSONArrayReader(a, vs)
	v := r.readValue().(Struct)

	assert.True(v.Type().Equals(at))
	assert.True(v.Get("b").Type().Equals(bt))

	assert.True(v.Equals(DecodeChunk(EncodeValue(v, vs), vs)))
}

func TestReadTypeValue(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	test := func(expected *Type, json string) {
		a := parseJSON(json)
		r := newJSONArrayReader(a, vs)
		tr := r.readValue()
		assert.True(expected.Equals(tr))
	}

	test(NumberType, `[TypeKind, NumberKind]`)
	test(MakeListType(BoolType), `[TypeKind, ListKind, BoolKind]`)
	test(MakeMapType(BoolType, StringType), `[TypeKind, MapKind, BoolKind, StringKind]`)
	test(MakeUnionType(), `[TypeKind, UnionKind, 0]`)
	test(MakeUnionType(NumberType, StringType), `[TypeKind, UnionKind, 2, NumberKind, StringKind]`)
	test(MakeListType(MakeUnionType()), `[TypeKind, ListKind, UnionKind, 0]`)
}

func TestReadUnionList(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	a := parseJSON(`[ListKind, UnionKind, 2, StringKind, NumberKind,
		false, [StringKind, "hi", NumberKind, "42"]]`)

	r := newJSONArrayReader(a, vs)
	v := r.readValue().(List)
	v2 := NewList(NewString("hi"), Number(42))
	assert.True(v.Equals(v2))

	assert.True(v2.Equals(DecodeChunk(EncodeValue(v, vs), vs)))
}

func TestReadEmptyUnionList(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()

	a := parseJSON(`[ListKind, UnionKind, 0, false, []]`)

	r := newJSONArrayReader(a, vs)
	v := r.readValue().(List)
	v2 := NewList()
	assert.True(v.Equals(v2))

	assert.True(v2.Equals(DecodeChunk(EncodeValue(v, vs), vs)))
}
