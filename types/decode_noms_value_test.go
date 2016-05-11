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
	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := []interface{}{Number(1), "hi", true}
	r := newJSONArrayReader(a, cs)

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
	cs := NewTestValueStore()

	test := func(expected *Type, s string, vs ...interface{}) {
		a := parseJSON(s, vs...)
		r := newJSONArrayReader(a, cs)
		tr := r.readType(nil)
		assert.True(t, expected.Equals(tr))
	}

	test(BoolType, "[%d, true]", BoolKind)
	test(TypeType, "[%d, %d]", TypeKind, BoolKind)
	test(MakeListType(BoolType), "[%d, %d, true, false]", ListKind, BoolKind)
}

func TestReadPrimitives(t *testing.T) {
	assert := assert.New(t)

	cs := NewTestValueStore()

	test := func(expected Value, s string) {
		a := parseJSON(s)
		r := newJSONArrayReader(a, cs)
		v := r.readValue()
		assert.True(expected.Equals(v))
	}

	test(Bool(true), "[BoolKind, true]")
	test(Bool(false), "[BoolKind, false]")
	test(Number(0), `[NumberKind, "0"]`)
	test(NewString("hi"), `[StringKind, "hi"]`)

	blob := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	test(blob, `[BlobKind, false, "AAE="]`)
}

func TestReadListOfNumber(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[ListKind, NumberKind, false, [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`)
	r := newJSONArrayReader(a, cs)

	tr := MakeListType(NumberType)

	l := r.readValue()
	l2 := NewTypedList(tr, Number(0), Number(1), Number(2), Number(3))
	assert.True(l2.Equals(l))
}

func TestReadListOfValue(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[ListKind, ValueKind, false, [NumberKind, "1", StringKind, "hi", BoolKind, true]]`)
	r := newJSONArrayReader(a, cs)
	l := r.readValue()
	assert.True(NewList(Number(1), NewString("hi"), Bool(true)).Equals(l))
}

func TestReadCompoundList(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	tr := MakeListType(NumberType)
	list1 := newList(newListLeafSequence(tr, cs, Number(0)))
	list2 := newList(newListLeafSequence(tr, cs, Number(1), Number(2), Number(3)))
	l2 := newList(newIndexedMetaSequence([]metaTuple{
		newMetaTuple(Number(1), list1, NewTypedRefFromValue(list1), 1),
		newMetaTuple(Number(4), list2, NewTypedRefFromValue(list2), 4),
	}, tr, cs))

	a := parseJSON(`[
		ListKind, NumberKind, true, [
			RefKind, ListKind, NumberKind, "%s", "1", NumberKind, "1", "1",
			RefKind, ListKind, NumberKind, "%s", "1", NumberKind, "4", "4"
		]
	]`, list1.Ref(), list2.Ref())
	r := newJSONArrayReader(a, cs)
	l := r.readValue()

	assert.True(l2.Equals(l))
}

func TestReadCompoundSet(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	tr := MakeSetType(NumberType)
	set1 := newSet(newSetLeafSequence(tr, cs, Number(0), Number(1)))
	set2 := newSet(newSetLeafSequence(tr, cs, Number(2), Number(3), Number(4)))
	l2 := newSet(newOrderedMetaSequence([]metaTuple{
		newMetaTuple(Number(1), set1, NewTypedRefFromValue(set1), 2),
		newMetaTuple(Number(4), set2, NewTypedRefFromValue(set2), 3),
	}, tr, cs))

	a := parseJSON(`[
		SetKind, NumberKind, true, [
			RefKind, SetKind, NumberKind, "%s", "1", NumberKind, "1", "2",
			RefKind, SetKind, NumberKind, "%s", "1", NumberKind, "4", "3"
		]
	]`, set1.Ref(), set2.Ref())
	r := newJSONArrayReader(a, cs)
	l := r.readValue()

	assert.True(l2.Equals(l))
}

func TestReadMapOfNumberToNumber(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[MapKind, NumberKind, NumberKind, false, [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`)
	r := newJSONArrayReader(a, cs)

	tr := MakeMapType(NumberType, NumberType)

	m := r.readValue()
	m2 := NewTypedMap(tr, Number(0), Number(1), Number(2), Number(3))
	assert.True(m2.Equals(m))
}

func TestReadSetOfNumber(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[SetKind, NumberKind, false, [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`)
	r := newJSONArrayReader(a, cs)

	tr := MakeSetType(NumberType)

	s := r.readValue()
	s2 := NewTypedSet(tr, Number(0), Number(1), Number(2), Number(3))
	assert.True(s2.Equals(s))
}

func TestReadCompoundBlob(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	// Arbitrary valid refs.
	r1 := Number(1).Ref()
	r2 := Number(2).Ref()
	r3 := Number(3).Ref()
	a := parseJSON(`[
		BlobKind, true, [
			RefKind, BlobKind, "%s", "1", NumberKind, "20", "20",
			RefKind, BlobKind, "%s", "1", NumberKind, "40", "40",
			RefKind, BlobKind, "%s", "1", NumberKind, "60", "60"
		]
	]`, r1, r2, r3)
	r := newJSONArrayReader(a, cs)

	m := r.readValue()
	_, ok := m.(Blob)
	assert.True(ok)
	m2 := newBlob(newIndexedMetaSequence([]metaTuple{
		newMetaTuple(Number(20), nil, NewTypedRef(RefOfBlobType, r1, 1), 20),
		newMetaTuple(Number(40), nil, NewTypedRef(RefOfBlobType, r2, 1), 40),
		newMetaTuple(Number(60), nil, NewTypedRef(RefOfBlobType, r3, 1), 60),
	}, BlobType, cs))

	assert.True(m.Type().Equals(m2.Type()))
	assert.Equal(m.Ref().String(), m2.Ref().String())
}

func TestReadStruct(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	typ := MakeStructType("A1", TypeMap{
		"x": NumberType,
		"s": StringType,
		"b": BoolType,
	})

	a := parseJSON(`[StructKind, "A1", ["b", BoolKind, "s", StringKind, "x", NumberKind], BoolKind, true, StringKind, "hi", NumberKind, "42"]`)
	r := newJSONArrayReader(a, cs)
	v := r.readValue().(Struct)

	assert.True(v.Type().Equals(typ))
	assert.True(v.Get("x").Equals(Number(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))
	assert.True(v.Get("b").Equals(Bool(true)))
}

func TestReadStructWithList(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

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
	r := newJSONArrayReader(a, cs)
	l32Tr := MakeListType(NumberType)
	v := r.readValue().(Struct)

	assert.True(v.Type().Equals(typ))
	assert.True(v.Get("b").Equals(Bool(true)))
	l := NewTypedList(l32Tr, Number(0), Number(1), Number(2))
	assert.True(v.Get("l").Equals(l))
	assert.True(v.Get("s").Equals(NewString("hi")))
}

func TestReadStructWithValue(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

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
	r := newJSONArrayReader(a, cs)
	v := r.readValue().(Struct)

	assert.True(v.Type().Equals(typ))
	assert.True(v.Get("b").Equals(Bool(true)))
	assert.True(v.Get("v").Equals(Number(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))
}

func TestReadRef(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	r := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	a := parseJSON(`[RefKind, NumberKind, "%s", "42"]`, r.String())
	reader := newJSONArrayReader(a, cs)
	v := reader.readValue()
	tr := MakeRefType(NumberType)
	assert.True(NewTypedRef(tr, r, 42).Equals(v))
}

func TestReadStructWithBlob(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	// struct A5 {
	//   b: Blob
	// }

	typ := MakeStructType("A5", TypeMap{
		"b": BlobType,
	})

	a := parseJSON(`[StructKind, "A5", ["b", BlobKind], BlobKind, false, "AAE="]`)
	r := newJSONArrayReader(a, cs)
	v := r.readValue().(Struct)
	assert.True(v.Type().Equals(typ))
	blob := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	assert.True(v.Get("b").Equals(blob))
}

func TestReadRecursiveStruct(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

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

	// {b: {a: [], b: []}}
	v2 := NewStruct(at, structData{
		"b": NewStruct(bt, structData{
			"a": NewTypedList(MakeListType(at)),
			"b": NewTypedList(MakeListType(bt)),
		}),
	})

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

	r := newJSONArrayReader(a, cs)

	v := r.readValue().(Struct)
	assert.True(v.Type().Equals(at))
	assert.True(v.Get("b").Type().Equals(bt))
	assert.True(v.Equals(v2))
}

func TestReadTypeValue(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	test := func(expected *Type, json string) {
		a := parseJSON(json)
		r := newJSONArrayReader(a, cs)
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
	cs := NewTestValueStore()

	a := parseJSON(`[ListKind, UnionKind, 2, StringKind, NumberKind,
		false, [StringKind, "hi", NumberKind, "42"]]`)

	r := newJSONArrayReader(a, cs)
	v := r.readValue().(List)
	v2 := NewTypedList(MakeListType(MakeUnionType(StringType, NumberType)), NewString("hi"), Number(42))
	assert.True(v.Equals(v2))
}

func TestReadEmptyUnionList(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[ListKind, UnionKind, 0, false, []]`)

	r := newJSONArrayReader(a, cs)
	v := r.readValue().(List)
	v2 := NewTypedList(MakeListType(MakeUnionType()))
	assert.True(v.Equals(v2))
}
