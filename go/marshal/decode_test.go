// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package marshal

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestDecode(tt *testing.T) {
	assert := assert.New(tt)

	t := func(v types.Value, ptr interface{}, expected interface{}) {
		p := reflect.ValueOf(ptr)
		assert.Equal(reflect.Ptr, p.Type().Kind())
		err := Unmarshal(v, p.Interface())
		assert.NoError(err)
		assert.Equal(expected, p.Elem().Interface())

		// Also test that types.Value is passed through
		var v2 types.Value
		err = Unmarshal(v, &v2)
		assert.NoError(err)
		assert.True(v.Equals(v2))
	}

	for _, n := range []float32{0, 42, 3.14159265359, math.MaxFloat32} {
		var f32 float32
		t(types.Number(n), &f32, float32(n))
	}

	for _, n := range []float64{0, 42, 3.14159265359, math.MaxFloat64} {
		var f64 float64
		t(types.Number(n), &f64, float64(n))
	}

	for _, n := range []int8{0, 42, math.MaxInt8} {
		var i8 int8
		t(types.Number(n), &i8, int8(n))
	}

	for _, n := range []int16{0, 42, math.MaxInt16} {
		var i16 int16
		t(types.Number(n), &i16, int16(n))
	}

	for _, n := range []int32{0, 42, math.MaxInt32} {
		var i32 int32
		t(types.Number(n), &i32, int32(n))
	}

	// int is at least int32
	for _, n := range []int{0, 42, math.MaxInt32} {
		var i int
		t(types.Number(n), &i, int(n))
	}

	// There is precision loss for values above Math.pow(2, 53) - 1
	for _, n := range []int64{0, 42, int64(math.Pow(2, 53) - 1)} {
		var i64 int64
		t(types.Number(n), &i64, int64(n))
	}

	for _, n := range []uint8{0, 42, math.MaxUint8} {
		var ui8 uint8
		t(types.Number(n), &ui8, uint8(n))
	}

	for _, n := range []uint16{0, 42, math.MaxUint16} {
		var ui16 uint16
		t(types.Number(n), &ui16, uint16(n))
	}

	for _, n := range []uint32{0, 42, math.MaxInt32} {
		var ui32 uint32
		t(types.Number(n), &ui32, uint32(n))
	}

	// uint is at least uint32
	for _, n := range []uint{0, 42, math.MaxInt32} {
		var ui uint
		t(types.Number(n), &ui, uint(n))
	}

	// There is precision loss for values above Math.pow(2, 53) - 1
	for _, n := range []uint64{0, 42, uint64(math.Pow(2, 53) - 1)} {
		var ui64 uint64
		t(types.Number(n), &ui64, uint64(n))
	}

	var b bool
	t(types.Bool(true), &b, true)
	t(types.Bool(false), &b, false)

	for _, s := range []string{"", "s", "hello", "💩"} {
		var s2 string
		t(types.String(s), &s2, s)
	}

	var list types.List
	list2 := types.NewList(types.Number(42))
	t(list2, &list, list2)

	var m types.Map
	map2 := types.NewMap(types.Number(42), types.String("Hi"))
	t(map2, &m, map2)

	var set types.Set
	set2 := types.NewSet(types.String("Bye"))
	t(set2, &set, set2)

	var blob types.Blob
	blob2 := types.NewBlob(bytes.NewBufferString("hello"))
	t(blob2, &blob, blob2)

	type TestStruct struct {
		B bool
		A float64
		C string
	}
	var ts TestStruct
	t(types.NewStruct("TestStruct", types.StructData{
		"b": types.Bool(true),
		"a": types.Number(42),
		"c": types.String("hi"),
	}), &ts, TestStruct{true, 42, "hi"})
	// again to test the caching
	t(types.NewStruct("TestStruct", types.StructData{
		"b": types.Bool(false),
		"a": types.Number(555),
		"c": types.String("hello"),
	}), &ts, TestStruct{false, 555, "hello"})

	var as struct {
		X int32
		Y bool
	}
	t(types.NewStruct("", types.StructData{
		"y": types.Bool(true),
		"x": types.Number(42),
	}), &as, struct {
		X int32
		Y bool
	}{
		42,
		true,
	})

	type T2 struct {
		Abc TestStruct
		Def types.List
	}
	var t2 T2
	t(types.NewStruct("T2", types.StructData{
		"abc": types.NewStruct("TestStruct", types.StructData{
			"a": types.Number(1),
			"b": types.Bool(false),
			"c": types.String("bye"),
		}),
		"def": types.NewList(types.Number(42)),
	}), &t2, T2{
		TestStruct{false, 1, "bye"},
		types.NewList(types.Number(42)),
	})

	// extra fields
	type T3 struct {
		B string
	}
	var t3 T3
	t(types.NewStruct("T3", types.StructData{
		"b": types.String("abc"),
		"a": types.Number(42),
	}), &t3, T3{"abc"})

	// Case of struct name is not relevant when unmarshalling.
	type aBc struct {
		E bool
	}
	var t4 aBc
	t(types.NewStruct("abc", types.StructData{
		"e": types.Bool(true),
	}), &t4, aBc{true})
	t(types.NewStruct("Abc", types.StructData{
		"e": types.Bool(false),
	}), &t4, aBc{false})

	// Name of struct is irrelevant to unmarshalling structs.
	type SomeOtherName struct {
		A int
	}
	var t5 SomeOtherName
	t(types.NewStruct("aeiou", types.StructData{
		"a": types.Number(42),
	}), &t5, SomeOtherName{42})

	var t6 SomeOtherName
	t(types.NewStruct("SomeOtherName", types.StructData{
		"a": types.Number(42),
	}), &t6, SomeOtherName{42})

	var t7 struct {
		A int
	}
	t(types.NewStruct("SomeOtherName", types.StructData{
		"a": types.Number(42),
	}), &t7, struct{ A int }{42})
}

func TestDecodeNilPointer(t *testing.T) {
	var x *bool
	assertDecodeErrorMessage(t, types.Bool(true), x, "Cannot unmarshal into Go nil pointer of type *bool")
}

func TestDecodeNonPointer(t *testing.T) {
	b := true
	assertDecodeErrorMessage(t, types.Bool(true), b, "Cannot unmarshal into Go non pointer of type bool")
}

func TestDecodeNil(t *testing.T) {
	err := Unmarshal(types.Bool(true), nil)
	assert.Error(t, err)
	assert.Equal(t, "Cannot unmarshal into Go nil value", err.Error())
}

func TestDecodeTypeMismatch(t *testing.T) {
	var b bool
	assertDecodeErrorMessage(t, types.Number(42), &b, "Cannot unmarshal Number into Go value of type bool")

	var blob types.Blob
	assertDecodeErrorMessage(t, types.NewList(), &blob, "Cannot unmarshal List<> into Go value of type types.Blob")

	type S struct {
		X int
	}
	var s S
	assertDecodeErrorMessage(t, types.String("hi!"), &s, "Cannot unmarshal String into Go value of type marshal.S, expected struct")
	assertDecodeErrorMessage(t, types.NewStruct("S", types.StructData{
		"x": types.String("hi"),
	}), &s, "Cannot unmarshal String into Go value of type int")
}

func assertDecodeErrorMessage(t *testing.T, v types.Value, ptr interface{}, msg string) {
	p := reflect.ValueOf(ptr)
	err := Unmarshal(v, p.Interface())
	assert.Error(t, err)
	assert.Equal(t, msg, err.Error())
}

func TestDecodeInvalidTypes(tt *testing.T) {
	t := func(p interface{}, ts string) {
		assertDecodeErrorMessage(tt, types.Number(42), p, "Type is not supported, type: "+ts)
	}

	var ptr *bool
	t(&ptr, "*bool")

	var c chan bool
	t(&c, "chan bool")

	type Nested struct {
		X *bool
	}
	var n Nested
	t(&n, "*bool")
}

func TestDecodeOverflows(tt *testing.T) {
	t := func(p interface{}, n float64, ts string) {
		assertDecodeErrorMessage(tt, types.Number(n), p, fmt.Sprintf("Cannot unmarshal Number into Go value of type %s (%g does not fit in %s)", ts, n, ts))
	}

	var ui8 uint8
	t(&ui8, 256, "uint8")
	t(&ui8, -1, "uint8")

	var ui16 uint16
	t(&ui16, math.Pow(2, 16), "uint16")
	t(&ui16, -1, "uint16")

	var ui32 uint32
	t(&ui32, math.Pow(2, 32), "uint32")
	t(&ui32, -1, "uint32")

	var i8 int8
	t(&i8, 128, "int8")
	t(&i8, -128-1, "int8")

	var i16 int16
	t(&i16, math.Pow(2, 15), "int16")
	t(&i16, -math.Pow(2, 15)-1, "int16")

	var i32 int32
	t(&i32, math.Pow(2, 31), "int32")
	t(&i32, -math.Pow(2, 31)-1, "int32")
}

func TestDecodeMissingField(t *testing.T) {
	type S struct {
		A int32
		B bool
	}
	var s S
	assertDecodeErrorMessage(t, types.NewStruct("S", types.StructData{
		"a": types.Number(42),
	}), &s, "Cannot unmarshal struct S {\n  a: Number,\n} into Go value of type marshal.S, missing field \"b\"")
}

func TestDecodeEmbeddedStruct(tt *testing.T) {
	type EmbeddedStruct struct{}
	type TestStruct struct {
		EmbeddedStruct
	}
	var ts TestStruct
	assertDecodeErrorMessage(tt, types.String("hi"), &ts, "Embedded structs are not supported, type: marshal.TestStruct")
}

func TestDecodeNonExportedField(tt *testing.T) {
	type TestStruct struct {
		x int
	}
	var ts TestStruct
	assertDecodeErrorMessage(tt, types.String("hi"), &ts, "Non exported fields are not supported, type: marshal.TestStruct")
}

func TestDecodeTaggingSkip(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		A int32 `noms:"-"`
		B bool
	}
	var s S
	err := Unmarshal(types.NewStruct("S", types.StructData{
		"b": types.Bool(true),
	}), &s)
	assert.NoError(err)
	assert.Equal(S{0, true}, s)

	var s2 S
	Unmarshal(types.NewStruct("S", types.StructData{
		"a": types.Number(42),
		"b": types.Bool(true),
	}), &s2)
	assert.Equal(S{0, true}, s2)

	s3 := S{555, true}
	err = Unmarshal(types.NewStruct("S", types.StructData{
		"a": types.Number(42),
		"b": types.Bool(false),
	}), &s3)
	assert.NoError(err)
	assert.Equal(S{555, false}, s3)
}

func TestDecodeNamedFields(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		Aaa int  `noms:"a"`
		Bbb bool `noms:"B"`
		Ccc string
	}
	var s S
	err := Unmarshal(types.NewStruct("S", types.StructData{
		"a":   types.Number(42),
		"B":   types.Bool(true),
		"ccc": types.String("Hi"),
	}), &s)
	assert.NoError(err)
	assert.Equal(S{42, true, "Hi"}, s)
}

func TestDecodeInvalidNamedFields(t *testing.T) {
	type S struct {
		A int `noms:"1a"`
	}
	var s S
	assertDecodeErrorMessage(t, types.NewStruct("S", types.StructData{
		"a": types.Number(42),
	}), &s, "Invalid struct field name: 1a")
}

func TestDecodeInvalidNomsType(t *testing.T) {
	type S struct {
		A types.List
	}
	var s S
	assertDecodeErrorMessage(t, types.NewStruct("S", types.StructData{
		"a": types.NewMap(types.String("A"), types.Number(1)),
	}), &s, "Cannot unmarshal Map<String, Number> into Go value of type types.List")
}

func TestDecodeNomsTypePtr(t *testing.T) {
	assert := assert.New(t)

	testUnmarshal := func(v types.Value, dest interface{}, expected interface{}) {
		err := Unmarshal(v, dest)
		assert.NoError(err)
		assert.Equal(expected, dest)
	}

	type S struct{ Type *types.Type }
	var s S

	primitive := types.StringType
	testUnmarshal(types.NewStruct("S", types.StructData{"type": primitive}), &s, &S{primitive})

	complex := types.MakeStructType("Complex",
		[]string{"stuff"},
		[]*types.Type{types.StringType},
	)
	testUnmarshal(types.NewStruct("S", types.StructData{"type": complex}), &s, &S{complex})

	var empty *types.Type
	testUnmarshal(types.NewStruct("S", types.StructData{"type": empty}), &s, &S{empty})
}

func ExampleUnmarshal() {
	type Person struct {
		Given string
		Male  bool
	}
	var rickon Person
	err := Unmarshal(types.NewStruct("Person", types.StructData{
		"given": types.String("Rickon"),
		"male":  types.Bool(true),
	}), &rickon)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Given: %s, Male: %t\n", rickon.Given, rickon.Male)
	// Output: Given: Rickon, Male: true
}

func TestDecodeSlice(t *testing.T) {
	assert := assert.New(t)
	var s []string

	err := Unmarshal(types.NewList(types.String("a"), types.String("b"), types.String("c")), &s)
	assert.NoError(err)
	assert.Equal([]string{"a", "b", "c"}, s)

	err = Unmarshal(types.NewSet(types.String("a"), types.String("b"), types.String("c")), &s)
	assert.NoError(err)
	assert.Equal([]string{"a", "b", "c"}, s)
}

func TestDecodeSliceReuse(t *testing.T) {
	assert := assert.New(t)
	s := []string{"A", "B", "C", "D"}
	s2 := s[1:3]
	err := Unmarshal(types.NewList(types.String("a"), types.String("b")), &s)
	assert.NoError(err)
	assert.Equal([]string{"a", "b"}, s)
	assert.Equal([]string{"b", "C"}, s2)

	err = Unmarshal(types.NewSet(types.String("a"), types.String("b")), &s)
	assert.NoError(err)
	assert.Equal([]string{"a", "b"}, s)
	assert.Equal([]string{"b", "C"}, s2)
}

func TestDecodeArray(t *testing.T) {
	assert := assert.New(t)
	s := [3]string{"", "", ""}

	err := Unmarshal(types.NewList(types.String("a"), types.String("b"), types.String("c")), &s)
	assert.NoError(err)
	assert.Equal([3]string{"a", "b", "c"}, s)

	err = Unmarshal(types.NewSet(types.String("a"), types.String("b"), types.String("c")), &s)
	assert.NoError(err)
	assert.Equal([3]string{"a", "b", "c"}, s)
}

func TestDecodeStructWithSlice(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		List []int
	}
	var s S
	err := Unmarshal(types.NewStruct("S", types.StructData{
		"list": types.NewList(types.Number(1), types.Number(2), types.Number(3)),
	}), &s)
	assert.NoError(err)
	assert.Equal(S{[]int{1, 2, 3}}, s)

	err = Unmarshal(types.NewStruct("S", types.StructData{
		"list": types.NewSet(types.Number(1), types.Number(2), types.Number(3)),
	}), &s)
	assert.NoError(err)
	assert.Equal(S{[]int{1, 2, 3}}, s)
}

func TestDecodeStructWithArrayOfNomsValue(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		List [1]types.Set
	}
	var s S
	err := Unmarshal(types.NewStruct("S", types.StructData{
		"list": types.NewList(types.NewSet(types.Bool(true))),
	}), &s)
	assert.NoError(err)
	assert.Equal(S{[1]types.Set{types.NewSet(types.Bool(true))}}, s)
}

func TestDecodeWrongArrayLength(t *testing.T) {
	var l [2]string
	assertDecodeErrorMessage(t, types.NewList(types.String("hi")), &l, "Cannot unmarshal List<String> into Go value of type [2]string, length does not match")
}

func TestDecodeWrongArrayType(t *testing.T) {
	var l [1]string
	assertDecodeErrorMessage(t, types.NewList(types.Number(1)), &l, "Cannot unmarshal Number into Go value of type string")
}

func TestDecodeWrongSliceType(t *testing.T) {
	var l []string
	assertDecodeErrorMessage(t, types.NewList(types.Number(1)), &l, "Cannot unmarshal Number into Go value of type string")
}

func TestDecodeSliceWrongNomsType(t *testing.T) {
	var l []string
	assertDecodeErrorMessage(t, types.NewMap(types.String("a"), types.Number(1)), &l, "Cannot unmarshal Map<String, Number> into Go value of type []string")
}

func TestDecodeArrayWrongNomsType(t *testing.T) {
	var l [1]string
	assertDecodeErrorMessage(t, types.NewMap(types.String("a"), types.Number(1)), &l, "Cannot unmarshal Map<String, Number> into Go value of type [1]string")
}

func TestDecodeRecursive(t *testing.T) {
	assert := assert.New(t)

	type Node struct {
		Value    int
		Children []Node
	}

	typ := types.MakeStructType("Node", []string{"children", "value"}, []*types.Type{
		types.MakeListType(types.MakeCycleType(0)),
		types.NumberType,
	})
	v := types.NewStructWithType(typ, types.ValueSlice{
		types.NewList(
			types.NewStructWithType(typ, types.ValueSlice{
				types.NewList(),
				types.Number(2),
			}),
			types.NewStructWithType(typ, types.ValueSlice{
				types.NewList(),
				types.Number(3),
			}),
		),
		types.Number(1),
	})

	var n Node
	err := Unmarshal(v, &n)
	assert.NoError(err)

	assert.Equal(Node{
		1, []Node{
			{2, []Node{}},
			{3, []Node{}},
		},
	}, n)
}

func TestDecodeMap(t *testing.T) {
	assert := assert.New(t)
	var m map[string]int

	testMap := types.NewMap(
		types.String("a"), types.Number(1),
		types.String("b"), types.Number(2),
		types.String("c"), types.Number(3))
	expectedMap := map[string]int{"a": 1, "b": 2, "c": 3}
	err := Unmarshal(testMap, &m)
	assert.NoError(err)
	assert.Equal(expectedMap, m)

	m = map[string]int{"b": 2, "c": 333}
	err = Unmarshal(types.NewMap(
		types.String("a"), types.Number(1),
		types.String("c"), types.Number(3)), &m)
	assert.NoError(err)
	assert.Equal(expectedMap, m)

	type S struct {
		N string
	}

	var m2 map[S]bool
	err = Unmarshal(types.NewMap(
		types.NewStruct("S", types.StructData{"n": types.String("Yes")}), types.Bool(true),
		types.NewStruct("S", types.StructData{"n": types.String("No")}), types.Bool(false)), &m2)
	assert.NoError(err)
	assert.Equal(map[S]bool{S{"Yes"}: true, S{"No"}: false}, m2)
}

func TestDecodeMapWrongNomsType(t *testing.T) {
	var m map[string]int
	assertDecodeErrorMessage(t, types.NewList(types.String("a"), types.Number(1)), &m, "Cannot unmarshal List<Number | String> into Go value of type map[string]int")
}

func TestDecodeOntoInterface(t *testing.T) {
	assert := assert.New(t)

	var i interface{}
	err := Unmarshal(types.Number(1), &i)
	assert.NoError(err)
	assert.Equal(float64(1), i)

	err = Unmarshal(types.String("abc"), &i)
	assert.NoError(err)
	assert.Equal("abc", i)

	err = Unmarshal(types.Bool(true), &i)
	assert.NoError(err)
	assert.Equal(true, i)

	err = Unmarshal(types.NewList(types.String("abc")), &i)
	assert.NoError(err)
	assert.Equal([]string{"abc"}, i)

	err = Unmarshal(types.NewMap(types.String("abc"), types.Number(1)), &i)
	assert.NoError(err)
	assert.Equal(map[string]float64{"abc": float64(1)}, i)

	err = Unmarshal(types.NewList(types.String("a"), types.Bool(true), types.Number(42)), &i)
	assert.NoError(err)
	assert.Equal([]interface{}{"a", true, float64(42)}, i)

	err = Unmarshal(types.NewMap(types.String("a"), types.Bool(true), types.Number(42), types.NewList()), &i)
	assert.NoError(err)
	assert.Equal(map[interface{}]interface{}{"a": true, float64(42): []interface{}{}}, i)
}

func TestDecodeOntoNonSupportedInterface(t *testing.T) {
	type I interface {
		M() int
	}
	var i I
	assertDecodeErrorMessage(t, types.Number(1), &i, "Type is not supported, type: marshal.I")
}

func TestDecodeOntoInterfaceStruct(t *testing.T) {
	// Not implemented because it requires Go 1.7.
	var i interface{}
	assertDecodeErrorMessage(t, types.NewStruct("", types.StructData{}), &i, "Cannot unmarshal struct {} into Go value of type interface {}")
}

func TestDecodeSet(t *testing.T) {
	assert := assert.New(t)

	type T struct {
		A map[int]struct{} `noms:",set"`
		B map[int]struct{}
		C map[string]struct{} `noms:",set"`
		D map[string]struct{}
		E []int
		F []int
	}

	ns := types.NewStruct("T", types.StructData{
		"a": types.NewSet(types.Number(0), types.Number(1), types.Number(2)),
		"b": types.NewMap(types.Number(3), types.EmptyStruct, types.Number(4), types.EmptyStruct, types.Number(5), types.EmptyStruct),
		"c": types.NewSet(types.String("0"), types.String("1"), types.String("2")),
		"d": types.NewMap(types.String("3"), types.EmptyStruct, types.String("4"), types.EmptyStruct, types.String("5"), types.EmptyStruct),
		"e": types.NewSet(types.Number(6), types.Number(7), types.Number(8)),
		"f": types.NewList(types.Number(9), types.Number(10), types.Number(11)),
	})

	gs := T{}
	assert.NoError(Unmarshal(ns, &gs))
	assert.Equal(T{
		A: map[int]struct{}{0: {}, 1: {}, 2: {}},
		B: map[int]struct{}{3: {}, 4: {}, 5: {}},
		C: map[string]struct{}{"0": {}, "1": {}, "2": {}},
		D: map[string]struct{}{"3": {}, "4": {}, "5": {}},
		E: []int{6, 7, 8},
		F: []int{9, 10, 11},
	}, gs)
}

func TestDecodeNamedSet(t *testing.T) {
	assert := assert.New(t)

	type T struct {
		A map[int]struct{} `noms:"foo,set"`
	}

	ns := types.NewStruct("T", types.StructData{
		"a":   types.NewSet(types.Number(0)),
		"foo": types.NewSet(types.Number(1)),
	})

	gs := T{}
	assert.NoError(Unmarshal(ns, &gs))
	assert.Equal(T{
		map[int]struct{}{1: {}},
	}, gs)
}

func TestDecodeSetWrongMapType(t *testing.T) {
	assert := assert.New(t)

	type T1 struct {
		A map[int]int `noms:",set"`
	}

	err := Unmarshal(types.NewStruct("T1", types.StructData{
		"a": types.NewSet(types.Number(0)),
	}), &T1{})
	assert.Error(err)
	assert.Equal("Cannot unmarshal Set<Number> into Go value of type map[int]int", err.Error())

	type T2 struct {
		A map[int]struct{}
	}

	err = Unmarshal(types.NewStruct("T2", types.StructData{
		"a": types.NewSet(types.Number(0)),
	}), &T2{})
	assert.Error(err)
	assert.Equal(`Cannot unmarshal Set<Number> into Go value of type map[int]struct {}, field missing "set" tag`, err.Error())

	type T3 struct {
		A map[int]struct{} `noms:",set"`
	}

	err = Unmarshal(types.NewStruct("T3", types.StructData{
		"a": types.NewMap(types.Number(0), types.EmptyStruct),
	}), &T3{})
	assert.Error(err)
	assert.Equal(`Cannot unmarshal Map<Number, struct {}> into Go value of type map[int]struct {}, field has "set" tag`, err.Error())
}

func TestDecodeOmitEmpty(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		Foo int `noms:",omitempty"`
		Bar struct {
			Baz    int
			Hotdog int `noms:",omitempty"`
		}
	}
	expected := S{
		Bar: struct {
			Baz    int
			Hotdog int `noms:",omitempty"`
		}{
			Baz: 42,
		},
	}
	var actual S
	err := Unmarshal(types.NewStruct("S", types.StructData{
		"bar": types.NewStruct("", types.StructData{
			"baz": types.Number(42),
		}),
	}), &actual)
	assert.NoError(err)
	assert.Equal(expected, actual)
}

func TestDecodeOriginal(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		Foo int          `noms:",omitempty"`
		Bar types.Struct `noms:",original"`
		Baz types.Struct `noms:",original"`
	}
	input := types.NewStruct("S", types.StructData{
		"foo": types.Number(42),
	})
	expected := S{
		Foo: 42,
		Bar: input,
		Baz: input,
	}
	var actual S
	err := Unmarshal(input, &actual)
	assert.NoError(err)
	assert.True(expected.Bar.Equals(actual.Bar))
}

func TestDecodeOriginalReceiveTypeError(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		Foo types.Value `noms:",original"`
	}
	input := types.NewStruct("S", types.StructData{})
	var actual S
	err := Unmarshal(input, &actual)
	assert.Error(err)
	assert.Equal(`Cannot unmarshal struct S {} into Go value of type marshal.S, field with tag "original" must have type Struct`, err.Error())
}
