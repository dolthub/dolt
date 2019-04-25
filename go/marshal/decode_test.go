// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package marshal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
)

func TestDecode(tt *testing.T) {
	assert := assert.New(tt)
	vs := newTestValueStore()
	defer vs.Close()

	t := func(v types.Value, ptr interface{}, expected interface{}) {
		p := reflect.ValueOf(ptr)
		assert.Equal(reflect.Ptr, p.Type().Kind())
		err := Unmarshal(v, p.Interface())
		assert.NoError(err)
		if expectedValue, ok := expected.(types.Value); ok {
			assert.True(expectedValue.Equals(p.Elem().Interface().(types.Value)))
		} else {
			assert.Equal(expected, p.Elem().Interface())
		}

		// Also test that types.Value is passed through
		var v2 types.Value
		err = Unmarshal(v, &v2)
		assert.NoError(err)
		assert.True(v.Equals(v2))
	}

	for _, n := range []float32{0, 42, 3.14159265359, math.MaxFloat32} {
		var f32 float32
		t(types.Float(n), &f32, float32(n))
	}

	for _, n := range []float64{0, 42, 3.14159265359, math.MaxFloat64} {
		var f64 float64
		t(types.Float(n), &f64, float64(n))
	}

	for _, n := range []int8{0, 42, math.MaxInt8} {
		var i8 int8
		t(types.Float(n), &i8, int8(n))
	}

	for _, n := range []int16{0, 42, math.MaxInt16} {
		var i16 int16
		t(types.Float(n), &i16, int16(n))
	}

	for _, n := range []int32{0, 42, math.MaxInt32} {
		var i32 int32
		t(types.Float(n), &i32, int32(n))
	}

	// int is at least int32
	for _, n := range []int{0, 42, math.MaxInt32} {
		var i int
		t(types.Float(n), &i, int(n))
	}

	// There is precision loss for values above Math.pow(2, 53) - 1
	for _, n := range []int64{0, 42, int64(math.Pow(2, 53) - 1)} {
		var i64 int64
		t(types.Float(n), &i64, int64(n))
	}

	for _, n := range []uint8{0, 42, math.MaxUint8} {
		var ui8 uint8
		t(types.Float(n), &ui8, uint8(n))
	}

	for _, n := range []uint16{0, 42, math.MaxUint16} {
		var ui16 uint16
		t(types.Float(n), &ui16, uint16(n))
	}

	for _, n := range []uint32{0, 42, math.MaxInt32} {
		var ui32 uint32
		t(types.Float(n), &ui32, uint32(n))
	}

	// uint is at least uint32
	for _, n := range []uint{0, 42, math.MaxInt32} {
		var ui uint
		t(types.Float(n), &ui, uint(n))
	}

	// There is precision loss for values above Math.pow(2, 53) - 1
	for _, n := range []uint64{0, 42, uint64(math.Pow(2, 53) - 1)} {
		var ui64 uint64
		t(types.Float(n), &ui64, uint64(n))
	}

	var b bool
	t(types.Bool(true), &b, true)
	t(types.Bool(false), &b, false)

	for _, s := range []string{"", "s", "hello", "💩"} {
		var s2 string
		t(types.String(s), &s2, s)
	}

	var list types.List
	list2 := types.NewList(vs, types.Float(42))
	t(list2, &list, list2)

	var m types.Map
	map2 := types.NewMap(vs, types.Float(42), types.String("Hi"))
	t(map2, &m, map2)

	var set types.Set
	set2 := types.NewSet(vs, types.String("Bye"))
	t(set2, &set, set2)

	var blob types.Blob
	blob2 := types.NewBlob(context.Background(), vs, bytes.NewBufferString("hello"))
	t(blob2, &blob, blob2)

	type TestStruct struct {
		B bool
		A float64
		C string
	}
	var ts TestStruct
	t(types.NewStruct("TestStruct", types.StructData{
		"b": types.Bool(true),
		"a": types.Float(42),
		"c": types.String("hi"),
	}), &ts, TestStruct{true, 42, "hi"})
	// again to test the caching
	t(types.NewStruct("TestStruct", types.StructData{
		"b": types.Bool(false),
		"a": types.Float(555),
		"c": types.String("hello"),
	}), &ts, TestStruct{false, 555, "hello"})

	var as struct {
		X int32
		Y bool
	}
	t(types.NewStruct("", types.StructData{
		"y": types.Bool(true),
		"x": types.Float(42),
	}), &as, struct {
		X int32
		Y bool
	}{
		42,
		true,
	})

	// extra fields
	type T3 struct {
		B string
	}
	var t3 T3
	t(types.NewStruct("T3", types.StructData{
		"b": types.String("abc"),
		"a": types.Float(42),
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
		"a": types.Float(42),
	}), &t5, SomeOtherName{42})

	var t6 SomeOtherName
	t(types.NewStruct("SomeOtherName", types.StructData{
		"a": types.Float(42),
	}), &t6, SomeOtherName{42})

	var t7 struct {
		A int
	}
	t(types.NewStruct("SomeOtherName", types.StructData{
		"a": types.Float(42),
	}), &t7, struct{ A int }{42})
}

func TestDecodeStructWithNomsValue(t *testing.T) {
	// This is split out of TestDecode because we cannot use testify Equal
	// on a go struct with a field that is a Noms value.
	vs := newTestValueStore()
	defer vs.Close()

	type TestStruct struct {
		B bool
		A float64
		C string
	}
	type T2 struct {
		Abc TestStruct
		Def types.List
	}

	v := types.NewStruct("T2", types.StructData{
		"abc": types.NewStruct("TestStruct", types.StructData{
			"a": types.Float(1),
			"b": types.Bool(false),
			"c": types.String("bye"),
		}),
		"def": types.NewList(vs, types.Float(42)),
	})
	var t2 T2
	MustUnmarshal(v, &t2)
	assert.IsType(t, T2{}, t2)
	assert.Equal(t, TestStruct{false, 1, "bye"}, t2.Abc)
	assert.True(t, t2.Def.Equals(types.NewList(vs, types.Float(42))))
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

func newTestValueStore() *types.ValueStore {
	st := &chunks.TestStorage{}
	return types.NewValueStore(st.NewView())
}

func TestDecodeTypeMismatch(t *testing.T) {
	vs := newTestValueStore()
	defer vs.Close()

	var b bool
	assertDecodeErrorMessage(t, types.Float(42), &b, "Cannot unmarshal Float into Go value of type bool")

	var blob types.Blob
	assertDecodeErrorMessage(t, types.NewList(vs), &blob, "Cannot unmarshal List<> into Go value of type types.Blob")

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
		assertDecodeErrorMessage(tt, types.Float(42), p, "Type is not supported, type: "+ts)
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
		assertDecodeErrorMessage(tt, types.Float(n), p, fmt.Sprintf("Cannot unmarshal Float into Go value of type %s (%g does not fit in %s)", ts, n, ts))
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
		"a": types.Float(42),
	}), &s, "Cannot unmarshal Struct S {\n  a: Float,\n} into Go value of type marshal.S, missing field \"b\"")
}

func TestDecodeEmbeddedStruct(tt *testing.T) {
	assert := assert.New(tt)

	type EmbeddedStruct struct {
		X int
	}
	type TestStruct struct {
		EmbeddedStruct
	}
	var ts TestStruct
	err := Unmarshal(types.NewStruct("S", types.StructData{
		"x": types.Float(1),
	}), &ts)
	assert.NoError(err)
	assert.Equal(TestStruct{EmbeddedStruct{1}}, ts)

	type OuterTest struct {
		Y bool
		TestStruct
	}
	var ts2 OuterTest
	err = Unmarshal(types.NewStruct("S", types.StructData{
		"x": types.Float(2),
		"y": types.Bool(true),
	}), &ts2)
	assert.NoError(err)
	assert.Equal(OuterTest{true, TestStruct{EmbeddedStruct{2}}}, ts2)
}

func TestDecodeEmbeddedStructSkip(tt *testing.T) {
	assert := assert.New(tt)

	type EmbeddedStruct struct {
		X int
	}
	type TestStruct struct {
		EmbeddedStruct `noms:"-"`
		Y              int
	}
	ts := TestStruct{EmbeddedStruct: EmbeddedStruct{42}}
	err := Unmarshal(types.NewStruct("S", types.StructData{
		"y": types.Float(2),
	}), &ts)
	assert.NoError(err)
	assert.Equal(TestStruct{EmbeddedStruct{42}, 2}, ts)
}

func TestDecodeEmbeddedStructNamed(tt *testing.T) {
	assert := assert.New(tt)

	type EmbeddedStruct struct {
		X int
	}
	type TestStruct struct {
		EmbeddedStruct `noms:"em"`
		Y              int
	}
	ts := TestStruct{EmbeddedStruct: EmbeddedStruct{42}}
	err := Unmarshal(types.NewStruct("S", types.StructData{
		"em": types.NewStruct("S", types.StructData{
			"x": types.Float(1),
		}),
		"y": types.Float(2),
	}), &ts)
	assert.NoError(err)
	assert.Equal(TestStruct{EmbeddedStruct{1}, 2}, ts)
}

func TestDecodeEmbeddedStructOriginal(tt *testing.T) {
	assert := assert.New(tt)

	type EmbeddedStruct struct {
		X int
		O types.Struct `noms:",original"`
	}
	type TestStruct struct {
		EmbeddedStruct
	}
	var ts TestStruct
	nomsStruct := types.NewStruct("S", types.StructData{
		"x": types.Float(1),
	})
	err := Unmarshal(nomsStruct, &ts)
	assert.NoError(err)
	expected := TestStruct{
		EmbeddedStruct: EmbeddedStruct{
			X: 1,
			O: nomsStruct,
		},
	}
	assert.Equal(expected, ts)
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
		"a": types.Float(42),
		"b": types.Bool(true),
	}), &s2)
	assert.Equal(S{0, true}, s2)

	s3 := S{555, true}
	err = Unmarshal(types.NewStruct("S", types.StructData{
		"a": types.Float(42),
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
		"a":   types.Float(42),
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
		"a": types.Float(42),
	}), &s, "Invalid struct field name: 1a")
}

func TestDecodeInvalidNomsType(t *testing.T) {
	vs := newTestValueStore()
	defer vs.Close()

	type S struct {
		A types.List
	}
	var s S
	assertDecodeErrorMessage(t, types.NewStruct("S", types.StructData{
		"a": types.NewMap(vs, types.String("A"), types.Float(1)),
	}), &s, "Cannot unmarshal Map<String, Float> into Go value of type types.List")
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
		types.StructField{
			Name: "stuff",
			Type: types.StringType,
		},
	)
	testUnmarshal(types.NewStruct("S", types.StructData{"type": complex}), &s, &S{complex})
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

	vs := newTestValueStore()
	defer vs.Close()

	var s []string

	err := Unmarshal(types.NewList(vs, types.String("a"), types.String("b"), types.String("c")), &s)
	assert.NoError(err)
	assert.Equal([]string{"a", "b", "c"}, s)

	err = Unmarshal(types.NewSet(vs, types.String("a"), types.String("b"), types.String("c")), &s)
	assert.NoError(err)
	assert.Equal([]string{"a", "b", "c"}, s)
}

func TestDecodeSliceEmpty(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	var s []string

	err := Unmarshal(types.NewList(vs), &s)
	assert.NoError(err)
	assert.Equal([]string(nil), s)

	err = Unmarshal(types.NewSet(vs), &s)
	assert.NoError(err)
	assert.Equal([]string(nil), s)

	s2 := []string{}
	err = Unmarshal(types.NewList(vs), &s2)
	assert.NoError(err)
	assert.Equal([]string{}, s2)

	err = Unmarshal(types.NewSet(vs), &s2)
	assert.NoError(err)
	assert.Equal([]string{}, s2)
}

func TestDecodeSliceReuse(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	s := []string{"A", "B", "C", "D"}
	s2 := s[1:3]
	err := Unmarshal(types.NewList(vs, types.String("a"), types.String("b")), &s)
	assert.NoError(err)
	assert.Equal([]string{"a", "b"}, s)
	assert.Equal([]string{"b", "C"}, s2)

	err = Unmarshal(types.NewSet(vs, types.String("a"), types.String("b")), &s)
	assert.NoError(err)
	assert.Equal([]string{"a", "b"}, s)
	assert.Equal([]string{"b", "C"}, s2)
}

func TestDecodeArray(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	s := [3]string{"", "", ""}

	err := Unmarshal(types.NewList(vs, types.String("a"), types.String("b"), types.String("c")), &s)
	assert.NoError(err)
	assert.Equal([3]string{"a", "b", "c"}, s)

	err = Unmarshal(types.NewSet(vs, types.String("a"), types.String("b"), types.String("c")), &s)
	assert.NoError(err)
	assert.Equal([3]string{"a", "b", "c"}, s)
}

func TestDecodeArrayEmpty(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	var s [0]string

	err := Unmarshal(types.NewList(vs), &s)
	assert.NoError(err)
	assert.Equal([0]string{}, s)

	err = Unmarshal(types.NewSet(vs), &s)
	assert.NoError(err)
	assert.Equal([0]string{}, s)
}

func TestDecodeStructWithSlice(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type S struct {
		List []int
	}
	var s S
	err := Unmarshal(types.NewStruct("S", types.StructData{
		"list": types.NewList(vs, types.Float(1), types.Float(2), types.Float(3)),
	}), &s)
	assert.NoError(err)
	assert.Equal(S{[]int{1, 2, 3}}, s)

	err = Unmarshal(types.NewStruct("S", types.StructData{
		"list": types.NewSet(vs, types.Float(1), types.Float(2), types.Float(3)),
	}), &s)
	assert.NoError(err)
	assert.Equal(S{[]int{1, 2, 3}}, s)
}

func TestDecodeStructWithArrayOfNomsValue(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type S struct {
		List [1]types.Set
	}
	var s S
	err := Unmarshal(types.NewStruct("S", types.StructData{
		"list": types.NewList(vs, types.NewSet(vs, types.Bool(true))),
	}), &s)
	assert.NoError(err)
	assert.Equal(S{[1]types.Set{types.NewSet(vs, types.Bool(true))}}, s)
}

func TestDecodeWrongArrayLength(t *testing.T) {
	vs := newTestValueStore()
	defer vs.Close()

	var l [2]string
	assertDecodeErrorMessage(t, types.NewList(vs, types.String("hi")), &l, "Cannot unmarshal List<String> into Go value of type [2]string, length does not match")
}

func TestDecodeWrongArrayType(t *testing.T) {
	vs := newTestValueStore()
	defer vs.Close()

	var l [1]string
	assertDecodeErrorMessage(t, types.NewList(vs, types.Float(1)), &l, "Cannot unmarshal Float into Go value of type string")
}

func TestDecodeWrongSliceType(t *testing.T) {
	vs := newTestValueStore()
	defer vs.Close()

	var l []string
	assertDecodeErrorMessage(t, types.NewList(vs, types.Float(1)), &l, "Cannot unmarshal Float into Go value of type string")
}

func TestDecodeSliceWrongNomsType(t *testing.T) {
	vs := newTestValueStore()
	defer vs.Close()

	var l []string
	assertDecodeErrorMessage(t, types.NewMap(vs, types.String("a"), types.Float(1)), &l, "Cannot unmarshal Map<String, Float> into Go value of type []string")
}

func TestDecodeArrayWrongNomsType(t *testing.T) {
	vs := newTestValueStore()
	defer vs.Close()

	var l [1]string
	assertDecodeErrorMessage(t, types.NewMap(vs, types.String("a"), types.Float(1)), &l, "Cannot unmarshal Map<String, Float> into Go value of type [1]string")
}

func TestDecodeRecursive(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type Node struct {
		Value    int
		Children []Node
	}

	v := types.NewStruct("Node", types.StructData{
		"children": types.NewList(
			vs,
			types.NewStruct("Node", types.StructData{
				"children": types.NewList(vs),
				"value":    types.Float(2),
			}),
			types.NewStruct("Node", types.StructData{
				"children": types.NewList(vs),
				"value":    types.Float(3),
			}),
		),
		"value": types.Float(1),
	})

	var n Node
	err := Unmarshal(v, &n)
	assert.NoError(err)

	assert.Equal(Node{
		1, []Node{
			{2, nil},
			{3, nil},
		},
	}, n)
}

func TestDecodeMap(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	var m map[string]int

	testMap := types.NewMap(
		vs,
		types.String("a"), types.Float(1),
		types.String("b"), types.Float(2),
		types.String("c"), types.Float(3))
	expectedMap := map[string]int{"a": 1, "b": 2, "c": 3}
	err := Unmarshal(testMap, &m)
	assert.NoError(err)
	assert.Equal(expectedMap, m)

	m = map[string]int{"b": 2, "c": 333}
	err = Unmarshal(types.NewMap(
		vs,
		types.String("a"), types.Float(1),
		types.String("c"), types.Float(3)), &m)
	assert.NoError(err)
	assert.Equal(expectedMap, m)

	type S struct {
		N string
	}

	var m2 map[S]bool
	err = Unmarshal(types.NewMap(
		vs,
		types.NewStruct("S", types.StructData{"n": types.String("Yes")}), types.Bool(true),
		types.NewStruct("S", types.StructData{"n": types.String("No")}), types.Bool(false)), &m2)
	assert.NoError(err)
	assert.Equal(map[S]bool{S{"Yes"}: true, S{"No"}: false}, m2)
}

func TestDecodeMapEmpty(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	var m map[string]int
	err := Unmarshal(types.NewMap(vs), &m)
	assert.NoError(err)
	assert.Equal(map[string]int(nil), m)

	m2 := map[string]int{}
	err = Unmarshal(types.NewMap(vs), &m2)
	assert.NoError(err)
	assert.Equal(map[string]int{}, m2)
}

func TestDecodeMapWrongNomsType(t *testing.T) {
	vs := newTestValueStore()
	defer vs.Close()

	var m map[string]int
	assertDecodeErrorMessage(t, types.NewList(vs, types.String("a"), types.Float(1)), &m, "Cannot unmarshal List<Float | String> into Go value of type map[string]int")
}

func TestDecodeOntoInterface(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	var i interface{}
	err := Unmarshal(types.Float(1), &i)
	assert.NoError(err)
	assert.Equal(float64(1), i)

	err = Unmarshal(types.String("abc"), &i)
	assert.NoError(err)
	assert.Equal("abc", i)

	err = Unmarshal(types.Bool(true), &i)
	assert.NoError(err)
	assert.Equal(true, i)

	err = Unmarshal(types.NewList(vs, types.String("abc")), &i)
	assert.NoError(err)
	assert.Equal([]string{"abc"}, i)

	err = Unmarshal(types.NewMap(vs, types.String("abc"), types.Float(1)), &i)
	assert.NoError(err)
	assert.Equal(map[string]float64{"abc": float64(1)}, i)

	err = Unmarshal(types.NewList(vs, types.String("a"), types.Bool(true), types.Float(42)), &i)
	assert.NoError(err)
	assert.Equal([]interface{}{"a", true, float64(42)}, i)

	err = Unmarshal(types.NewMap(vs, types.String("a"), types.Bool(true), types.Float(42), types.NewList(vs)), &i)
	assert.NoError(err)
	assert.Equal(map[interface{}]interface{}{"a": true, float64(42): []interface{}(nil)}, i)
}

func TestDecodeOntoNonSupportedInterface(t *testing.T) {
	type I interface {
		M() int
	}
	var i I
	assertDecodeErrorMessage(t, types.Float(1), &i, "Type is not supported, type: marshal.I")
}

func TestDecodeOntoInterfaceStruct(t *testing.T) {
	// Not implemented because it requires Go 1.7.
	var i interface{}
	assertDecodeErrorMessage(t, types.NewStruct("", types.StructData{}), &i, "Cannot unmarshal Struct {} into Go value of type interface {}")
}

func TestDecodeSet(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type T struct {
		A map[int]struct{} `noms:",set"`
		B map[int]struct{}
		C map[string]struct{} `noms:",set"`
		D map[string]struct{}
		E []int
		F []int `noms:",set"`
		G []int
	}

	ns := types.NewStruct("T", types.StructData{
		"a": types.NewSet(vs, types.Float(0), types.Float(1), types.Float(2)),
		"b": types.NewMap(vs, types.Float(3), types.EmptyStruct, types.Float(4), types.EmptyStruct, types.Float(5), types.EmptyStruct),
		"c": types.NewSet(vs, types.String("0"), types.String("1"), types.String("2")),
		"d": types.NewMap(vs, types.String("3"), types.EmptyStruct, types.String("4"), types.EmptyStruct, types.String("5"), types.EmptyStruct),
		"e": types.NewSet(vs, types.Float(6), types.Float(7), types.Float(8)),
		"f": types.NewSet(vs, types.Float(9), types.Float(10), types.Float(11)),
		"g": types.NewList(vs, types.Float(12), types.Float(13), types.Float(14)),
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
		G: []int{12, 13, 14},
	}, gs)

	ns2 := types.NewStruct("T", types.StructData{
		"a": types.NewSet(vs),
		"b": types.NewMap(vs),
		"c": types.NewSet(vs),
		"d": types.NewMap(vs),
		"e": types.NewSet(vs),
		"f": types.NewSet(vs),
		"g": types.NewList(vs),
	})

	gs2 := T{
		A: map[int]struct{}{},
	}
	assert.NoError(Unmarshal(ns2, &gs2))
	assert.Equal(T{
		A: map[int]struct{}{},
	}, gs2)
}

func TestDecodeOpt(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	tc := []struct {
		in        types.Value
		opt       Opt
		onto      interface{}
		wantValue interface{}
		wantError string
	}{
		{
			types.NewSet(vs, types.String("a"), types.String("b")),
			Opt{},
			&[]string{},
			&[]string{"a", "b"},
			"",
		},
		{
			types.NewSet(vs, types.String("a"), types.String("b")),
			Opt{Set: true},
			&[]string{},
			&[]string{"a", "b"},
			"",
		},
		{
			types.NewSet(vs, types.String("a"), types.String("b")),
			Opt{Set: true},
			&map[string]struct{}{},
			&map[string]struct{}{"a": struct{}{}, "b": struct{}{}},
			"",
		},
		{
			types.NewSet(vs, types.String("a"), types.String("b")),
			Opt{},
			&map[string]struct{}{},
			&map[string]struct{}{},
			"Cannot unmarshal Set<String> into Go value of type map[string]struct {}, field missing \"set\" tag",
		},
	}

	for _, t := range tc {
		err := UnmarshalOpt(t.in, t.opt, t.onto)
		assert.Equal(t.wantValue, t.onto)
		if t.wantError == "" {
			assert.Nil(err)
		} else {
			assert.Equal(t.wantError, err.Error())
		}
	}
}

func TestDecodeNamedSet(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type T struct {
		A map[int]struct{} `noms:"foo,set"`
	}

	ns := types.NewStruct("T", types.StructData{
		"a":   types.NewSet(vs, types.Float(0)),
		"foo": types.NewSet(vs, types.Float(1)),
	})

	gs := T{}
	assert.NoError(Unmarshal(ns, &gs))
	assert.Equal(T{
		map[int]struct{}{1: {}},
	}, gs)
}

func TestDecodeSetWrongMapType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type T1 struct {
		A map[int]int `noms:",set"`
	}

	err := Unmarshal(types.NewStruct("T1", types.StructData{
		"a": types.NewSet(vs, types.Float(0)),
	}), &T1{})
	assert.Error(err)
	assert.Equal("Cannot unmarshal Set<Float> into Go value of type map[int]int", err.Error())

	type T2 struct {
		A map[int]struct{}
	}

	err = Unmarshal(types.NewStruct("T2", types.StructData{
		"a": types.NewSet(vs, types.Float(0)),
	}), &T2{})
	assert.Error(err)
	assert.Equal(`Cannot unmarshal Set<Float> into Go value of type map[int]struct {}, field missing "set" tag`, err.Error())

	type T3 struct {
		A map[int]struct{} `noms:",set"`
	}

	err = Unmarshal(types.NewStruct("T3", types.StructData{
		"a": types.NewMap(vs, types.Float(0), types.EmptyStruct),
	}), &T3{})
	assert.Error(err)
	assert.Equal(`Cannot unmarshal Map<Float, Struct {}> into Go value of type map[int]struct {}, field has "set" tag`, err.Error())
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
			"baz": types.Float(42),
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
		"foo": types.Float(42),
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
	assert.Equal(`Cannot unmarshal Struct S {} into Go value of type marshal.S, field with tag "original" must have type Struct`, err.Error())
}

func TestDecodeCanSkipUnexportedField(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		Abc         int
		notExported bool `noms:"-"`
	}
	var s S
	err := Unmarshal(types.NewStruct("S", types.StructData{
		"abc": types.Float(42),
	}), &s)
	assert.NoError(err)
	assert.Equal(S{42, false}, s)
}

func (u *primitiveType) UnmarshalNoms(v types.Value) error {
	*u = primitiveType(v.(types.Float) - 1)
	return nil
}

func TestUnmarshalerPrimitiveType(t *testing.T) {
	assert := assert.New(t)

	v := types.Float(43)
	u := primitiveType(0)
	assert.NoError(Unmarshal(v, &u))
	assert.Equal(primitiveType(42), u)
}

func (u *primitiveSliceType) UnmarshalNoms(v types.Value) error {
	sv := string(v.(types.String))
	spl := strings.Split(sv, ",")
	*u = make(primitiveSliceType, len(spl))
	for i, s := range spl {
		(*u)[i] = s
	}
	return nil
}

func TestUnmarshalerPrimitiveSliceType(t *testing.T) {
	assert := assert.New(t)

	v := types.String("a,b,c")
	u := primitiveSliceType{}
	assert.NoError(Unmarshal(v, &u))
	assert.Equal(primitiveSliceType{"a", "b", "c"}, u)
}

func (u *primitiveMapType) UnmarshalNoms(v types.Value) error {
	*u = primitiveMapType{}
	v.(types.Set).IterAll(func(v types.Value) {
		sv := v.(types.String)
		spl := strings.Split(string(sv), ",")
		d.PanicIfFalse(len(spl) == 2)
		(*u)[spl[0]] = spl[1]
	})
	return nil
}

func TestUnmarshalerPrimitiveMapType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	v := types.NewSet(vs, types.String("a,foo"), types.String("b,bar"))
	u := primitiveMapType{}
	assert.NoError(Unmarshal(v, &u))
	assert.Equal(primitiveMapType(map[string]string{
		"a": "foo",
		"b": "bar",
	}), u)
}

func (u *primitiveStructType) UnmarshalNoms(v types.Value) error {
	n := int(v.(types.Float))
	u.x = n / 3
	u.y = n % 3
	return nil
}

func TestUnmarshalerPrimitiveStructType(t *testing.T) {
	assert := assert.New(t)

	v := types.Float(10)
	u := primitiveStructType{}
	assert.NoError(Unmarshal(v, &u))
	assert.Equal(primitiveStructType{3, 1}, u)
}

func (u *builtinType) UnmarshalNoms(v types.Value) error {
	sv := v.(types.String)
	*u = builtinType(*regexp.MustCompile(string(sv)))
	return nil
}

func TestUnmarshalerBuiltinType(t *testing.T) {
	assert := assert.New(t)

	s := "[a-z]+$"
	v := types.String(s)
	u := builtinType{}
	assert.NoError(Unmarshal(v, &u))
	r := regexp.Regexp(u)
	assert.Equal(s, r.String())
}

func (u *wrappedMarshalerType) UnmarshalNoms(v types.Value) error {
	n := v.(types.Float)
	*u = wrappedMarshalerType(int(n) - 2)
	return nil
}

func TestUnmarshalerWrappedMarshalerType(t *testing.T) {
	assert := assert.New(t)

	v := types.Float(44)
	u := wrappedMarshalerType(0)
	assert.NoError(Unmarshal(v, &u))
	assert.Equal(wrappedMarshalerType(42), u)
}

func TestUnmarshalerComplexStructType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	s := "foo|bar"
	r := regexp.MustCompile(s)
	v := types.NewStruct("TestComplexStructType", types.StructData{
		"p":       types.Float(43),
		"ps":      types.NewList(vs, types.Float(2), types.Float(3)),
		"pm":      types.NewMap(vs, types.String("x"), types.Float(101), types.String("y"), types.Float(102)),
		"pslice":  types.String("a,b,c"),
		"pmap":    types.NewSet(vs, types.String("c,123"), types.String("d,456")),
		"pstruct": types.Float(5),
		"b":       types.String(s),
	})
	u := TestComplexStructType{}
	assert.NoError(Unmarshal(v, &u))
	assert.Equal(TestComplexStructType{
		P:  42,
		Ps: []primitiveType{1, 2},
		Pm: map[string]primitiveType{
			"x": 100,
			"y": 101,
		},
		Pslice: primitiveSliceType{"a", "b", "c"},
		Pmap: primitiveMapType{
			"c": "123",
			"d": "456",
		},
		Pstruct: primitiveStructType{1, 2},
		B:       builtinType(*r),
	}, u)
}

func (u *returnsMarshalerError) UnmarshalNoms(v types.Value) error {
	// Can't use u.err because an empty returnsMarshalerError is created for each
	// call to UnmarshalNoms.
	return errors.New("foo bar baz")
}

func (u panicsMarshaler) UnmarshalNoms(v types.Value) error {
	panic("panic")
}

func TestUnmarshalerError(t *testing.T) {
	assert := assert.New(t)

	m1 := returnsMarshalerError{}
	err := Unmarshal(types.EmptyStruct, &m1)
	assert.Equal(errors.New("foo bar baz"), err)

	m2 := panicsMarshaler{}
	assert.Panics(func() { Unmarshal(types.EmptyStruct, &m2) })
}

type notPointer struct {
	x int
}

func (u notPointer) UnmarshalNoms(v types.Value) error {
	u.x++
	return nil
}

func TestUnmarshalNomsNotPointerDoesNotShareState(t *testing.T) {
	assert := assert.New(t)

	u := notPointer{0}
	assert.NoError(Unmarshal(types.EmptyStruct, &u))
	assert.NoError(Unmarshal(types.EmptyStruct, &u))
	assert.NoError(Unmarshal(types.EmptyStruct, &u))
	assert.Equal(notPointer{0}, u)
}

func TestUnmarshalMustUnmarshal(t *testing.T) {
	a := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type TestStruct struct{ F1 int }

	v := MustMarshal(vs, types.Float(1))
	var out TestStruct
	a.Panics(func() { MustUnmarshal(v, &out) })

	v = MustMarshal(vs, TestStruct{2})
	a.NotPanics(func() { MustUnmarshal(v, &out) })
}
