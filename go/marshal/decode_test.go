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
}

func TestDecodeNilPointer(t *testing.T) {
	var x *bool = nil
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

	var slice []bool
	t(&slice, "[]bool")

	var array [2]bool
	t(&array, "[2]bool")

	var m map[string]bool
	t(&m, "map[string]bool")

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

func TestDecodeWrongStructName(tt *testing.T) {
	type TestStruct struct {
		X int
	}
	var ts TestStruct
	assertDecodeErrorMessage(tt, types.NewStruct("Abc", types.StructData{
		"X": types.Number(42),
	}), &ts, "Cannot unmarshal struct Abc {\n  X: Number,\n} into Go value of type marshal.TestStruct, names do not match")
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
