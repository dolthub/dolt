// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package marshal

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestEncode(tt *testing.T) {
	t := func(exp types.Value, v interface{}) {
		actual, err := Marshal(v)
		assert.NoError(tt, err)
		assert.True(tt, exp.Equals(actual))

		// Encode again for fallthrough
		actual2, err := Marshal(actual)
		assert.NoError(tt, err)
		assert.True(tt, exp.Equals(actual2))
	}

	for _, n := range []float32{0, 42, 3.14159265359, math.MaxFloat32} {
		t(types.Number(n), n)
		t(types.Number(-n), -n)
	}

	for _, n := range []float64{0, 42, 3.14159265359, 9007199254740991, math.MaxFloat64} {
		t(types.Number(n), n)
		t(types.Number(-n), -n)
	}

	for _, n := range []int8{0, 42, math.MaxInt8} {
		t(types.Number(n), n)
		t(types.Number(-n), -n)
	}

	for _, n := range []int16{0, 42, math.MaxInt16} {
		t(types.Number(n), n)
		t(types.Number(-n), -n)
	}

	for _, n := range []int32{0, 42, math.MaxInt32} {
		t(types.Number(n), n)
		t(types.Number(-n), -n)
	}

	// int is at least int32
	for _, n := range []int{0, 42, math.MaxInt32} {
		t(types.Number(n), n)
		t(types.Number(-n), -n)
	}

	for _, n := range []int64{0, 42, math.MaxInt64} {
		t(types.Number(n), n)
		t(types.Number(-n), -n)
	}

	for _, n := range []uint8{0, 42, math.MaxUint8} {
		t(types.Number(n), n)
	}

	for _, n := range []uint16{0, 42, math.MaxUint16} {
		t(types.Number(n), n)
	}

	for _, n := range []uint32{0, 42, math.MaxUint32} {
		t(types.Number(n), n)
	}

	// uint is at least uint32
	for _, n := range []uint{0, 42, math.MaxUint32} {
		t(types.Number(n), n)
	}

	for _, n := range []uint64{0, 42, math.MaxUint64} {
		t(types.Number(n), n)
	}

	t(types.Bool(true), true)
	t(types.Bool(false), false)

	for _, s := range []string{"", "s", "hello", "💩"} {
		t(types.String(s), s)
	}

	t(types.NewList(types.Number(42)), types.NewList(types.Number(42)))
	t(types.NewMap(types.Number(42), types.String("hi")), types.NewMap(types.Number(42), types.String("hi")))
	t(types.NewSet(types.String("bye")), types.NewSet(types.String("bye")))
	t(types.NewBlob(bytes.NewBufferString("hello")), types.NewBlob(bytes.NewBufferString("hello")))

	type TestStruct struct {
		Str string
		Num float64
	}
	t(types.NewStruct("TestStruct", types.StructData{
		"num": types.Number(42),
		"str": types.String("Hello"),
	}), TestStruct{Str: "Hello", Num: 42})
	// Same again to test caching
	t(types.NewStruct("TestStruct", types.StructData{
		"num": types.Number(1),
		"str": types.String("Bye"),
	}), TestStruct{Str: "Bye", Num: 1})

	anonStruct := struct {
		B bool
	}{
		true,
	}
	t(types.NewStruct("", types.StructData{
		"b": types.Bool(true),
	}), anonStruct)

	type TestNestedStruct struct {
		A types.List
		B TestStruct
		C float64
	}
	t(types.NewStruct("TestNestedStruct", types.StructData{
		"a": types.NewList(types.String("hi")),
		"b": types.NewStruct("TestStruct", types.StructData{
			"str": types.String("bye"),
			"num": types.Number(5678),
		}),
		"c": types.Number(1234),
	}), TestNestedStruct{
		A: types.NewList(types.String("hi")),
		B: TestStruct{
			Str: "bye",
			Num: 5678,
		},
		C: 1234,
	})

	type testStruct struct {
		Str string
		Num float64
	}
	t(types.NewStruct("TestStruct", types.StructData{
		"num": types.Number(42),
		"str": types.String("Hello"),
	}), testStruct{Str: "Hello", Num: 42})
}

func assertEncodeErrorMessage(t *testing.T, v interface{}, expectedMessage string) {
	_, err := Marshal(v)
	assert.Error(t, err)
	assert.Equal(t, expectedMessage, err.Error())
}

func TestInvalidTypes(t *testing.T) {
	assertEncodeErrorMessage(t, make(chan int), "Type is not supported, type: chan int")
	x := 42
	assertEncodeErrorMessage(t, &x, "Type is not supported, type: *int")
}

func TestEncodeEmbeddedStruct(t *testing.T) {
	type EmbeddedStruct struct{}
	type TestStruct struct {
		EmbeddedStruct
	}
	assertEncodeErrorMessage(t, TestStruct{EmbeddedStruct{}}, "Embedded structs are not supported, type: marshal.TestStruct")
}

func TestEncodeNonExportedField(t *testing.T) {
	type TestStruct struct {
		x int
	}
	assertEncodeErrorMessage(t, TestStruct{1}, "Non exported fields are not supported, type: marshal.TestStruct")
}

func TestEncodeTaggingSkip(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		Abc int `noms:"-"`
		Def bool
	}
	s := S{42, true}
	v, err := Marshal(s)
	assert.NoError(err)
	assert.True(types.NewStruct("S", types.StructData{
		"def": types.Bool(true),
	}).Equals(v))
}

func TestEncodeNamedFields(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		Aaa int  `noms:"a"`
		Bbb bool `noms:"B"`
		Ccc string
	}
	s := S{42, true, "Hi"}
	v, err := Marshal(s)
	assert.NoError(err)
	assert.True(types.NewStruct("S", types.StructData{
		"a":   types.Number(42),
		"B":   types.Bool(true),
		"ccc": types.String("Hi"),
	}).Equals(v))
}

func TestEncodeInvalidNamedFields(t *testing.T) {
	type S struct {
		A int `noms:"1a"`
	}
	assertEncodeErrorMessage(t, S{42}, "Invalid struct field name: 1a")
}

func ExampleMarshal() {
	type Person struct {
		Given string
		Male  bool
	}
	arya, err := Marshal(Person{"Arya", false})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Given: %s, Male: %t\n", arya.(types.Struct).Get("given").(types.String), arya.(types.Struct).Get("male").(types.Bool))
	// Output: Given: Arya, Male: false
}

func TestEncodeSlice(t *testing.T) {
	assert := assert.New(t)
	v, err := Marshal([]string{"a", "b", "c"})
	assert.NoError(err)
	assert.True(types.NewList(types.String("a"), types.String("b"), types.String("c")).Equals(v))
}

func TestEncodeArray(t *testing.T) {
	assert := assert.New(t)
	v, err := Marshal([3]int{1, 2, 3})
	assert.NoError(err)
	assert.True(types.NewList(types.Number(1), types.Number(2), types.Number(3)).Equals(v))
}

func TestEncodeStructWithSlice(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		List []int
	}
	v, err := Marshal(S{[]int{1, 2, 3}})
	assert.NoError(err)
	assert.True(types.NewStruct("S", types.StructData{
		"list": types.NewList(types.Number(1), types.Number(2), types.Number(3)),
	}).Equals(v))
}

func TestEncodeStructWithArrayOfNomsValue(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		List [1]types.Set
	}
	v, err := Marshal(S{[1]types.Set{types.NewSet(types.Bool(true))}})
	assert.NoError(err)
	assert.True(types.NewStruct("S", types.StructData{
		"list": types.NewList(types.NewSet(types.Bool(true))),
	}).Equals(v))
}

func TestEncodeRecursive(t *testing.T) {
	assert := assert.New(t)

	type Node struct {
		Value    int
		Children []Node
	}
	v, err := Marshal(Node{
		1, []Node{
			{2, []Node{}},
			{3, []Node(nil)},
		},
	})
	assert.NoError(err)

	typ := types.MakeStructType("Node", []string{"children", "value"}, []*types.Type{
		types.MakeListType(types.MakeCycleType(0)),
		types.NumberType,
	})
	assert.True(typ.Equals(v.Type()))

	assert.True(types.NewStructWithType(typ, types.ValueSlice{
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
	}).Equals(v))
}

func TestEncodeMap(t *testing.T) {
	assert := assert.New(t)

	v, err := Marshal(map[string]int{"a": 1, "b": 2, "c": 3})
	assert.NoError(err)
	assert.True(types.NewMap(
		types.String("a"), types.Number(1),
		types.String("b"), types.Number(2),
		types.String("c"), types.Number(3)).Equals(v))

	type S struct {
		N string
	}
	v, err = Marshal(map[S]bool{S{"Yes"}: true, S{"No"}: false})
	assert.NoError(err)
	assert.True(types.NewMap(
		types.NewStruct("S", types.StructData{"n": types.String("Yes")}), types.Bool(true),
		types.NewStruct("S", types.StructData{"n": types.String("No")}), types.Bool(false)).Equals(v))

	v, err = Marshal(map[string]int(nil))
	assert.NoError(err)
	assert.True(types.NewMap().Equals(v))

	v, err = Marshal(map[string]int{})
	assert.NoError(err)
	assert.True(types.NewMap().Equals(v))
}

func TestEncodeInterface(t *testing.T) {
	assert := assert.New(t)

	var i interface{}
	i = []string{"a", "b"}
	v, err := Marshal(i)
	assert.NoError(err)
	assert.True(types.NewList(types.String("a"), types.String("b")).Equals(v))

	i = map[interface{}]interface{}{"a": true, struct{ Name string }{"b"}: 42}
	v, err = Marshal(i)
	assert.NoError(err)
	assert.True(types.NewMap(
		types.String("a"), types.Bool(true),
		types.NewStruct("", types.StructData{"name": types.String("b")}), types.Number(42),
	).Equals(v))
}

type TestInterface interface {
	M()
}
type TestImpl int

func (impl TestImpl) M() {}
