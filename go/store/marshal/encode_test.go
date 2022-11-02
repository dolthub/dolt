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

package marshal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestEncode(tt *testing.T) {
	vs := newTestValueStore()
	defer vs.Close()

	t := func(exp types.Value, v interface{}) {
		actual, err := Marshal(context.Background(), vs, v)
		assert.NoError(tt, err)
		assert.True(tt, exp.Equals(actual))

		// Encode again for fallthrough
		actual2, err := Marshal(context.Background(), vs, actual)
		assert.NoError(tt, err)
		assert.True(tt, exp.Equals(actual2))
	}

	for _, n := range []float32{0, 42, 3.14159265359, math.MaxFloat32} {
		t(types.Float(n), n)
		t(types.Float(-n), -n)
	}

	for _, n := range []float64{0, 42, 3.14159265359, 9007199254740991, math.MaxFloat64} {
		t(types.Float(n), n)
		t(types.Float(-n), -n)
	}

	for _, n := range []int8{0, 42, math.MaxInt8} {
		t(types.Float(n), n)
		t(types.Float(-n), -n)
	}

	for _, n := range []int16{0, 42, math.MaxInt16} {
		t(types.Float(n), n)
		t(types.Float(-n), -n)
	}

	for _, n := range []int32{0, 42, math.MaxInt32} {
		t(types.Float(n), n)
		t(types.Float(-n), -n)
	}

	// int is at least int32
	for _, n := range []int{0, 42, math.MaxInt32} {
		t(types.Float(n), n)
		t(types.Float(-n), -n)
	}

	for _, n := range []int64{0, 42, math.MaxInt64} {
		t(types.Float(n), n)
		t(types.Float(-n), -n)
	}

	for _, n := range []uint8{0, 42, math.MaxUint8} {
		t(types.Float(n), n)
	}

	for _, n := range []uint16{0, 42, math.MaxUint16} {
		t(types.Float(n), n)
	}

	for _, n := range []uint32{0, 42, math.MaxUint32} {
		t(types.Float(n), n)
	}

	// uint is at least uint32
	for _, n := range []uint{0, 42, math.MaxUint32} {
		t(types.Float(n), n)
	}

	for _, n := range []uint64{0, 42, math.MaxUint64} {
		t(types.Float(n), n)
	}

	t(types.Bool(true), true)
	t(types.Bool(false), false)

	for _, s := range []string{"", "s", "hello", "💩"} {
		t(types.String(s), s)
	}

	t(mustValue(types.NewList(context.Background(), vs, types.Float(42))), mustValue(types.NewList(context.Background(), vs, types.Float(42))))
	t(mustValue(types.NewMap(context.Background(), vs, types.Float(42), types.String("hi"))), mustValue(types.NewMap(context.Background(), vs, types.Float(42), types.String("hi"))))
	t(mustValue(types.NewSet(context.Background(), vs, types.String("bye"))), mustValue(types.NewSet(context.Background(), vs, types.String("bye"))))
	t(mustValue(types.NewBlob(context.Background(), vs, bytes.NewBufferString("hello"))), mustValue(types.NewBlob(context.Background(), vs, bytes.NewBufferString("hello"))))

	type TestStruct struct {
		Str string
		Num float64
	}
	t(mustValue(types.NewStruct(vs.Format(), "TestStruct", types.StructData{
		"num": types.Float(42),
		"str": types.String("Hello"),
	})), TestStruct{Str: "Hello", Num: 42})
	// Same again to test caching
	t(mustValue(types.NewStruct(vs.Format(), "TestStruct", types.StructData{
		"num": types.Float(1),
		"str": types.String("Bye"),
	})), TestStruct{Str: "Bye", Num: 1})

	anonStruct := struct {
		B bool
	}{
		true,
	}
	t(mustValue(types.NewStruct(vs.Format(), "", types.StructData{
		"b": types.Bool(true),
	})), anonStruct)

	type TestNestedStruct struct {
		A types.List
		B TestStruct
		C float64
	}
	t(mustValue(types.NewStruct(vs.Format(), "TestNestedStruct", types.StructData{
		"a": mustValue(types.NewList(context.Background(), vs, types.String("hi"))),
		"b": mustValue(types.NewStruct(vs.Format(), "TestStruct", types.StructData{
			"str": types.String("bye"),
			"num": types.Float(5678),
		})),
		"c": types.Float(1234),
	})), TestNestedStruct{
		A: mustList(types.NewList(context.Background(), vs, types.String("hi"))),
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
	t(mustValue(types.NewStruct(vs.Format(), "TestStruct", types.StructData{
		"num": types.Float(42),
		"str": types.String("Hello"),
	})), testStruct{Str: "Hello", Num: 42})
}

func assertEncodeErrorMessage(t *testing.T, v interface{}, expectedMessage string) {
	vs := newTestValueStore()
	defer vs.Close()

	_, err := Marshal(context.Background(), vs, v)
	assert.Error(t, err)
	assert.Equal(t, expectedMessage, err.Error())
}

func TestInvalidTypes(t *testing.T) {
	assertEncodeErrorMessage(t, make(chan int), "Type is not supported, type: chan int")
	x := 42
	assertEncodeErrorMessage(t, &x, "Type is not supported, type: *int")
}

func TestEncodeEmbeddedStructSkip(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type EmbeddedStruct struct {
		X int
	}
	type TestStruct struct {
		EmbeddedStruct `noms:"-"`
		Y              int
	}
	s := TestStruct{EmbeddedStruct{1}, 2}
	v, err := Marshal(context.Background(), vs, s)
	require.NoError(t, err)
	assert.True(mustValue(types.NewStruct(vs.Format(), "TestStruct", types.StructData{
		"y": types.Float(2),
	})).Equals(v))
}

func TestEncodeEmbeddedStructWithName(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type EmbeddedStruct struct {
		X int
	}
	type TestStruct struct {
		EmbeddedStruct `noms:"em"`
		Y              int
	}
	s := TestStruct{EmbeddedStruct{1}, 2}
	v, err := Marshal(context.Background(), vs, s)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "TestStruct", types.StructData{
		"em": mustStruct(types.NewStruct(vs.Format(), "EmbeddedStruct", types.StructData{
			"x": types.Float(1),
		})),
		"y": types.Float(2),
	})).Equals(v))
}

func TestEncodeEmbeddedStruct(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type EmbeddedStruct struct {
		X int
	}
	type TestStruct struct {
		EmbeddedStruct
	}
	s := TestStruct{EmbeddedStruct{1}}
	v, err := Marshal(context.Background(), vs, s)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "TestStruct", types.StructData{
		"x": types.Float(1),
	})).Equals(v))

	type TestOuter struct {
		A int
		TestStruct
		B int
	}
	s2 := TestOuter{0, TestStruct{EmbeddedStruct{1}}, 2}
	v2, err := Marshal(context.Background(), vs, s2)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "TestOuter", types.StructData{
		"a": types.Float(0),
		"b": types.Float(2),
		"x": types.Float(1),
	})).Equals(v2))
}

func TestEncodeEmbeddedStructOriginal(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type EmbeddedStruct struct {
		X int
		O types.Struct `noms:",original"`
		B bool
	}
	type TestStruct struct {
		EmbeddedStruct
	}
	s := TestStruct{
		EmbeddedStruct: EmbeddedStruct{
			X: 1,
			B: true,
		},
	}
	v, err := Marshal(context.Background(), vs, s)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "TestStruct", types.StructData{
		"b": types.Bool(true),
		"x": types.Float(1),
	})).Equals(v))
}

func TestEncodeNonExportedField(t *testing.T) {
	type TestStruct struct {
		x int
	}
	assertEncodeErrorMessage(t, TestStruct{1}, "Non exported fields are not supported, type: marshal.TestStruct")
}

func TestEncodeTaggingSkip(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type S struct {
		Abc int `noms:"-"`
		Def bool
	}
	s := S{42, true}
	v, err := Marshal(context.Background(), vs, s)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{
		"def": types.Bool(true),
	})).Equals(v))
}

func TestEncodeNamedFields(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type S struct {
		Aaa int  `noms:"a"`
		Bbb bool `noms:"B"`
		Ccc string
	}
	s := S{42, true, "Hi"}
	v, err := Marshal(context.Background(), vs, s)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{
		"a":   types.Float(42),
		"B":   types.Bool(true),
		"ccc": types.String("Hi"),
	})).Equals(v))
}

func TestEncodeInvalidNamedFields(t *testing.T) {
	type S struct {
		A int `noms:"1a"`
	}
	assertEncodeErrorMessage(t, S{42}, "Invalid struct field name: 1a")
}

func TestEncodeOmitEmpty(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type S struct {
		String  string  `noms:",omitempty"`
		Bool    bool    `noms:",omitempty"`
		Int     int     `noms:",omitempty"`
		Int8    int8    `noms:",omitempty"`
		Int16   int16   `noms:",omitempty"`
		Int32   int32   `noms:",omitempty"`
		Int64   int64   `noms:",omitempty"`
		Uint    uint    `noms:",omitempty"`
		Uint8   uint8   `noms:",omitempty"`
		Uint16  uint16  `noms:",omitempty"`
		Uint32  uint32  `noms:",omitempty"`
		Uint64  uint64  `noms:",omitempty"`
		Float32 float32 `noms:",omitempty"`
		Float64 float64 `noms:",omitempty"`
	}
	s := S{
		String:  "s",
		Bool:    true,
		Int:     1,
		Int8:    1,
		Int16:   1,
		Int32:   1,
		Int64:   1,
		Uint:    1,
		Uint8:   1,
		Uint16:  1,
		Uint32:  1,
		Uint64:  1,
		Float32: 1,
		Float64: 1,
	}
	v, err := Marshal(context.Background(), vs, s)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{
		"string":  types.String("s"),
		"bool":    types.Bool(true),
		"int":     types.Float(1),
		"int8":    types.Float(1),
		"int16":   types.Float(1),
		"int32":   types.Float(1),
		"int64":   types.Float(1),
		"uint":    types.Float(1),
		"uint8":   types.Float(1),
		"uint16":  types.Float(1),
		"uint32":  types.Float(1),
		"uint64":  types.Float(1),
		"float32": types.Float(1),
		"float64": types.Float(1),
	})).Equals(v))

	s2 := S{
		String:  "",
		Bool:    false,
		Int:     0,
		Int8:    0,
		Int16:   0,
		Int32:   0,
		Int64:   0,
		Uint:    0,
		Uint8:   0,
		Uint16:  0,
		Uint32:  0,
		Uint64:  0,
		Float32: 0,
		Float64: 0,
	}
	v2, err := Marshal(context.Background(), vs, s2)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{})).Equals(v2))

	type S2 struct {
		Slice []int       `noms:",omitempty"`
		Map   map[int]int `noms:",omitempty"`
	}

	s3 := S2{
		Slice: []int{0},
		Map:   map[int]int{0: 0},
	}
	v3, err := Marshal(context.Background(), vs, s3)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S2", types.StructData{
		"slice": mustList(types.NewList(context.Background(), vs, types.Float(0))),
		"map":   mustValue(types.NewMap(context.Background(), vs, types.Float(0), types.Float(0))),
	})).Equals(v3))

	s4 := S2{
		Slice: []int{},
		Map:   map[int]int{},
	}
	v4, err := Marshal(context.Background(), vs, s4)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S2", types.StructData{})).Equals(v4))

	s5 := S2{
		Slice: nil,
		Map:   nil,
	}
	v5, err := Marshal(context.Background(), vs, s5)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S2", types.StructData{})).Equals(v5))

	type S3 struct {
		List  types.List  `noms:",omitempty"`
		Value types.Value `noms:",omitempty"`
	}
	s6 := S3{
		List:  mustList(types.NewList(context.Background(), vs)),
		Value: types.Float(0),
	}
	v6, err := Marshal(context.Background(), vs, s6)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S3", types.StructData{
		"list":  mustList(types.NewList(context.Background(), vs)),
		"value": types.Float(0),
	})).Equals(v6))

	s7 := S3{
		List:  types.List{},
		Value: nil,
	}
	v7, err := Marshal(context.Background(), vs, s7)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S3", types.StructData{})).Equals(v7))

	// Both name and omitempty
	type S4 struct {
		X int `noms:"y,omitempty"`
	}
	s8 := S4{
		X: 1,
	}
	v8, err := Marshal(context.Background(), vs, s8)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S4", types.StructData{
		"y": types.Float(1),
	})).Equals(v8))

	s9 := S4{
		X: 0,
	}
	v9, err := Marshal(context.Background(), vs, s9)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S4", types.StructData{})).Equals(v9))
}

func ExampleMarshal() {
	vs := newTestValueStore()
	defer vs.Close()

	type Person struct {
		Given string
		Male  bool
	}
	arya, err := Marshal(context.Background(), vs, Person{"Arya", false})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Given: %s, Male: %t\n", mustGetValue(arya.(types.Struct).MaybeGet("given")).(types.String), mustGetValue(arya.(types.Struct).MaybeGet("male")).(types.Bool))
	// Output: Given: Arya, Male: false
}

func TestEncodeSlice(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	v, err := Marshal(context.Background(), vs, []string{"a", "b", "c"})
	require.NoError(t, err)
	assert.True(mustList(types.NewList(context.Background(), vs, types.String("a"), types.String("b"), types.String("c"))).Equals(v))
}

func TestEncodeArray(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	v, err := Marshal(context.Background(), vs, [3]int{1, 2, 3})
	require.NoError(t, err)
	assert.True(mustList(types.NewList(context.Background(), vs, types.Float(1), types.Float(2), types.Float(3))).Equals(v))
}

func TestEncodeStructWithSlice(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type S struct {
		List []int
	}
	v, err := Marshal(context.Background(), vs, S{[]int{1, 2, 3}})
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{
		"list": mustList(types.NewList(context.Background(), vs, types.Float(1), types.Float(2), types.Float(3))),
	})).Equals(v))
}

func TestEncodeStructWithArrayOfNomsValue(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type S struct {
		List [1]types.Set
	}
	v, err := Marshal(context.Background(), vs, S{[1]types.Set{mustSet(types.NewSet(context.Background(), vs, types.Bool(true)))}})
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{
		"list": mustList(types.NewList(context.Background(), vs, mustSet(types.NewSet(context.Background(), vs, types.Bool(true))))),
	})).Equals(v))
}

func TestEncodeNomsTypePtr(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	testMarshal := func(g interface{}, expected types.Value) {
		v, err := Marshal(context.Background(), vs, g)
		require.NoError(t, err)
		assert.Equal(expected, v)
	}

	type S struct {
		Type *types.Type
	}

	primitive := types.PrimitiveTypeMap[types.StringKind]
	testMarshal(S{primitive}, mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{"type": primitive})))

	complex := mustType(types.MakeStructType("Complex",
		types.StructField{
			Name: "stuff",
			Type: types.PrimitiveTypeMap[types.StringKind],
		},
	))
	testMarshal(S{complex}, mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{"type": complex})))
}

func TestEncodeRecursive(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type Node struct {
		Value    int
		Children []Node
	}
	v, err := Marshal(context.Background(), vs, Node{
		1, []Node{
			{2, []Node{}},
			{3, []Node(nil)},
		},
	})
	require.NoError(t, err)

	typ, err := types.MakeStructType("Node",
		types.StructField{
			Name: "children",
			Type: mustType(types.MakeListType(types.MakeCycleType("Node"))),
		},
		types.StructField{
			Name: "value",
			Type: types.PrimitiveTypeMap[types.FloatKind],
		},
	)
	require.NoError(t, err)
	tp, err := types.TypeOf(v)
	require.NoError(t, err)
	assert.True(typ.Equals(tp))

	assert.True(mustStruct(types.NewStruct(vs.Format(), "Node", types.StructData{
		"children": mustValue(types.NewList(context.Background(),
			vs,
			mustStruct(types.NewStruct(vs.Format(), "Node", types.StructData{
				"children": mustValue(types.NewList(context.Background(), vs)),
				"value":    types.Float(2),
			})),
			mustStruct(types.NewStruct(vs.Format(), "Node", types.StructData{
				"children": mustValue(types.NewList(context.Background(), vs)),
				"value":    types.Float(3),
			})),
		)),
		"value": types.Float(1),
	})).Equals(v))
}

func TestEncodeMap(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	v, err := Marshal(context.Background(), vs, map[string]int{"a": 1, "b": 2, "c": 3})
	require.NoError(t, err)
	assert.True(mustValue(types.NewMap(context.Background(),
		vs,
		types.String("a"), types.Float(1),
		types.String("b"), types.Float(2),
		types.String("c"), types.Float(3))).Equals(v))

	type S struct {
		N string
	}
	v, err = Marshal(context.Background(), vs, map[S]bool{S{"Yes"}: true, S{"No"}: false})
	require.NoError(t, err)
	assert.True(mustValue(types.NewMap(context.Background(),
		vs,
		mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{"n": types.String("Yes")})), types.Bool(true),
		mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{"n": types.String("No")})), types.Bool(false))).Equals(v))

	v, err = Marshal(context.Background(), vs, map[string]int(nil))
	require.NoError(t, err)
	assert.True(mustValue(types.NewMap(context.Background(), vs)).Equals(v))

	v, err = Marshal(context.Background(), vs, map[string]int{})
	require.NoError(t, err)
	assert.True(mustValue(types.NewMap(context.Background(), vs)).Equals(v))
}

func TestEncodeInterface(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	var i interface{} = []string{"a", "b"}
	v, err := Marshal(context.Background(), vs, i)
	require.NoError(t, err)
	assert.True(mustValue(types.NewList(context.Background(), vs, types.String("a"), types.String("b"))).Equals(v))

	i = map[interface{}]interface{}{"a": true, struct{ Name string }{"b"}: 42}
	v, err = Marshal(context.Background(), vs, i)
	require.NoError(t, err)
	assert.True(mustValue(types.NewMap(context.Background(),
		vs,
		types.String("a"), types.Bool(true),
		mustStruct(types.NewStruct(vs.Format(), "", types.StructData{"name": types.String("b")})), types.Float(42),
	)).Equals(v))
}

func TestEncodeSet(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	v, err := Marshal(context.Background(), vs, struct {
		A map[int]struct{} `noms:",set"`
		B map[int]struct{}
		C map[int]string      `noms:",set"`
		D map[string]struct{} `noms:",set"`
		E map[string]struct{}
		F map[string]int `noms:",set"`
		G []int          `noms:",set"`
		H string         `noms:",set"`
	}{
		map[int]struct{}{0: {}, 1: {}, 2: {}},
		map[int]struct{}{3: {}, 4: {}, 5: {}},
		map[int]string{},
		map[string]struct{}{"A": {}, "B": {}, "C": {}},
		map[string]struct{}{"D": {}, "E": {}, "F": {}},
		map[string]int{},
		[]int{1, 2, 3},
		"",
	})
	require.NoError(t, err)
	s, ok := v.(types.Struct)
	assert.True(ok)

	expect := map[string]types.NomsKind{
		"a": types.SetKind,
		"b": types.MapKind,
		"c": types.MapKind,
		"d": types.SetKind,
		"e": types.MapKind,
		"f": types.MapKind,
		"g": types.SetKind,
		"h": types.StringKind,
	}
	for fieldName, kind := range expect {
		assert.Equal(kind, mustGetValue(s.MaybeGet(fieldName)).Kind())
	}

	// Test both the Set values are correct, and that the equivalent typed Map
	// are correct in case the Set marshaling interferes with it.

	val, ok, err := s.MaybeGet("a")
	require.NoError(t, err)
	assert.True(ok)
	a := val.(types.Set)
	assert.True(a.Has(context.Background(), types.Float(0)))
	assert.True(a.Has(context.Background(), types.Float(1)))
	assert.True(a.Has(context.Background(), types.Float(2)))

	val, ok, err = s.MaybeGet("b")
	require.NoError(t, err)
	assert.True(ok)
	b := val.(types.Map)
	assert.True(b.Has(context.Background(), types.Float(3)))
	assert.True(b.Has(context.Background(), types.Float(4)))
	assert.True(b.Has(context.Background(), types.Float(5)))

	val, ok, err = s.MaybeGet("d")
	require.NoError(t, err)
	assert.True(ok)
	d := val.(types.Set)
	assert.True(d.Has(context.Background(), types.String("A")))
	assert.True(d.Has(context.Background(), types.String("B")))
	assert.True(d.Has(context.Background(), types.String("C")))

	val, ok, err = s.MaybeGet("e")
	require.NoError(t, err)
	assert.True(ok)
	e := val.(types.Map)
	assert.True(e.Has(context.Background(), types.String("D")))
	assert.True(e.Has(context.Background(), types.String("E")))
	assert.True(e.Has(context.Background(), types.String("F")))

	val, ok, err = s.MaybeGet("g")
	require.NoError(t, err)
	assert.True(ok)
	g := val.(types.Set)
	assert.True(g.Has(context.Background(), types.Float(1)))
	assert.True(g.Has(context.Background(), types.Float(2)))
	assert.True(g.Has(context.Background(), types.Float(3)))
}

func TestEncodeOpt(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	tc := []struct {
		in        interface{}
		opt       Opt
		wantValue types.Value
	}{
		{
			[]string{"a", "b"},
			Opt{},
			mustValue(types.NewList(context.Background(), vs, types.String("a"), types.String("b"))),
		},
		{
			[]string{"a", "b"},
			Opt{Set: true},
			mustValue(types.NewSet(context.Background(), vs, types.String("a"), types.String("b"))),
		},
		{
			map[string]struct{}{"a": {}, "b": {}},
			Opt{},
			mustValue(types.NewMap(context.Background(), vs,
				types.String("a"), mustStruct(types.NewStruct(vs.Format(), "", nil)),
				types.String("b"), mustStruct(types.NewStruct(vs.Format(), "", nil)),
			)),
		},
		{
			map[string]struct{}{"a": {}, "b": {}},
			Opt{Set: true},
			mustSet(types.NewSet(context.Background(), vs, types.String("a"), types.String("b"))),
		},
	}

	for _, t := range tc {
		r, err := MarshalOpt(context.Background(), vs, t.in, t.opt)
		assert.True(t.wantValue.Equals(r))
		assert.Nil(err)
	}
}

func TestEncodeSetWithTags(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	v, err := Marshal(context.Background(), vs, struct {
		A map[int]struct{} `noms:"foo,set"`
		B map[int]struct{} `noms:",omitempty,set"`
		C map[int]struct{} `noms:"bar,omitempty,set"`
	}{
		A: map[int]struct{}{0: {}, 1: {}},
		C: map[int]struct{}{2: {}, 3: {}},
	})
	require.NoError(t, err)
	s, ok := v.(types.Struct)
	assert.True(ok)

	_, ok, err = s.MaybeGet("a")
	require.NoError(t, err)
	assert.False(ok)
	_, ok, err = s.MaybeGet("b")
	require.NoError(t, err)
	assert.False(ok)
	_, ok, err = s.MaybeGet("c")
	require.NoError(t, err)
	assert.False(ok)

	val, ok, err := s.MaybeGet("foo")
	require.NoError(t, err)
	foo := val.(types.Set)
	require.NoError(t, err)
	assert.True(ok)
	assert.True(mustSet(types.NewSet(context.Background(), vs, types.Float(0), types.Float(1))).Equals(foo))

	val, ok, err = s.MaybeGet("bar")
	require.NoError(t, err)
	bar := val.(types.Set)
	require.NoError(t, err)
	assert.True(ok)
	assert.True(mustSet(types.NewSet(context.Background(), vs, types.Float(2), types.Float(3))).Equals(bar))
}

func TestInvalidTag(t *testing.T) {
	vs := newTestValueStore()
	defer vs.Close()

	_, err := Marshal(context.Background(), vs, struct {
		F string `noms:",omitEmpty"`
	}{"F"})
	assert.Error(t, err)
	assert.Equal(t, `Unrecognized tag: omitEmpty`, err.Error())
}

func TestEncodeCanSkipUnexportedField(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type S struct {
		Abc         int
		notExported bool `noms:"-"`
	}
	s := S{42, true}
	v, err := Marshal(context.Background(), vs, s)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{
		"abc": types.Float(42),
	})).Equals(v))
}

func TestEncodeOriginal(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type S struct {
		Foo int          `noms:",omitempty"`
		Bar types.Struct `noms:",original"`
	}

	var s S
	var err error

	// New field value clobbers old field value
	orig := mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{
		"foo": types.Float(42),
	}))
	err = Unmarshal(context.Background(), vs.Format(), orig, &s)
	require.NoError(t, err)
	s.Foo = 43
	assert.True(mustValue(Marshal(context.Background(), vs, s)).Equals(mustStruct(orig.Set("foo", types.Float(43)))))

	// New field extends old struct
	orig, err = types.NewStruct(vs.Format(), "S", types.StructData{})
	require.NoError(t, err)
	err = Unmarshal(context.Background(), vs.Format(), orig, &s)
	require.NoError(t, err)
	s.Foo = 43
	assert.True(mustValue(Marshal(context.Background(), vs, s)).Equals(mustStruct(orig.Set("foo", types.Float(43)))))

	// Old struct name always used
	orig = mustStruct(types.NewStruct(vs.Format(), "Q", types.StructData{}))
	err = Unmarshal(context.Background(), vs.Format(), orig, &s)
	require.NoError(t, err)
	s.Foo = 43
	assert.True(mustValue(Marshal(context.Background(), vs, s)).Equals(mustStruct(orig.Set("foo", types.Float(43)))))

	// Field type of base are preserved
	orig = mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{
		"foo": types.Float(42),
	}))
	err = Unmarshal(context.Background(), vs.Format(), orig, &s)
	require.NoError(t, err)
	s.Foo = 43
	out, err := Marshal(context.Background(), vs, s)
	require.NoError(t, err)
	assert.True(out.Equals(mustStruct(orig.Set("foo", types.Float(43)))))

	st2 := mustType(types.MakeStructTypeFromFields("S", types.FieldMap{
		"foo": types.PrimitiveTypeMap[types.FloatKind],
	}))
	assert.True(mustType(types.TypeOf(out)).Equals(st2))

	// It's OK to have an empty original field
	s = S{
		Foo: 42,
	}
	assert.True(mustValue(Marshal(context.Background(), vs, s)).Equals(
		mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{"foo": types.Float(float64(42))}))))
}

func TestNomsTypes(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	type S struct {
		Blob   types.Blob
		Bool   types.Bool
		Number types.Float
		String types.String
		Type   *types.Type
	}
	s := S{
		Blob:   mustBlob(types.NewBlob(context.Background(), vs)),
		Bool:   types.Bool(true),
		Number: types.Float(42),
		String: types.String("hi"),
		Type:   types.PrimitiveTypeMap[types.FloatKind],
	}
	assert.True(mustValue(Marshal(context.Background(), vs, s)).Equals(
		mustStruct(types.NewStruct(vs.Format(), "S", types.StructData{
			"blob":   mustBlob(types.NewBlob(context.Background(), vs)),
			"bool":   types.Bool(true),
			"number": types.Float(42),
			"string": types.String("hi"),
			"type":   types.PrimitiveTypeMap[types.FloatKind],
		})),
	))
}

type primitiveType int

func (t primitiveType) MarshalNoms(vrw types.ValueReadWriter) (types.Value, error) {
	return types.Float(int(t) + 1), nil
}

func TestMarshalerPrimitiveType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	u := primitiveType(42)
	v, err := Marshal(context.Background(), vs, u)
	require.NoError(t, err)
	assert.Equal(types.Float(43), v)
}

type primitiveSliceType []string

func (u primitiveSliceType) MarshalNoms(vrw types.ValueReadWriter) (types.Value, error) {
	return types.String(strings.Join(u, ",")), nil
}

func TestMarshalerPrimitiveSliceType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	u := primitiveSliceType([]string{"a", "b", "c"})
	v, err := Marshal(context.Background(), vs, u)
	require.NoError(t, err)
	assert.Equal(types.String("a,b,c"), v)
}

type primitiveMapType map[string]string

func (u primitiveMapType) MarshalNoms(vrw types.ValueReadWriter) (types.Value, error) {
	var vals types.ValueSlice
	for k, v := range u {
		vals = append(vals, types.String(k+","+v))
	}
	return types.NewSet(context.Background(), vrw, vals...)
}

func TestMarshalerPrimitiveMapType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	u := primitiveMapType(map[string]string{
		"a": "foo",
		"b": "bar",
	})
	v, err := Marshal(context.Background(), vs, u)
	require.NoError(t, err)
	assert.True(mustSet(types.NewSet(context.Background(), vs, types.String("a,foo"), types.String("b,bar"))).Equals(v))
}

type primitiveStructType struct {
	x, y int
}

func (u primitiveStructType) MarshalNoms(vrw types.ValueReadWriter) (types.Value, error) {
	return types.Float(u.x + u.y), nil
}

func TestMarshalerPrimitiveStructType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	u := primitiveStructType{1, 2}
	v, err := Marshal(context.Background(), vs, u)
	require.NoError(t, err)
	assert.Equal(types.Float(3), v)
}

type builtinType regexp.Regexp

func (u builtinType) MarshalNoms(vrw types.ValueReadWriter) (types.Value, error) {
	r := regexp.Regexp(u)
	return types.String(r.String()), nil
}

func TestMarshalerBuiltinType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	s := "[a-z]+$"
	r := regexp.MustCompile(s)
	u := builtinType(*r)
	v, err := Marshal(context.Background(), vs, u)
	require.NoError(t, err)
	assert.Equal(types.String(s), v)
}

type wrappedMarshalerType primitiveType

func (u wrappedMarshalerType) MarshalNoms(vrw types.ValueReadWriter) (types.Value, error) {
	return types.Float(int(u) + 2), nil
}

func TestMarshalerWrapperMarshalerType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	u := wrappedMarshalerType(primitiveType(42))
	v, err := Marshal(context.Background(), vs, u)
	require.NoError(t, err)
	assert.Equal(types.Float(44), v)
}

type TestComplexStructType struct {
	P       primitiveType
	Ps      []primitiveType
	Pm      map[string]primitiveType
	Pslice  primitiveSliceType
	Pmap    primitiveMapType
	Pstruct primitiveStructType
	B       builtinType
}

func TestMarshalerComplexStructType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	s := "foo|bar"
	r := regexp.MustCompile(s)
	u := TestComplexStructType{
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
		Pstruct: primitiveStructType{10, 20},
		B:       builtinType(*r),
	}

	v, err := Marshal(context.Background(), vs, u)
	require.NoError(t, err)

	assert.True(mustStruct(types.NewStruct(vs.Format(), "TestComplexStructType", types.StructData{
		"p":       types.Float(43),
		"ps":      mustValue(types.NewList(context.Background(), vs, types.Float(2), types.Float(3))),
		"pm":      mustValue(types.NewMap(context.Background(), vs, types.String("x"), types.Float(101), types.String("y"), types.Float(102))),
		"pslice":  types.String("a,b,c"),
		"pmap":    mustValue(types.NewSet(context.Background(), vs, types.String("c,123"), types.String("d,456"))),
		"pstruct": types.Float(30),
		"b":       types.String(s),
	})).Equals(v))
}

type returnsMarshalerError struct {
	err error
}

func (u returnsMarshalerError) MarshalNoms(vrw types.ValueReadWriter) (types.Value, error) {
	return nil, u.err
}

type returnsMarshalerNil struct{}

func (u returnsMarshalerNil) MarshalNoms(vrw types.ValueReadWriter) (types.Value, error) {
	return nil, nil
}

func TestMarshalerErrors(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	expErr := errors.New("expected error")
	m1 := returnsMarshalerError{expErr}
	_, actErr := Marshal(context.Background(), vs, m1)
	assert.Equal(&marshalNomsError{expErr}, actErr)

	m2 := returnsMarshalerNil{}
	_, err := Marshal(context.Background(), vs, m2)
	assert.Error(err)
}

type TestStructWithNameImpl struct {
	X int
}

func (ts TestStructWithNameImpl) MarshalNomsStructName() string {
	return "A"
}
func TestMarshalStructName(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	ts := TestStructWithNameImpl{
		X: 1,
	}
	v, err := Marshal(context.Background(), vs, ts)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "A", types.StructData{
		"x": types.Float(1),
	})).Equals(v), mustString(types.EncodedValue(context.Background(), v)))
}

type TestStructWithNameImpl2 struct {
	X int
}

func (ts TestStructWithNameImpl2) MarshalNomsStructName() string {
	return ""
}
func TestMarshalStructName2(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	ts := TestStructWithNameImpl2{
		X: 1,
	}
	v, err := Marshal(context.Background(), vs, ts)
	require.NoError(t, err)
	assert.True(mustStruct(types.NewStruct(vs.Format(), "", types.StructData{
		"x": types.Float(1),
	})).Equals(v), mustString(types.EncodedValue(context.Background(), v)))
}
