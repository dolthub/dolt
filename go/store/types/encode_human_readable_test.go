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

package types

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/util/test"
	"github.com/dolthub/dolt/go/store/util/writers"
)

func assertWriteHRSEqual(t *testing.T, expected string, v Value) {
	assert := assert.New(t)
	var buf bytes.Buffer
	w := &hrsWriter{w: &buf, floatFormat: 'g'}
	w.Write(context.Background(), v)
	assert.Equal(test.RemoveHashes(expected), test.RemoveHashes(buf.String()))
}

func TestWriteHumanReadablePrimitiveValues(t *testing.T) {
	assertWriteHRSEqual(t, "true", Bool(true))
	assertWriteHRSEqual(t, "false", Bool(false))

	assertWriteHRSEqual(t, "0", Float(0))
	assertWriteHRSEqual(t, "42", Float(42))

	assertWriteHRSEqual(t, "-42", Float(-42))

	assertWriteHRSEqual(t, "3.1415926535", Float(3.1415926535))
	assertWriteHRSEqual(t, "314159.26535", Float(3.1415926535e5))
	assertWriteHRSEqual(t, "3.1415926535e+20", Float(3.1415926535e20))

	assertWriteHRSEqual(t, `"abc"`, String("abc"))
	assertWriteHRSEqual(t, `" "`, String(" "))
	assertWriteHRSEqual(t, `"\t"`, String("\t"))
	assertWriteHRSEqual(t, `"\t"`, String("	"))
	assertWriteHRSEqual(t, `"\n"`, String("\n"))
	assertWriteHRSEqual(t, `"\n"`, String(`
`))
	assertWriteHRSEqual(t, `"\r"`, String("\r"))
	assertWriteHRSEqual(t, `"\r\n"`, String("\r\n"))
	assertWriteHRSEqual(t, `"\xff"`, String("\xff"))
	assertWriteHRSEqual(t, `"💩"`, String("\xf0\x9f\x92\xa9"))
	assertWriteHRSEqual(t, `"💩"`, String("💩"))
	assertWriteHRSEqual(t, `"\a"`, String("\007"))
	assertWriteHRSEqual(t, `"☺"`, String("\u263a"))
}

func TestWriteHumanReadableRef(t *testing.T) {
	vs := newTestValueStore()

	x := Float(42)
	rv, err := vs.WriteValue(context.Background(), x)
	require.NoError(t, err)
	assertWriteHRSEqual(t, "#0123456789abcdefghijklmnopqrstuv", rv)
}

func TestWriteHumanReadableCollections(t *testing.T) {
	vrw := newTestValueStore()

	l, err := NewList(context.Background(), vrw, Float(0), Float(1), Float(2), Float(3))
	require.NoError(t, err)
	assertWriteHRSEqual(t, "[  // 4 items\n  0,\n  1,\n  2,\n  3,\n]", l)

	s, err := NewSet(context.Background(), vrw, Float(0), Float(1), Float(2), Float(3))
	require.NoError(t, err)
	assertWriteHRSEqual(t, "set {  // 4 items\n  0,\n  1,\n  2,\n  3,\n}", s)

	m, err := NewMap(context.Background(), vrw, Float(0), Bool(false), Float(1), Bool(true))
	require.NoError(t, err)
	assertWriteHRSEqual(t, "map {\n  0: false,\n  1: true,\n}", m)

	l2, err := NewList(context.Background(), vrw)
	require.NoError(t, err)
	assertWriteHRSEqual(t, "[]", l2)

	l3, err := NewList(context.Background(), vrw, Float(0))
	require.NoError(t, err)
	assertWriteHRSEqual(t, "[\n  0,\n]", l3)

	nums := make([]Value, 2000)
	for i := range nums {
		nums[i] = Float(0)
	}
	l4, err := NewList(context.Background(), vrw, nums...)
	require.NoError(t, err)
	assertWriteHRSEqual(t, "[  // 2,000 items\n"+strings.Repeat("  0,\n", 2000)+"]", l4)
}

func TestWriteHumanReadableNested(t *testing.T) {
	vrw := newTestValueStore()

	l, err := NewList(context.Background(), vrw, Float(0), Float(1))
	require.NoError(t, err)
	l2, err := NewList(context.Background(), vrw, Float(2), Float(3))
	require.NoError(t, err)

	s, err := NewSet(context.Background(), vrw, String("a"), String("b"))
	require.NoError(t, err)
	s2, err := NewSet(context.Background(), vrw, String("c"), String("d"))
	require.NoError(t, err)

	m, err := NewMap(context.Background(), vrw, s, l, s2, l2)
	require.NoError(t, err)
	assertWriteHRSEqual(t, `map {
  set {
    "c",
    "d",
  }: [
    2,
    3,
  ],
  set {
    "a",
    "b",
  }: [
    0,
    1,
  ],
}`, m)
}

func TestWriteHumanReadableStruct(t *testing.T) {
	vrw := newTestValueStore()
	str := mustValue(NewStruct(vrw.Format(), "S1", StructData{
		"x": Float(1),
		"y": Float(2),
	}))
	assertWriteHRSEqual(t, "struct S1 {\n  x: 1,\n  y: 2,\n}", str)
}

func TestWriteHumanReadableListOfStruct(t *testing.T) {
	vrw := newTestValueStore()

	str1 := mustValue(NewStruct(vrw.Format(), "S3", StructData{
		"x": Float(1),
	}))
	str2 := mustValue(NewStruct(vrw.Format(), "S3", StructData{
		"x": Float(2),
	}))
	str3 := mustValue(NewStruct(vrw.Format(), "S3", StructData{
		"x": Float(3),
	}))
	l := mustValue(NewList(context.Background(), vrw, str1, str2, str3))
	assertWriteHRSEqual(t, `[
  struct S3 {
    x: 1,
  },
  struct S3 {
    x: 2,
  },
  struct S3 {
    x: 3,
  },
]`, l)
}

func TestWriteHumanReadableBlob(t *testing.T) {
	vrw := newTestValueStore()
	assertWriteHRSEqual(t, "blob {}", mustValue(NewEmptyBlob(vrw)))

	b1, err := NewBlob(context.Background(), vrw, bytes.NewBuffer([]byte{0x01}))
	require.NoError(t, err)
	assertWriteHRSEqual(t, "blob {01}", b1)

	b2, err := NewBlob(context.Background(), vrw, bytes.NewBuffer([]byte{0x01, 0x02}))
	require.NoError(t, err)
	assertWriteHRSEqual(t, "blob {01 02}", b2)

	b3, err := NewBlob(context.Background(), vrw, bytes.NewBuffer([]byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	}))
	require.NoError(t, err)
	assertWriteHRSEqual(t, "blob {00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f}", b3)

	b4, err := NewBlob(context.Background(), vrw, bytes.NewBuffer([]byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x10,
	}))
	require.NoError(t, err)
	assertWriteHRSEqual(t, "blob {  // 17 B\n  00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f\n  10\n}", b4)

	bs := make([]byte, 256)
	for i := range bs {
		bs[i] = byte(i)
	}

	b5, err := NewBlob(context.Background(), vrw, bytes.NewBuffer(bs))
	require.NoError(t, err)
	assertWriteHRSEqual(t, "blob {  // 256 B\n  00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f\n  10 11 12 13 14 15 16 17 18 19 1a 1b 1c 1d 1e 1f\n  20 21 22 23 24 25 26 27 28 29 2a 2b 2c 2d 2e 2f\n  30 31 32 33 34 35 36 37 38 39 3a 3b 3c 3d 3e 3f\n  40 41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f\n  50 51 52 53 54 55 56 57 58 59 5a 5b 5c 5d 5e 5f\n  60 61 62 63 64 65 66 67 68 69 6a 6b 6c 6d 6e 6f\n  70 71 72 73 74 75 76 77 78 79 7a 7b 7c 7d 7e 7f\n  80 81 82 83 84 85 86 87 88 89 8a 8b 8c 8d 8e 8f\n  90 91 92 93 94 95 96 97 98 99 9a 9b 9c 9d 9e 9f\n  a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 aa ab ac ad ae af\n  b0 b1 b2 b3 b4 b5 b6 b7 b8 b9 ba bb bc bd be bf\n  c0 c1 c2 c3 c4 c5 c6 c7 c8 c9 ca cb cc cd ce cf\n  d0 d1 d2 d3 d4 d5 d6 d7 d8 d9 da db dc dd de df\n  e0 e1 e2 e3 e4 e5 e6 e7 e8 e9 ea eb ec ed ee ef\n  f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 fa fb fc fd fe ff\n}", b5)

	b6, err := NewBlob(context.Background(), vrw, bytes.NewBuffer(make([]byte, 16*100)))
	require.NoError(t, err)
	row := "  " + strings.Repeat("00 ", 15) + "00\n"
	s := strings.Repeat(row, 100)
	assertWriteHRSEqual(t, "blob {  // 1.6 kB\n"+s+"}", b6)
}

func TestWriteHumanReadableListOfBlob(t *testing.T) {
	vrw := newTestValueStore()

	b1, err := NewBlob(context.Background(), vrw, bytes.NewBuffer([]byte{0x01}))
	require.NoError(t, err)
	b2, err := NewBlob(context.Background(), vrw, bytes.NewBuffer([]byte{0x02}))
	require.NoError(t, err)
	b3, err := NewBlob(context.Background(), vrw, bytes.NewBuffer([]byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x10,
	}))
	require.NoError(t, err)
	l, err := NewList(context.Background(), vrw, b1, mustValue(NewEmptyBlob(vrw)), b2, b3)
	require.NoError(t, err)
	assertWriteHRSEqual(t, "[  // 4 items\n  blob {01},\n  blob {},\n  blob {02},\n  blob {  // 17 B\n    00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f\n    10\n  },\n]", l)
}

func TestWriteHumanReadableType(t *testing.T) {
	assertWriteHRSEqual(t, "Bool", PrimitiveTypeMap[BoolKind])
	assertWriteHRSEqual(t, "Blob", PrimitiveTypeMap[BlobKind])
	assertWriteHRSEqual(t, "String", PrimitiveTypeMap[StringKind])
	assertWriteHRSEqual(t, "Float", PrimitiveTypeMap[FloatKind])
	assertWriteHRSEqual(t, "UUID", PrimitiveTypeMap[UUIDKind])
	assertWriteHRSEqual(t, "Int", PrimitiveTypeMap[IntKind])
	assertWriteHRSEqual(t, "Uint", PrimitiveTypeMap[UintKind])
	assertWriteHRSEqual(t, "InlineBlob", PrimitiveTypeMap[InlineBlobKind])
	assertWriteHRSEqual(t, "Null", PrimitiveTypeMap[NullKind])

	assertWriteHRSEqual(t, "List<Float>", mustType(MakeListType(PrimitiveTypeMap[FloatKind])))
	assertWriteHRSEqual(t, "Set<Float>", mustType(MakeSetType(PrimitiveTypeMap[FloatKind])))
	assertWriteHRSEqual(t, "Ref<Float>", mustType(MakeRefType(PrimitiveTypeMap[FloatKind])))
	assertWriteHRSEqual(t, "Map<Float, String>", mustType(MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])))
	assertWriteHRSEqual(t, "Float | String", mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])))
	assertWriteHRSEqual(t, "Bool", mustType(MakeUnionType(PrimitiveTypeMap[BoolKind])))
	assertWriteHRSEqual(t, "Union<>", mustType(MakeUnionType()))
	assertWriteHRSEqual(t, "List<Float | String>", mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])))))
	assertWriteHRSEqual(t, "List<Int | Uint>", mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[IntKind], PrimitiveTypeMap[UintKind])))))
	assertWriteHRSEqual(t, "List<Int | Null>", mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[IntKind], PrimitiveTypeMap[NullKind])))))
	assertWriteHRSEqual(t, "List<Union<>>", mustType(MakeListType(mustType(MakeUnionType()))))
}

func TestRecursiveStruct(t *testing.T) {
	// struct A {
	//   b: A
	//   c: List<A>
	//   d: struct D {
	//     e: D
	//     f: A
	//   }
	// }

	a := mustType(MakeStructType("A",
		StructField{"b", MakeCycleType("A"), false},
		StructField{"c", mustType(MakeListType(MakeCycleType("A"))), false},
		StructField{"d", mustType(MakeStructType("D",
			StructField{"e", MakeCycleType("D"), false},
			StructField{"f", MakeCycleType("A"), false},
		)), false},
	))

	assertWriteHRSEqual(t, `Struct A {
  b: Cycle<A>,
  c: List<Cycle<A>>,
  d: Struct D {
    e: Cycle<D>,
    f: Cycle<A>,
  },
}`, a)

	d, _ := a.Desc.(StructDesc).Field("d")

	assertWriteHRSEqual(t, `Struct D {
  e: Cycle<D>,
  f: Struct A {
    b: Cycle<A>,
    c: List<Cycle<A>>,
    d: Cycle<D>,
  },
}`, d)
}

func TestUnresolvedRecursiveStruct(t *testing.T) {
	// struct A {
	//   a: A
	//   b: Cycle<1> (unresolved)
	// }
	a := mustType(MakeStructType("A",
		StructField{"a", MakeCycleType("A"), false},
		StructField{"b", MakeCycleType("X"), false},
	))

	assertWriteHRSEqual(t, `Struct A {
  a: Cycle<A>,
  b: UnresolvedCycle<X>,
}`, a)
}

type errorWriter struct {
	err error
}

func (w *errorWriter) Write(p []byte) (int, error) {
	return 0, w.err
}

func TestWriteHumanReadableWriterError(t *testing.T) {
	assert := assert.New(t)
	err := errors.New("test")
	w := &errorWriter{err}
	assert.Equal(err, WriteEncodedValue(context.Background(), w, Float(42)))
}

func TestEmptyCollections(t *testing.T) {
	vrw := newTestValueStore()

	a, err := MakeStructType("Nothing")
	require.NoError(t, err)
	assertWriteHRSEqual(t, "Struct Nothing {}", a)
	b, err := NewStruct(vrw.Format(), "Rien", StructData{})
	require.NoError(t, err)
	assertWriteHRSEqual(t, "struct Rien {}", b)
	c, err := MakeMapType(PrimitiveTypeMap[BlobKind], PrimitiveTypeMap[FloatKind])
	require.NoError(t, err)
	assertWriteHRSEqual(t, "Map<Blob, Float>", c)
	d, err := NewMap(context.Background(), vrw)
	require.NoError(t, err)
	assertWriteHRSEqual(t, "map {}", d)
	e, err := MakeSetType(PrimitiveTypeMap[StringKind])
	require.NoError(t, err)
	assertWriteHRSEqual(t, "Set<String>", e)
	f, err := NewSet(context.Background(), vrw)
	require.NoError(t, err)
	assertWriteHRSEqual(t, "set {}", f)
}

func TestEncodedValueMaxLines(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	l1, err := NewList(context.Background(), vrw, generateNumbersAsValues(vrw.Format(), 11)...)
	require.NoError(t, err)
	expected := strings.Join(strings.SplitAfterN(mustString(EncodedValue(context.Background(), l1)), "\n", 6)[:5], "")
	actual, err := EncodedValueMaxLines(context.Background(), l1, 5)
	assert.True(err == writers.MaxLinesErr)
	assert.Equal(expected, actual)

	buf := bytes.Buffer{}
	err = WriteEncodedValueMaxLines(context.Background(), &buf, l1, 5)
	assert.True(err == writers.MaxLinesErr)
	assert.Equal(expected, buf.String())
}

func TestWriteHumanReadableStructOptionalFields(t *testing.T) {
	typ, err := MakeStructType("S1",
		StructField{"a", PrimitiveTypeMap[BoolKind], false},
		StructField{"b", PrimitiveTypeMap[BoolKind], true})
	require.NoError(t, err)
	assertWriteHRSEqual(t, "Struct S1 {\n  a: Bool,\n  b?: Bool,\n}", typ)
}

type TestCommenter struct {
	prefix   string
	testType *Type
}

func (c TestCommenter) Comment(ctx context.Context, v Value) string {
	if !(mustType(v.typeOf()).Equals(c.testType)) {
		return ""
	}

	v, _, err := v.(Struct).MaybeGet("Name")
	d.PanicIfError(err)

	return c.prefix + string(v.(String))
}

func TestRegisterCommenter(t *testing.T) {
	a := assert.New(t)
	vrw := newTestValueStore()

	tt, err := NewStruct(vrw.Format(), "TestType1", StructData{"Name": String("abc-123")})
	a.NoError(err)
	nt, err := NewStruct(vrw.Format(), "TestType2", StructData{"Name": String("abc-123")})
	a.NoError(err)

	RegisterHRSCommenter("TestType1", "mylib1", TestCommenter{prefix: "MyTest: ", testType: mustType(tt.typeOf())})

	s1, err := EncodedValue(context.Background(), tt)
	a.NoError(err)
	a.True(strings.Contains(s1, "// MyTest: abc-123"))
	s1, err = EncodedValue(context.Background(), nt)
	a.NoError(err)
	a.False(strings.Contains(s1, "// MyTest: abc-123"))

	RegisterHRSCommenter("TestType1", "mylib1", TestCommenter{prefix: "MyTest2: ", testType: mustType(tt.typeOf())})
	s1, err = EncodedValue(context.Background(), tt)
	a.NoError(err)
	a.True(strings.Contains(s1, "// MyTest2: abc-123"))

	UnregisterHRSCommenter("TestType1", "mylib1")
	s1, err = EncodedValue(context.Background(), tt)
	a.NoError(err)
	a.False(strings.Contains(s1, "// MyTest2: abc-123"))
}
