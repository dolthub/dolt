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

	"github.com/liquidata-inc/ld/dolt/go/store/util/test"
	"github.com/stretchr/testify/assert"
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
	rv := vs.WriteValue(context.Background(), x)
	assertWriteHRSEqual(t, "#0123456789abcdefghijklmnopqrstuv", rv)
}

func TestWriteHumanReadableCollections(t *testing.T) {
	vrw := newTestValueStore()

	// TODO(binformat)
	l := NewList(context.Background(), vrw, Float(0), Float(1), Float(2), Float(3))
	assertWriteHRSEqual(t, "[  // 4 items\n  0,\n  1,\n  2,\n  3,\n]", l)

	s := NewSet(context.Background(), Format_7_18, vrw, Float(0), Float(1), Float(2), Float(3))
	assertWriteHRSEqual(t, "set {  // 4 items\n  0,\n  1,\n  2,\n  3,\n}", s)

	m := NewMap(context.Background(), vrw, Float(0), Bool(false), Float(1), Bool(true))
	assertWriteHRSEqual(t, "map {\n  0: false,\n  1: true,\n}", m)

	// TODO(binformat)
	l2 := NewList(context.Background(), vrw)
	assertWriteHRSEqual(t, "[]", l2)

	// TODO(binformat)
	l3 := NewList(context.Background(), vrw, Float(0))
	assertWriteHRSEqual(t, "[\n  0,\n]", l3)

	nums := make([]Value, 2000)
	for i := range nums {
		nums[i] = Float(0)
	}
	// TODO(binformat)
	l4 := NewList(context.Background(), vrw, nums...)
	assertWriteHRSEqual(t, "[  // 2,000 items\n"+strings.Repeat("  0,\n", 2000)+"]", l4)
}

func TestWriteHumanReadableNested(t *testing.T) {
	vrw := newTestValueStore()

	// TODO(binformat)
	l := NewList(context.Background(), vrw, Float(0), Float(1))
	l2 := NewList(context.Background(), vrw, Float(2), Float(3))

	s := NewSet(context.Background(), Format_7_18, vrw, String("a"), String("b"))
	s2 := NewSet(context.Background(), Format_7_18, vrw, String("c"), String("d"))

	m := NewMap(context.Background(), vrw, s, l, s2, l2)
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
	str := NewStruct(Format_7_18, "S1", StructData{
		"x": Float(1),
		"y": Float(2),
	})
	assertWriteHRSEqual(t, "struct S1 {\n  x: 1,\n  y: 2,\n}", str)
}

func TestWriteHumanReadableListOfStruct(t *testing.T) {
	vrw := newTestValueStore()

	str1 := NewStruct(Format_7_18, "S3", StructData{
		"x": Float(1),
	})
	str2 := NewStruct(Format_7_18, "S3", StructData{
		"x": Float(2),
	})
	str3 := NewStruct(Format_7_18, "S3", StructData{
		"x": Float(3),
	})
	l := NewList(context.Background(), vrw, str1, str2, str3)
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
	// TODO(binformat)
	assertWriteHRSEqual(t, "blob {}", NewEmptyBlob(vrw, Format_7_18))

	// TODO(binformat)
	b1 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBuffer([]byte{0x01}))
	assertWriteHRSEqual(t, "blob {01}", b1)

	// TODO(binformat)
	b2 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBuffer([]byte{0x01, 0x02}))
	assertWriteHRSEqual(t, "blob {01 02}", b2)

	// TODO(binformat)
	b3 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBuffer([]byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	}))
	assertWriteHRSEqual(t, "blob {00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f}", b3)

	// TODO(binformat)
	b4 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBuffer([]byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x10,
	}))
	assertWriteHRSEqual(t, "blob {  // 17 B\n  00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f\n  10\n}", b4)

	bs := make([]byte, 256)
	for i := range bs {
		bs[i] = byte(i)
	}

	// TODO(binformat)
	b5 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBuffer(bs))
	assertWriteHRSEqual(t, "blob {  // 256 B\n  00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f\n  10 11 12 13 14 15 16 17 18 19 1a 1b 1c 1d 1e 1f\n  20 21 22 23 24 25 26 27 28 29 2a 2b 2c 2d 2e 2f\n  30 31 32 33 34 35 36 37 38 39 3a 3b 3c 3d 3e 3f\n  40 41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f\n  50 51 52 53 54 55 56 57 58 59 5a 5b 5c 5d 5e 5f\n  60 61 62 63 64 65 66 67 68 69 6a 6b 6c 6d 6e 6f\n  70 71 72 73 74 75 76 77 78 79 7a 7b 7c 7d 7e 7f\n  80 81 82 83 84 85 86 87 88 89 8a 8b 8c 8d 8e 8f\n  90 91 92 93 94 95 96 97 98 99 9a 9b 9c 9d 9e 9f\n  a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 aa ab ac ad ae af\n  b0 b1 b2 b3 b4 b5 b6 b7 b8 b9 ba bb bc bd be bf\n  c0 c1 c2 c3 c4 c5 c6 c7 c8 c9 ca cb cc cd ce cf\n  d0 d1 d2 d3 d4 d5 d6 d7 d8 d9 da db dc dd de df\n  e0 e1 e2 e3 e4 e5 e6 e7 e8 e9 ea eb ec ed ee ef\n  f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 fa fb fc fd fe ff\n}", b5)

	// TODO(binformat)
	b6 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBuffer(make([]byte, 16*100)))
	row := "  " + strings.Repeat("00 ", 15) + "00\n"
	s := strings.Repeat(row, 100)
	assertWriteHRSEqual(t, "blob {  // 1.6 kB\n"+s+"}", b6)
}

func TestWriteHumanReadableListOfBlob(t *testing.T) {
	vrw := newTestValueStore()

	// TODO(binformat)
	b1 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBuffer([]byte{0x01}))
	b2 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBuffer([]byte{0x02}))
	b3 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBuffer([]byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x10,
	}))
	// TODO(binformat)
	l := NewList(context.Background(), vrw, b1, NewEmptyBlob(vrw, Format_7_18), b2, b3)
	assertWriteHRSEqual(t, "[  // 4 items\n  blob {01},\n  blob {},\n  blob {02},\n  blob {  // 17 B\n    00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f\n    10\n  },\n]", l)
}

func TestWriteHumanReadableType(t *testing.T) {
	assertWriteHRSEqual(t, "Bool", BoolType)
	assertWriteHRSEqual(t, "Blob", BlobType)
	assertWriteHRSEqual(t, "String", StringType)
	assertWriteHRSEqual(t, "Float", FloaTType)
	assertWriteHRSEqual(t, "UUID", UUIDType)
	assertWriteHRSEqual(t, "Int", IntType)
	assertWriteHRSEqual(t, "Uint", UintType)
	assertWriteHRSEqual(t, "Null", NullType)

	assertWriteHRSEqual(t, "List<Float>", MakeListType(FloaTType))
	assertWriteHRSEqual(t, "Set<Float>", MakeSetType(FloaTType))
	assertWriteHRSEqual(t, "Ref<Float>", MakeRefType(FloaTType))
	assertWriteHRSEqual(t, "Map<Float, String>", MakeMapType(FloaTType, StringType))
	assertWriteHRSEqual(t, "Float | String", MakeUnionType(FloaTType, StringType))
	assertWriteHRSEqual(t, "Bool", MakeUnionType(BoolType))
	assertWriteHRSEqual(t, "", MakeUnionType())
	assertWriteHRSEqual(t, "List<Float | String>", MakeListType(MakeUnionType(FloaTType, StringType)))
	assertWriteHRSEqual(t, "List<Int | Uint>", MakeListType(MakeUnionType(IntType, UintType)))
	assertWriteHRSEqual(t, "List<Int | Null>", MakeListType(MakeUnionType(IntType, NullType)))
	assertWriteHRSEqual(t, "List<>", MakeListType(MakeUnionType()))
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

	a := MakeStructType("A",
		StructField{"b", MakeCycleType("A"), false},
		StructField{"c", MakeListType(MakeCycleType("A")), false},
		StructField{"d", MakeStructType("D",
			StructField{"e", MakeCycleType("D"), false},
			StructField{"f", MakeCycleType("A"), false},
		), false},
	)

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
	a := MakeStructType("A",
		StructField{"a", MakeCycleType("A"), false},
		StructField{"b", MakeCycleType("X"), false},
	)

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
	assert.Equal(err, WriteEncodedValue(context.Background(), Format_7_18, w, Float(42)))
}

func TestEmptyCollections(t *testing.T) {
	vrw := newTestValueStore()

	a := MakeStructType("Nothing")
	assertWriteHRSEqual(t, "Struct Nothing {}", a)
	b := NewStruct(Format_7_18, "Rien", StructData{})
	assertWriteHRSEqual(t, "struct Rien {}", b)
	c := MakeMapType(BlobType, FloaTType)
	assertWriteHRSEqual(t, "Map<Blob, Float>", c)
	d := NewMap(context.Background(), vrw)
	assertWriteHRSEqual(t, "map {}", d)
	e := MakeSetType(StringType)
	assertWriteHRSEqual(t, "Set<String>", e)
	f := NewSet(context.Background(), Format_7_18, vrw)
	assertWriteHRSEqual(t, "set {}", f)
}

func TestEncodedValueMaxLines(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	// TODO(binformat)
	l1 := NewList(context.Background(), vrw, generateNumbersAsValues(11)...)
	expected := strings.Join(strings.SplitAfterN(EncodedValue(context.Background(), Format_7_18, l1), "\n", 6)[:5], "")
	assert.Equal(expected, EncodedValueMaxLines(context.Background(), Format_7_18, l1, 5))

	buf := bytes.Buffer{}
	WriteEncodedValueMaxLines(context.Background(), Format_7_18, &buf, l1, 5)
	assert.Equal(expected, buf.String())
}

func TestWriteHumanReadableStructOptionalFields(t *testing.T) {
	typ := MakeStructType("S1",
		StructField{"a", BoolType, false},
		StructField{"b", BoolType, true})
	assertWriteHRSEqual(t, "Struct S1 {\n  a: Bool,\n  b?: Bool,\n}", typ)
}

type TestCommenter struct {
	prefix   string
	testType *Type
}

func (c TestCommenter) Comment(ctx context.Context, f *Format, v Value) string {
	if !(v.typeOf().Equals(f, c.testType)) {
		return ""
	}
	return c.prefix + string(v.(Struct).Get("Name").(String))
}

func TestRegisterCommenter(t *testing.T) {
	a := assert.New(t)

	tt := NewStruct(Format_7_18, "TestType1", StructData{"Name": String("abc-123")})
	nt := NewStruct(Format_7_18, "TestType2", StructData{"Name": String("abc-123")})

	RegisterHRSCommenter("TestType1", "mylib1", TestCommenter{prefix: "MyTest: ", testType: tt.typeOf()})

	s1 := EncodedValue(context.Background(), Format_7_18, tt)
	a.True(strings.Contains(s1, "// MyTest: abc-123"))
	s1 = EncodedValue(context.Background(), Format_7_18, nt)
	a.False(strings.Contains(s1, "// MyTest: abc-123"))

	RegisterHRSCommenter("TestType1", "mylib1", TestCommenter{prefix: "MyTest2: ", testType: tt.typeOf()})
	s1 = EncodedValue(context.Background(), Format_7_18, tt)
	a.True(strings.Contains(s1, "// MyTest2: abc-123"))

	UnregisterHRSCommenter("TestType1", "mylib1")
	s1 = EncodedValue(context.Background(), Format_7_18, tt)
	a.False(strings.Contains(s1, "// MyTest2: abc-123"))
}
