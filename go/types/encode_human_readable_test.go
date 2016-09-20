// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func assertWriteHRSEqual(t *testing.T, expected string, v Value) {
	assert := assert.New(t)
	var buf bytes.Buffer
	w := &hrsWriter{w: &buf, floatFormat: 'g'}
	w.Write(v)
	assert.Equal(expected, buf.String())
}

func assertWriteTaggedHRSEqual(t *testing.T, expected string, v Value) {
	assert := assert.New(t)
	var buf bytes.Buffer
	w := &hrsWriter{w: &buf, floatFormat: 'g'}
	w.WriteTagged(v)
	assert.Equal(expected, buf.String())
}

func TestWriteHumanReadablePrimitiveValues(t *testing.T) {
	assertWriteHRSEqual(t, "true", Bool(true))
	assertWriteHRSEqual(t, "false", Bool(false))

	assertWriteHRSEqual(t, "0", Number(0))
	assertWriteHRSEqual(t, "42", Number(42))

	assertWriteHRSEqual(t, "-42", Number(-42))

	assertWriteHRSEqual(t, "3.1415926535", Number(3.1415926535))
	assertWriteHRSEqual(t, "314159.26535", Number(3.1415926535e5))
	assertWriteHRSEqual(t, "3.1415926535e+20", Number(3.1415926535e20))

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
	vs := NewTestValueStore()

	x := Number(42)
	rv := vs.WriteValue(x)
	assertWriteHRSEqual(t, "b828k24s0s43lf9q70l302o6p6k7rfak", rv)
	assertWriteTaggedHRSEqual(t, "Ref<Number>(b828k24s0s43lf9q70l302o6p6k7rfak)", rv)
}

func TestWriteHumanReadableCollections(t *testing.T) {
	l := NewList(Number(0), Number(1), Number(2), Number(3))
	assertWriteHRSEqual(t, "[  // 4 items\n  0,\n  1,\n  2,\n  3,\n]", l)
	assertWriteTaggedHRSEqual(t, "List<Number>([  // 4 items\n  0,\n  1,\n  2,\n  3,\n])", l)

	s := NewSet(Number(0), Number(1), Number(2), Number(3))
	assertWriteHRSEqual(t, "{  // 4 items\n  0,\n  1,\n  2,\n  3,\n}", s)
	assertWriteTaggedHRSEqual(t, "Set<Number>({  // 4 items\n  0,\n  1,\n  2,\n  3,\n})", s)

	m := NewMap(Number(0), Bool(false), Number(1), Bool(true))
	assertWriteHRSEqual(t, "{\n  0: false,\n  1: true,\n}", m)
	assertWriteTaggedHRSEqual(t, "Map<Number, Bool>({\n  0: false,\n  1: true,\n})", m)

	l2 := NewList()
	assertWriteHRSEqual(t, "[]", l2)
	assertWriteTaggedHRSEqual(t, "List<>([])", l2)

	l3 := NewList(Number(0))
	assertWriteHRSEqual(t, "[\n  0,\n]", l3)
	assertWriteTaggedHRSEqual(t, "List<Number>([\n  0,\n])", l3)

	nums := make([]Value, 2000)
	for i := range nums {
		nums[i] = Number(0)
	}
	l4 := NewList(nums...)
	assertWriteTaggedHRSEqual(t, "List<Number>([  // 2,000 items\n"+strings.Repeat("  0,\n", 2000)+"])", l4)
}

func TestWriteHumanReadableNested(t *testing.T) {
	l := NewList(Number(0), Number(1))
	l2 := NewList(Number(2), Number(3))

	s := NewSet(String("a"), String("b"))
	s2 := NewSet(String("c"), String("d"))

	m := NewMap(s, l, s2, l2)
	assertWriteHRSEqual(t, `{
  {
    "c",
    "d",
  }: [
    2,
    3,
  ],
  {
    "a",
    "b",
  }: [
    0,
    1,
  ],
}`, m)
	assertWriteTaggedHRSEqual(t, `Map<Set<String>, List<Number>>({
  {
    "c",
    "d",
  }: [
    2,
    3,
  ],
  {
    "a",
    "b",
  }: [
    0,
    1,
  ],
})`, m)
}

func TestWriteHumanReadableStruct(t *testing.T) {
	str := NewStruct("S1", StructData{
		"x": Number(1),
		"y": Number(2),
	})
	assertWriteHRSEqual(t, "S1 {\n  x: 1,\n  y: 2,\n}", str)
	assertWriteTaggedHRSEqual(t, "struct S1 {\n  x: Number,\n  y: Number,\n}({\n  x: 1,\n  y: 2,\n})", str)
}

func TestWriteHumanReadableListOfStruct(t *testing.T) {
	str1 := NewStruct("S3", StructData{
		"x": Number(1),
	})
	str2 := NewStruct("S3", StructData{
		"x": Number(2),
	})
	str3 := NewStruct("S3", StructData{
		"x": Number(3),
	})
	l := NewList(str1, str2, str3)
	assertWriteHRSEqual(t, `[
  S3 {
    x: 1,
  },
  S3 {
    x: 2,
  },
  S3 {
    x: 3,
  },
]`, l)
	assertWriteTaggedHRSEqual(t, `List<struct S3 {
  x: Number,
}>([
  S3 {
    x: 1,
  },
  S3 {
    x: 2,
  },
  S3 {
    x: 3,
  },
])`, l)
}

func TestWriteHumanReadableBlob(t *testing.T) {
	assertWriteHRSEqual(t, "", NewEmptyBlob())
	assertWriteTaggedHRSEqual(t, "Blob()", NewEmptyBlob())

	b1 := NewBlob(bytes.NewBuffer([]byte{0x01}))
	assertWriteHRSEqual(t, "01", b1)
	assertWriteTaggedHRSEqual(t, "Blob(01)", b1)

	b2 := NewBlob(bytes.NewBuffer([]byte{0x01, 0x02}))
	assertWriteHRSEqual(t, "01 02", b2)
	assertWriteTaggedHRSEqual(t, "Blob(01 02)", b2)

	b3 := NewBlob(bytes.NewBuffer([]byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	}))
	assertWriteHRSEqual(t, "00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f", b3)
	assertWriteTaggedHRSEqual(t, "Blob(00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f)", b3)

	b4 := NewBlob(bytes.NewBuffer([]byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x10,
	}))
	assertWriteHRSEqual(t, "00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f  // 17 B\n10", b4)
	assertWriteTaggedHRSEqual(t, "Blob(00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f  // 17 B\n10)", b4)

	bs := make([]byte, 256)
	for i := range bs {
		bs[i] = byte(i)
	}

	b5 := NewBlob(bytes.NewBuffer(bs))
	assertWriteHRSEqual(t, "00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f  // 256 B\n10 11 12 13 14 15 16 17 18 19 1a 1b 1c 1d 1e 1f\n20 21 22 23 24 25 26 27 28 29 2a 2b 2c 2d 2e 2f\n30 31 32 33 34 35 36 37 38 39 3a 3b 3c 3d 3e 3f\n40 41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f\n50 51 52 53 54 55 56 57 58 59 5a 5b 5c 5d 5e 5f\n60 61 62 63 64 65 66 67 68 69 6a 6b 6c 6d 6e 6f\n70 71 72 73 74 75 76 77 78 79 7a 7b 7c 7d 7e 7f\n80 81 82 83 84 85 86 87 88 89 8a 8b 8c 8d 8e 8f\n90 91 92 93 94 95 96 97 98 99 9a 9b 9c 9d 9e 9f\na0 a1 a2 a3 a4 a5 a6 a7 a8 a9 aa ab ac ad ae af\nb0 b1 b2 b3 b4 b5 b6 b7 b8 b9 ba bb bc bd be bf\nc0 c1 c2 c3 c4 c5 c6 c7 c8 c9 ca cb cc cd ce cf\nd0 d1 d2 d3 d4 d5 d6 d7 d8 d9 da db dc dd de df\ne0 e1 e2 e3 e4 e5 e6 e7 e8 e9 ea eb ec ed ee ef\nf0 f1 f2 f3 f4 f5 f6 f7 f8 f9 fa fb fc fd fe ff", b5)
	assertWriteTaggedHRSEqual(t, "Blob(00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f  // 256 B\n10 11 12 13 14 15 16 17 18 19 1a 1b 1c 1d 1e 1f\n20 21 22 23 24 25 26 27 28 29 2a 2b 2c 2d 2e 2f\n30 31 32 33 34 35 36 37 38 39 3a 3b 3c 3d 3e 3f\n40 41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f\n50 51 52 53 54 55 56 57 58 59 5a 5b 5c 5d 5e 5f\n60 61 62 63 64 65 66 67 68 69 6a 6b 6c 6d 6e 6f\n70 71 72 73 74 75 76 77 78 79 7a 7b 7c 7d 7e 7f\n80 81 82 83 84 85 86 87 88 89 8a 8b 8c 8d 8e 8f\n90 91 92 93 94 95 96 97 98 99 9a 9b 9c 9d 9e 9f\na0 a1 a2 a3 a4 a5 a6 a7 a8 a9 aa ab ac ad ae af\nb0 b1 b2 b3 b4 b5 b6 b7 b8 b9 ba bb bc bd be bf\nc0 c1 c2 c3 c4 c5 c6 c7 c8 c9 ca cb cc cd ce cf\nd0 d1 d2 d3 d4 d5 d6 d7 d8 d9 da db dc dd de df\ne0 e1 e2 e3 e4 e5 e6 e7 e8 e9 ea eb ec ed ee ef\nf0 f1 f2 f3 f4 f5 f6 f7 f8 f9 fa fb fc fd fe ff)", b5)

	b6 := NewBlob(bytes.NewBuffer(make([]byte, 16*100)))
	row := strings.Repeat("00 ", 15) + "00"
	s := strings.Repeat(row+"\n", 98) + row
	assertWriteTaggedHRSEqual(t, "Blob("+row+"  // 1.6 kB\n"+s+")", b6)
}

func TestWriteHumanReadableListOfBlob(t *testing.T) {
	b1 := NewBlob(bytes.NewBuffer([]byte{0x01}))
	b2 := NewBlob(bytes.NewBuffer([]byte{0x02}))
	b3 := NewBlob(bytes.NewBuffer([]byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x10,
	}))
	l := NewList(b1, NewEmptyBlob(), b2, b3)
	assertWriteHRSEqual(t, "[  // 4 items\n  01,\n  ,\n  02,\n  00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f  // 17 B\n  10,\n]", l)
	assertWriteTaggedHRSEqual(t, "List<Blob>([  // 4 items\n  01,\n  ,\n  02,\n  00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f  // 17 B\n  10,\n])", l)
}

func TestWriteHumanReadableType(t *testing.T) {
	assertWriteHRSEqual(t, "Bool", BoolType)
	assertWriteHRSEqual(t, "Blob", BlobType)
	assertWriteHRSEqual(t, "String", StringType)
	assertWriteHRSEqual(t, "Number", NumberType)

	assertWriteHRSEqual(t, "List<Number>", MakeListType(NumberType))
	assertWriteHRSEqual(t, "Set<Number>", MakeSetType(NumberType))
	assertWriteHRSEqual(t, "Ref<Number>", MakeRefType(NumberType))
	assertWriteHRSEqual(t, "Map<Number, String>", MakeMapType(NumberType, StringType))
	assertWriteHRSEqual(t, "Number | String", MakeUnionType(NumberType, StringType))
	assertWriteHRSEqual(t, "Bool", MakeUnionType(BoolType))
	assertWriteHRSEqual(t, "", MakeUnionType())
	assertWriteHRSEqual(t, "List<Number | String>", MakeListType(MakeUnionType(NumberType, StringType)))
	assertWriteHRSEqual(t, "List<>", MakeListType(MakeUnionType()))
}

func TestWriteHumanReadableTaggedPrimitiveValues(t *testing.T) {
	assertWriteHRSEqual(t, "true", Bool(true))
	assertWriteHRSEqual(t, "false", Bool(false))

	assertWriteTaggedHRSEqual(t, "0", Number(0))
	assertWriteTaggedHRSEqual(t, "42", Number(42))
	assertWriteTaggedHRSEqual(t, "-42", Number(-42))

	assertWriteTaggedHRSEqual(t, "3.1415926535", Number(3.1415926535))

	assertWriteTaggedHRSEqual(t, "314159.26535", Number(3.1415926535e5))

	assertWriteTaggedHRSEqual(t, "3.1415926535e+20", Number(3.1415926535e20))

	assertWriteTaggedHRSEqual(t, `"abc"`, String("abc"))
	assertWriteTaggedHRSEqual(t, `" "`, String(" "))
	assertWriteTaggedHRSEqual(t, `"\t"`, String("\t"))
	assertWriteTaggedHRSEqual(t, `"\t"`, String("	"))
	assertWriteTaggedHRSEqual(t, `"\n"`, String("\n"))
	assertWriteTaggedHRSEqual(t, `"\n"`, String(`
`))
	assertWriteTaggedHRSEqual(t, `"\r"`, String("\r"))
	assertWriteTaggedHRSEqual(t, `"\r\n"`, String("\r\n"))
	assertWriteTaggedHRSEqual(t, `"\xff"`, String("\xff"))
	assertWriteTaggedHRSEqual(t, `"💩"`, String("\xf0\x9f\x92\xa9"))
	assertWriteTaggedHRSEqual(t, `"💩"`, String("💩"))
	assertWriteTaggedHRSEqual(t, `"\a"`, String("\007"))
	assertWriteTaggedHRSEqual(t, `"☺"`, String("\u263a"))
}

func TestWriteHumanReadableTaggedType(t *testing.T) {
	assertWriteTaggedHRSEqual(t, "Type(Bool)", BoolType)
	assertWriteTaggedHRSEqual(t, "Type(Blob)", BlobType)
	assertWriteTaggedHRSEqual(t, "Type(String)", StringType)
	assertWriteTaggedHRSEqual(t, "Type(Number)", NumberType)
	assertWriteTaggedHRSEqual(t, "Type(List<Number>)", MakeListType(NumberType))
	assertWriteTaggedHRSEqual(t, "Type(Set<Number>)", MakeSetType(NumberType))
	assertWriteTaggedHRSEqual(t, "Type(Ref<Number>)", MakeRefType(NumberType))
	assertWriteTaggedHRSEqual(t, "Type(Map<Number, String>)", MakeMapType(NumberType, StringType))

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
		[]string{"b", "c", "d"},
		[]*Type{
			MakeCycleType(0),
			MakeListType(MakeCycleType(0)),
			MakeStructType("D",
				[]string{"e", "f"},
				[]*Type{
					MakeCycleType(0),
					MakeCycleType(1),
				},
			),
		})

	assertWriteHRSEqual(t, `struct A {
  b: Cycle<0>,
  c: List<Cycle<0>>,
  d: struct D {
    e: Cycle<0>,
    f: Cycle<1>,
  },
}`, a)
	assertWriteTaggedHRSEqual(t, `Type(struct A {
  b: Cycle<0>,
  c: List<Cycle<0>>,
  d: struct D {
    e: Cycle<0>,
    f: Cycle<1>,
  },
})`, a)

	f, _ := a.Desc.(StructDesc).findField("d")
	d := f.t

	assertWriteHRSEqual(t, `struct D {
  e: Cycle<0>,
  f: struct A {
    b: Cycle<0>,
    c: List<Cycle<0>>,
    d: Cycle<1>,
  },
}`, d)
	assertWriteTaggedHRSEqual(t, `Type(struct D {
  e: Cycle<0>,
  f: struct A {
    b: Cycle<0>,
    c: List<Cycle<0>>,
    d: Cycle<1>,
  },
})`, d)
}

func TestUnresolvedRecursiveStruct(t *testing.T) {
	// struct A {
	//   a: A
	//   b: Cycle<1> (unresolved)
	// }

	a := MakeStructType("A",
		[]string{"a", "b"},
		[]*Type{
			MakeCycleType(0),
			MakeCycleType(1),
		})

	assertWriteHRSEqual(t, `struct A {
  a: Cycle<0>,
  b: UnresolvedCycle<1>,
}`, a)
	assertWriteTaggedHRSEqual(t, `Type(struct A {
  a: Cycle<0>,
  b: UnresolvedCycle<1>,
})`, a)
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
	assert.Equal(err, WriteEncodedValueWithTags(w, Number(42)))
}

func TestEmptyCollections(t *testing.T) {
	a := MakeStructType("Nothing", []string{}, []*Type{})
	assertWriteTaggedHRSEqual(t, "Type(struct Nothing {})", a)
	b := NewStruct("Rien", StructData{})
	assertWriteTaggedHRSEqual(t, "struct Rien {}({})", b)
	c := MakeMapType(BlobType, NumberType)
	assertWriteTaggedHRSEqual(t, "Type(Map<Blob, Number>)", c)
	d := NewMap()
	assertWriteTaggedHRSEqual(t, "Map<>({})", d)
	e := MakeSetType(StringType)
	assertWriteTaggedHRSEqual(t, "Type(Set<String>)", e)
	f := NewSet()
	assertWriteTaggedHRSEqual(t, "Set<>({})", f)
}

func TestEncodedValueMaxLines(t *testing.T) {
	assert := assert.New(t)
	l1 := NewList(generateNumbersAsValues(11)...)
	expected := strings.Join(strings.SplitAfterN(EncodedValue(l1), "\n", 6)[:5], "")
	assert.Equal(expected, EncodedValueMaxLines(l1, 5))

	buf := bytes.Buffer{}
	WriteEncodedValueMaxLines(&buf, l1, 5)
	assert.Equal(expected, buf.String())
}
