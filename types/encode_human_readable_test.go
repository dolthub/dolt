package types

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func assertWriteHRSEqual(t *testing.T, expected string, v Value) {
	assert := assert.New(t)
	var buf bytes.Buffer
	w := &hrsWriter{w: &buf}
	w.Write(v)
	assert.Equal(expected, buf.String())
}

func assertWriteTaggedHRSEqual(t *testing.T, expected string, v Value) {
	assert := assert.New(t)
	var buf bytes.Buffer
	w := &hrsWriter{w: &buf}
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

	assertWriteHRSEqual(t, `"abc"`, NewString("abc"))
	assertWriteHRSEqual(t, `" "`, NewString(" "))
	assertWriteHRSEqual(t, `"\t"`, NewString("\t"))
	assertWriteHRSEqual(t, `"\t"`, NewString("	"))
	assertWriteHRSEqual(t, `"\n"`, NewString("\n"))
	assertWriteHRSEqual(t, `"\n"`, NewString(`
`))
	assertWriteHRSEqual(t, `"\r"`, NewString("\r"))
	assertWriteHRSEqual(t, `"\r\n"`, NewString("\r\n"))
	assertWriteHRSEqual(t, `"\xff"`, NewString("\xff"))
	assertWriteHRSEqual(t, `"💩"`, NewString("\xf0\x9f\x92\xa9"))
	assertWriteHRSEqual(t, `"💩"`, NewString("💩"))
	assertWriteHRSEqual(t, `"\a"`, NewString("\007"))
	assertWriteHRSEqual(t, `"☺"`, NewString("\u263a"))
}

func TestWriteHumanReadableRef(t *testing.T) {
	vs := NewTestValueStore()

	x := Number(42)
	rv := vs.WriteValue(x)
	assertWriteHRSEqual(t, "sha1-bd0b7d4cb11321762f4206f0d6c6fdf820f8556e", rv)
	assertWriteTaggedHRSEqual(t, "Ref<Number>(sha1-bd0b7d4cb11321762f4206f0d6c6fdf820f8556e)", rv)
}

func TestWriteHumanReadableCollections(t *testing.T) {
	lt := MakeListType(NumberType)
	l := NewTypedList(lt, Number(0), Number(1), Number(2), Number(3))
	assertWriteHRSEqual(t, "[\n  0,\n  1,\n  2,\n  3,\n]", l)
	assertWriteTaggedHRSEqual(t, "List<Number>([\n  0,\n  1,\n  2,\n  3,\n])", l)

	st := MakeSetType(NumberType)
	s := NewTypedSet(st, Number(0), Number(1), Number(2), Number(3))
	assertWriteHRSEqual(t, "{\n  0,\n  1,\n  2,\n  3,\n}", s)
	assertWriteTaggedHRSEqual(t, "Set<Number>({\n  0,\n  1,\n  2,\n  3,\n})", s)

	mt := MakeMapType(NumberType, BoolType)
	m := NewTypedMap(mt, Number(0), Bool(false), Number(1), Bool(true))
	assertWriteHRSEqual(t, "{\n  0: false,\n  1: true,\n}", m)
	assertWriteTaggedHRSEqual(t, "Map<Number, Bool>({\n  0: false,\n  1: true,\n})", m)
}

func TestWriteHumanReadableNested(t *testing.T) {
	lt := MakeListType(NumberType)
	l := NewTypedList(lt, Number(0), Number(1))
	l2 := NewTypedList(lt, Number(2), Number(3))

	st := MakeSetType(StringType)
	s := NewTypedSet(st, NewString("a"), NewString("b"))
	s2 := NewTypedSet(st, NewString("c"), NewString("d"))

	mt := MakeMapType(st, lt)
	m := NewTypedMap(mt, s, l, s2, l2)
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
	typ := MakeStructType("S1", TypeMap{
		"x": NumberType,
		"y": NumberType,
	})

	str := NewStruct(typ, map[string]Value{
		"x": Number(1),
		"y": Number(2),
	})
	assertWriteHRSEqual(t, "S1 {\n  x: 1,\n  y: 2,\n}", str)
	assertWriteTaggedHRSEqual(t, "struct S1 {\n  x: Number\n  y: Number\n}({\n  x: 1,\n  y: 2,\n})", str)
}

func TestWriteHumanReadableListOfStruct(t *testing.T) {
	typ := MakeStructType("S3", TypeMap{
		"x": NumberType,
	})

	str1 := NewStruct(typ, map[string]Value{
		"x": Number(1),
	})
	str2 := NewStruct(typ, map[string]Value{
		"x": Number(2),
	})
	str3 := NewStruct(typ, map[string]Value{
		"x": Number(3),
	})
	lt := MakeListType(typ)
	l := NewTypedList(lt, str1, str2, str3)
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
  x: Number
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
	assertWriteHRSEqual(t, "AQ==", b1)
	assertWriteTaggedHRSEqual(t, "Blob(AQ==)", b1)

	b2 := NewBlob(bytes.NewBuffer([]byte{0x01, 0x02}))
	assertWriteHRSEqual(t, "AQI=", b2)
	assertWriteTaggedHRSEqual(t, "Blob(AQI=)", b2)

	b3 := NewBlob(bytes.NewBuffer([]byte{0x01, 0x02, 0x03}))
	assertWriteHRSEqual(t, "AQID", b3)
	assertWriteTaggedHRSEqual(t, "Blob(AQID)", b3)

	bs := make([]byte, 256)
	for i := range bs {
		bs[i] = byte(i)
	}
	b4 := NewBlob(bytes.NewBuffer(bs))
	assertWriteHRSEqual(t, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w==", b4)
	assertWriteTaggedHRSEqual(t, "Blob(AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w==)", b4)
}

func TestWriteHumanReadableListOfBlob(t *testing.T) {
	lt := MakeListType(BlobType)
	b1 := NewBlob(bytes.NewBuffer([]byte{0x01}))
	b2 := NewBlob(bytes.NewBuffer([]byte{0x02}))
	l := NewTypedList(lt, b1, NewEmptyBlob(), b2)
	assertWriteHRSEqual(t, "[\n  AQ==,\n  ,\n  Ag==,\n]", l)
	assertWriteTaggedHRSEqual(t, "List<Blob>([\n  AQ==,\n  ,\n  Ag==,\n])", l)
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
}

func TestWriteHumanReadableTaggedPrimitiveValues(t *testing.T) {
	assertWriteHRSEqual(t, "true", Bool(true))
	assertWriteHRSEqual(t, "false", Bool(false))

	assertWriteTaggedHRSEqual(t, "Number(0)", Number(0))
	assertWriteTaggedHRSEqual(t, "Number(42)", Number(42))
	assertWriteTaggedHRSEqual(t, "Number(-42)", Number(-42))

	assertWriteTaggedHRSEqual(t, "Number(3.1415926535)", Number(3.1415926535))

	assertWriteTaggedHRSEqual(t, "Number(314159.26535)", Number(3.1415926535e5))

	assertWriteTaggedHRSEqual(t, "Number(3.1415926535e+20)", Number(3.1415926535e20))

	assertWriteTaggedHRSEqual(t, `"abc"`, NewString("abc"))
	assertWriteTaggedHRSEqual(t, `" "`, NewString(" "))
	assertWriteTaggedHRSEqual(t, `"\t"`, NewString("\t"))
	assertWriteTaggedHRSEqual(t, `"\t"`, NewString("	"))
	assertWriteTaggedHRSEqual(t, `"\n"`, NewString("\n"))
	assertWriteTaggedHRSEqual(t, `"\n"`, NewString(`
`))
	assertWriteTaggedHRSEqual(t, `"\r"`, NewString("\r"))
	assertWriteTaggedHRSEqual(t, `"\r\n"`, NewString("\r\n"))
	assertWriteTaggedHRSEqual(t, `"\xff"`, NewString("\xff"))
	assertWriteTaggedHRSEqual(t, `"💩"`, NewString("\xf0\x9f\x92\xa9"))
	assertWriteTaggedHRSEqual(t, `"💩"`, NewString("💩"))
	assertWriteTaggedHRSEqual(t, `"\a"`, NewString("\007"))
	assertWriteTaggedHRSEqual(t, `"☺"`, NewString("\u263a"))
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

	a := MakeStructType("A", TypeMap{
		"b": nil,
		"c": nil,
		"d": nil,
	})
	d := MakeStructType("D", TypeMap{
		"e": nil,
		"f": a,
	})
	a.Desc.(StructDesc).Fields["b"] = a
	a.Desc.(StructDesc).Fields["c"] = MakeListType(a)
	a.Desc.(StructDesc).Fields["d"] = d
	d.Desc.(StructDesc).Fields["e"] = d
	assertWriteHRSEqual(t, `struct A {
  b: Parent<0>
  c: List<Parent<0>>
  d: struct D {
    e: Parent<0>
    f: Parent<1>
  }
}`, a)
	assertWriteTaggedHRSEqual(t, `Type(struct A {
  b: Parent<0>
  c: List<Parent<0>>
  d: struct D {
    e: Parent<0>
    f: Parent<1>
  }
})`, a)

	assertWriteHRSEqual(t, `struct D {
  e: Parent<0>
  f: struct A {
    b: Parent<0>
    c: List<Parent<0>>
    d: Parent<1>
  }
}`, d)
	assertWriteTaggedHRSEqual(t, `Type(struct D {
  e: Parent<0>
  f: struct A {
    b: Parent<0>
    c: List<Parent<0>>
    d: Parent<1>
  }
})`, d)
}
