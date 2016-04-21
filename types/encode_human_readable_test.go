package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/ref"
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

	assertWriteHRSEqual(t, "0", Uint8(0))
	assertWriteHRSEqual(t, "0", Uint16(0))
	assertWriteHRSEqual(t, "0", Uint32(0))
	assertWriteHRSEqual(t, "0", Uint64(0))
	assertWriteHRSEqual(t, "0", Int8(0))
	assertWriteHRSEqual(t, "0", Int16(0))
	assertWriteHRSEqual(t, "0", Int32(0))
	assertWriteHRSEqual(t, "0", Int64(0))
	assertWriteHRSEqual(t, "0", Float32(0))
	assertWriteHRSEqual(t, "0", Float64(0))

	assertWriteHRSEqual(t, "42", Uint8(42))
	assertWriteHRSEqual(t, "42", Uint16(42))
	assertWriteHRSEqual(t, "42", Uint32(42))
	assertWriteHRSEqual(t, "42", Uint64(42))
	assertWriteHRSEqual(t, "42", Int8(42))
	assertWriteHRSEqual(t, "42", Int16(42))
	assertWriteHRSEqual(t, "42", Int32(42))
	assertWriteHRSEqual(t, "42", Int64(42))
	assertWriteHRSEqual(t, "42", Float32(42))
	assertWriteHRSEqual(t, "42", Float64(42))

	assertWriteHRSEqual(t, "-42", Int8(-42))
	assertWriteHRSEqual(t, "-42", Int16(-42))
	assertWriteHRSEqual(t, "-42", Int32(-42))
	assertWriteHRSEqual(t, "-42", Int64(-42))
	assertWriteHRSEqual(t, "-42", Float32(-42))
	assertWriteHRSEqual(t, "-42", Float64(-42))

	assertWriteHRSEqual(t, "3.1415927", Float32(3.1415926535))
	assertWriteHRSEqual(t, "3.1415926535", Float64(3.1415926535))

	assertWriteHRSEqual(t, "314159.25", Float32(3.1415926535e5))
	assertWriteHRSEqual(t, "314159.26535", Float64(3.1415926535e5))

	assertWriteHRSEqual(t, "3.1415925e+20", Float32(3.1415926535e20))
	assertWriteHRSEqual(t, "3.1415926535e+20", Float64(3.1415926535e20))

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

	x := Int32(42)
	rv := vs.WriteValue(x)
	assertWriteHRSEqual(t, "sha1-c56efb6071a71743b826f2e10df26761549df9c2", rv)
	assertWriteTaggedHRSEqual(t, "Ref<Int32>(sha1-c56efb6071a71743b826f2e10df26761549df9c2)", rv)
}

func TestWriteHumanReadableCollections(t *testing.T) {
	lt := MakeListType(Float64Type)
	l := NewTypedList(lt, Float64(0), Float64(1), Float64(2), Float64(3))
	assertWriteHRSEqual(t, "[\n  0,\n  1,\n  2,\n  3,\n]", l)
	assertWriteTaggedHRSEqual(t, "List<Float64>([\n  0,\n  1,\n  2,\n  3,\n])", l)

	st := MakeSetType(Int8Type)
	s := NewTypedSet(st, Int8(0), Int8(1), Int8(2), Int8(3))
	assertWriteHRSEqual(t, "{\n  0,\n  1,\n  2,\n  3,\n}", s)
	assertWriteTaggedHRSEqual(t, "Set<Int8>({\n  0,\n  1,\n  2,\n  3,\n})", s)

	mt := MakeMapType(Int32Type, BoolType)
	m := NewTypedMap(mt, Int32(0), Bool(false), Int32(1), Bool(true))
	assertWriteHRSEqual(t, "{\n  0: false,\n  1: true,\n}", m)
	assertWriteTaggedHRSEqual(t, "Map<Int32, Bool>({\n  0: false,\n  1: true,\n})", m)
}

func TestWriteHumanReadableNested(t *testing.T) {
	lt := MakeListType(Float64Type)
	l := NewTypedList(lt, Float64(0), Float64(1))
	l2 := NewTypedList(lt, Float64(2), Float64(3))

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
	assertWriteTaggedHRSEqual(t, `Map<Set<String>, List<Float64>>({
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
	pkg := NewPackage([]Type{
		MakeStructType("S1", []Field{
			Field{Name: "x", T: Int32Type, Optional: false},
			Field{Name: "y", T: Int32Type, Optional: true},
		}, []Field{}),
	}, []ref.Ref{})
	typeDef := pkg.Types()[0]
	RegisterPackage(&pkg)
	typ := MakeType(pkg.Ref(), 0)

	str := NewStruct(typ, typeDef, map[string]Value{
		"x": Int32(1),
	})
	assertWriteHRSEqual(t, "S1 {\n  x: 1,\n}", str)
	assertWriteTaggedHRSEqual(t, "Struct<S1, sha1-bdd35d6fe5b89487d71d0ec27c1a6c79a0261baa, 0>({\n  x: 1,\n})", str)

	str2 := NewStruct(typ, typeDef, map[string]Value{
		"x": Int32(2),
		"y": Int32(3),
	})
	assertWriteHRSEqual(t, "S1 {\n  x: 2,\n  y: 3,\n}", str2)
	assertWriteTaggedHRSEqual(t, "Struct<S1, sha1-bdd35d6fe5b89487d71d0ec27c1a6c79a0261baa, 0>({\n  x: 2,\n  y: 3,\n})", str2)
}

func TestWriteHumanReadableStructWithUnion(t *testing.T) {
	pkg := NewPackage([]Type{
		MakeStructType("S2", []Field{}, []Field{
			Field{Name: "x", T: Int32Type, Optional: false},
			Field{Name: "y", T: Int32Type, Optional: false},
		}),
	}, []ref.Ref{})
	typeDef := pkg.Types()[0]
	RegisterPackage(&pkg)
	typ := MakeType(pkg.Ref(), 0)

	str := NewStruct(typ, typeDef, map[string]Value{
		"x": Int32(1),
	})
	assertWriteHRSEqual(t, "S2 {\n  x: 1,\n}", str)
	assertWriteTaggedHRSEqual(t, "Struct<S2, sha1-13e3f926c03c637bc474442a10af9023b24010f8, 0>({\n  x: 1,\n})", str)

	str2 := NewStruct(typ, typeDef, map[string]Value{
		"y": Int32(2),
	})
	assertWriteHRSEqual(t, "S2 {\n  y: 2,\n}", str2)
	assertWriteTaggedHRSEqual(t, "Struct<S2, sha1-13e3f926c03c637bc474442a10af9023b24010f8, 0>({\n  y: 2,\n})", str2)
}

func TestWriteHumanReadableListOfStruct(t *testing.T) {
	pkg := NewPackage([]Type{
		MakeStructType("S3", []Field{}, []Field{
			Field{Name: "x", T: Int32Type, Optional: false},
		}),
	}, []ref.Ref{})
	typeDef := pkg.Types()[0]
	RegisterPackage(&pkg)
	typ := MakeType(pkg.Ref(), 0)

	str1 := NewStruct(typ, typeDef, map[string]Value{
		"x": Int32(1),
	})
	str2 := NewStruct(typ, typeDef, map[string]Value{
		"x": Int32(2),
	})
	str3 := NewStruct(typ, typeDef, map[string]Value{
		"x": Int32(3),
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
	assertWriteTaggedHRSEqual(t, `List<Struct<S3, sha1-543f7124883ace7da7fccaed6d5cfc31598020f1, 0>>([
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

func TestWriteHumanReadableEnum(t *testing.T) {
	pkg := NewPackage([]Type{
		MakeEnumType("Color", "red", "green", "blue"),
	}, []ref.Ref{})
	RegisterPackage(&pkg)
	typ := MakeType(pkg.Ref(), 0)

	assertWriteHRSEqual(t, "red", newEnum(0, typ))
	assertWriteTaggedHRSEqual(t, "Enum<Color, sha1-51b66eaa0827d76d1618c8d4e7e42215d00d6642, 0>(red)", newEnum(0, typ))
	assertWriteHRSEqual(t, "green", newEnum(1, typ))
	assertWriteTaggedHRSEqual(t, "Enum<Color, sha1-51b66eaa0827d76d1618c8d4e7e42215d00d6642, 0>(green)", newEnum(1, typ))
	assertWriteHRSEqual(t, "blue", newEnum(2, typ))
	assertWriteTaggedHRSEqual(t, "Enum<Color, sha1-51b66eaa0827d76d1618c8d4e7e42215d00d6642, 0>(blue)", newEnum(2, typ))
}

func TestWriteHumanReadableBlob(t *testing.T) {
	assertWriteHRSEqual(t, "", NewEmptyBlob())
	assertWriteTaggedHRSEqual(t, "Blob()", NewEmptyBlob())

	b1 := NewBlob(bytes.NewBuffer([]byte{0x01}))
	assertWriteHRSEqual(t, "AQ", b1)
	assertWriteTaggedHRSEqual(t, "Blob(AQ)", b1)

	b2 := NewBlob(bytes.NewBuffer([]byte{0x01, 0x02}))
	assertWriteHRSEqual(t, "AQI", b2)
	assertWriteTaggedHRSEqual(t, "Blob(AQI)", b2)

	b3 := NewBlob(bytes.NewBuffer([]byte{0x01, 0x02, 0x03}))
	assertWriteHRSEqual(t, "AQID", b3)
	assertWriteTaggedHRSEqual(t, "Blob(AQID)", b3)

	bs := make([]byte, 256)
	for i := range bs {
		bs[i] = byte(i)
	}
	b4 := NewBlob(bytes.NewBuffer(bs))
	assertWriteHRSEqual(t, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w", b4)
	assertWriteTaggedHRSEqual(t, "Blob(AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w)", b4)
}

func TestWriteHumanReadableListOfBlob(t *testing.T) {
	lt := MakeListType(BlobType)
	b1 := NewBlob(bytes.NewBuffer([]byte{0x01}))
	b2 := NewBlob(bytes.NewBuffer([]byte{0x02}))
	l := NewTypedList(lt, b1, NewEmptyBlob(), b2)
	assertWriteHRSEqual(t, "[\n  AQ,\n  ,\n  Ag,\n]", l)
	assertWriteTaggedHRSEqual(t, "List<Blob>([\n  AQ,\n  ,\n  Ag,\n])", l)
}

func TestWriteHumanReadableListOfEnum(t *testing.T) {
	pkg := NewPackage([]Type{
		MakeEnumType("Color", "red", "green", "blue"),
	}, []ref.Ref{})
	RegisterPackage(&pkg)
	typ := MakeType(pkg.Ref(), 0)
	lt := MakeListType(typ)
	l := NewTypedList(lt, newEnum(0, typ), newEnum(1, typ), newEnum(2, typ))
	assertWriteHRSEqual(t, "[\n  red,\n  green,\n  blue,\n]", l)
	assertWriteTaggedHRSEqual(t, "List<Enum<Color, sha1-51b66eaa0827d76d1618c8d4e7e42215d00d6642, 0>>([\n  red,\n  green,\n  blue,\n])", l)
}

func TestWriteHumanReadableType(t *testing.T) {
	assertWriteHRSEqual(t, "Bool", BoolType)
	assertWriteHRSEqual(t, "Blob", BlobType)
	assertWriteHRSEqual(t, "String", StringType)

	assertWriteHRSEqual(t, "Int8", Int8Type)
	assertWriteHRSEqual(t, "Int16", Int16Type)
	assertWriteHRSEqual(t, "Int32", Int32Type)
	assertWriteHRSEqual(t, "Int64", Int64Type)
	assertWriteHRSEqual(t, "Uint8", Uint8Type)
	assertWriteHRSEqual(t, "Uint16", Uint16Type)
	assertWriteHRSEqual(t, "Uint32", Uint32Type)
	assertWriteHRSEqual(t, "Uint64", Uint64Type)
	assertWriteHRSEqual(t, "Float32", Float32Type)
	assertWriteHRSEqual(t, "Float64", Float64Type)

	assertWriteHRSEqual(t, "List<Int8>", MakeListType(Int8Type))
	assertWriteHRSEqual(t, "Set<Int16>", MakeSetType(Int16Type))
	assertWriteHRSEqual(t, "Ref<Int32>", MakeRefType(Int32Type))
	assertWriteHRSEqual(t, "Map<Int64, String>", MakeMapType(Int64Type, StringType))

	pkg := NewPackage([]Type{
		MakeEnumType("Color", "red", "green", "blue"),
		MakeStructType("Str", []Field{
			Field{Name: "c", T: MakeType(ref.Ref{}, 0), Optional: false},
			Field{Name: "o", T: StringType, Optional: true},
		}, []Field{
			Field{Name: "x", T: MakeType(ref.Ref{}, 1), Optional: false},
			Field{Name: "y", T: BoolType, Optional: false},
		}),
	}, []ref.Ref{})
	RegisterPackage(&pkg)
	et := MakeType(pkg.Ref(), 0)
	st := MakeType(pkg.Ref(), 1)

	assertWriteHRSEqual(t, "Enum<Color, sha1-9323c4c8d8a5745550b914fb01c8641ab42f121a, 0>", et)
	assertWriteTaggedHRSEqual(t, "Type(Enum<Color, sha1-9323c4c8d8a5745550b914fb01c8641ab42f121a, 0>)", et)
	assertWriteHRSEqual(t, "Struct<Str, sha1-9323c4c8d8a5745550b914fb01c8641ab42f121a, 1>", st)
	assertWriteTaggedHRSEqual(t, "Type(Struct<Str, sha1-9323c4c8d8a5745550b914fb01c8641ab42f121a, 1>)", st)

	eTypeDef := pkg.Types()[0]
	assertWriteHRSEqual(t, "enum Color {\n  red\n  green\n  blue\n}", eTypeDef)
	assertWriteTaggedHRSEqual(t, "Type(enum Color {\n  red\n  green\n  blue\n})", eTypeDef)

	sTypeDef := pkg.Types()[1]
	assertWriteHRSEqual(t, `struct Str {
  c: Enum<Color, sha1-9323c4c8d8a5745550b914fb01c8641ab42f121a, 0>
  o: optional String
  union {
    x: Struct<Str, sha1-9323c4c8d8a5745550b914fb01c8641ab42f121a, 1>
    y: Bool
  }
}`, sTypeDef)
	assertWriteTaggedHRSEqual(t, `Type(struct Str {
  c: Enum<Color, sha1-9323c4c8d8a5745550b914fb01c8641ab42f121a, 0>
  o: optional String
  union {
    x: Struct<Str, sha1-9323c4c8d8a5745550b914fb01c8641ab42f121a, 1>
    y: Bool
  }
})`, sTypeDef)
}

func TestWriteHumanReadableTaggedPrimitiveValues(t *testing.T) {
	assertWriteHRSEqual(t, "true", Bool(true))
	assertWriteHRSEqual(t, "false", Bool(false))

	assertWriteTaggedHRSEqual(t, "Uint8(0)", Uint8(0))
	assertWriteTaggedHRSEqual(t, "Uint16(0)", Uint16(0))
	assertWriteTaggedHRSEqual(t, "Uint32(0)", Uint32(0))
	assertWriteTaggedHRSEqual(t, "Uint64(0)", Uint64(0))
	assertWriteTaggedHRSEqual(t, "Int8(0)", Int8(0))
	assertWriteTaggedHRSEqual(t, "Int16(0)", Int16(0))
	assertWriteTaggedHRSEqual(t, "Int32(0)", Int32(0))
	assertWriteTaggedHRSEqual(t, "Int64(0)", Int64(0))
	assertWriteTaggedHRSEqual(t, "Float32(0)", Float32(0))
	assertWriteTaggedHRSEqual(t, "Float64(0)", Float64(0))

	assertWriteTaggedHRSEqual(t, "Uint8(42)", Uint8(42))
	assertWriteTaggedHRSEqual(t, "Uint16(42)", Uint16(42))
	assertWriteTaggedHRSEqual(t, "Uint32(42)", Uint32(42))
	assertWriteTaggedHRSEqual(t, "Uint64(42)", Uint64(42))
	assertWriteTaggedHRSEqual(t, "Int8(42)", Int8(42))
	assertWriteTaggedHRSEqual(t, "Int16(42)", Int16(42))
	assertWriteTaggedHRSEqual(t, "Int32(42)", Int32(42))
	assertWriteTaggedHRSEqual(t, "Int64(42)", Int64(42))
	assertWriteTaggedHRSEqual(t, "Float32(42)", Float32(42))
	assertWriteTaggedHRSEqual(t, "Float64(42)", Float64(42))

	assertWriteTaggedHRSEqual(t, "Int8(-42)", Int8(-42))
	assertWriteTaggedHRSEqual(t, "Int16(-42)", Int16(-42))
	assertWriteTaggedHRSEqual(t, "Int32(-42)", Int32(-42))
	assertWriteTaggedHRSEqual(t, "Int64(-42)", Int64(-42))
	assertWriteTaggedHRSEqual(t, "Float32(-42)", Float32(-42))
	assertWriteTaggedHRSEqual(t, "Float64(-42)", Float64(-42))

	assertWriteTaggedHRSEqual(t, "Float32(3.1415927)", Float32(3.1415926535))
	assertWriteTaggedHRSEqual(t, "Float64(3.1415926535)", Float64(3.1415926535))

	assertWriteTaggedHRSEqual(t, "Float32(314159.25)", Float32(3.1415926535e5))
	assertWriteTaggedHRSEqual(t, "Float64(314159.26535)", Float64(3.1415926535e5))

	assertWriteTaggedHRSEqual(t, "Float32(3.1415925e+20)", Float32(3.1415926535e20))
	assertWriteTaggedHRSEqual(t, "Float64(3.1415926535e+20)", Float64(3.1415926535e20))

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

	assertWriteTaggedHRSEqual(t, "Type(Int8)", Int8Type)
	assertWriteTaggedHRSEqual(t, "Type(Int16)", Int16Type)
	assertWriteTaggedHRSEqual(t, "Type(Int32)", Int32Type)
	assertWriteTaggedHRSEqual(t, "Type(Int64)", Int64Type)
	assertWriteTaggedHRSEqual(t, "Type(Uint8)", Uint8Type)
	assertWriteTaggedHRSEqual(t, "Type(Uint16)", Uint16Type)
	assertWriteTaggedHRSEqual(t, "Type(Uint32)", Uint32Type)
	assertWriteTaggedHRSEqual(t, "Type(Uint64)", Uint64Type)
	assertWriteTaggedHRSEqual(t, "Type(Float32)", Float32Type)
	assertWriteTaggedHRSEqual(t, "Type(Float64)", Float64Type)

	assertWriteTaggedHRSEqual(t, "Type(List<Int8>)", MakeListType(Int8Type))
	assertWriteTaggedHRSEqual(t, "Type(Set<Int16>)", MakeSetType(Int16Type))
	assertWriteTaggedHRSEqual(t, "Type(Ref<Int32>)", MakeRefType(Int32Type))
	assertWriteTaggedHRSEqual(t, "Type(Map<Int64, String>)", MakeMapType(Int64Type, StringType))

}
