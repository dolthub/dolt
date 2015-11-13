package types

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func TestRead(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := []interface{}{float64(1), "hi", true}
	r := newJsonArrayReader(a, cs)

	assert.Equal(float64(1), r.read().(float64))
	assert.False(r.atEnd())

	assert.Equal("hi", r.readString())
	assert.False(r.atEnd())

	assert.Equal(true, r.readBool())
	assert.True(r.atEnd())
}

func parseJson(s string, vs ...interface{}) (v []interface{}) {
	dec := json.NewDecoder(strings.NewReader(fmt.Sprintf(s, vs...)))
	dec.Decode(&v)
	return
}

func TestReadTypeAsTag(t *testing.T) {
	cs := chunks.NewMemoryStore()

	test := func(expected Type, s string, vs ...interface{}) {
		a := parseJson(s, vs...)
		r := newJsonArrayReader(a, cs)
		tr := r.readTypeAsTag()
		assert.True(t, expected.Equals(tr))
	}

	test(MakePrimitiveType(BoolKind), "[%d, true]", BoolKind)
	test(MakePrimitiveType(TypeKind), "[%d, %d]", TypeKind, BoolKind)
	test(MakeCompoundType(ListKind, MakePrimitiveType(BoolKind)), "[%d, %d, true, false]", ListKind, BoolKind)

	pkgRef := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	test(MakeType(pkgRef, 42), `[%d, "%s", 42]`, UnresolvedKind, pkgRef.String())

	test(MakePrimitiveType(TypeKind), `[%d, %d, "%s", 12]`, TypeKind, TypeKind, pkgRef.String())
}

func TestReadPrimitives(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	test := func(expected Value, s string, vs ...interface{}) {
		a := parseJson(s, vs...)
		r := newJsonArrayReader(a, cs)
		v := r.readTopLevelValue()
		assert.True(expected.Equals(v))
	}

	test(Bool(true), "[%d, true]", BoolKind)
	test(Bool(false), "[%d, false]", BoolKind)

	test(UInt8(0), "[%d, 0]", UInt8Kind)
	test(UInt16(0), "[%d, 0]", UInt16Kind)
	test(UInt32(0), "[%d, 0]", UInt32Kind)
	test(UInt64(0), "[%d, 0]", UInt64Kind)
	test(Int8(0), "[%d, 0]", Int8Kind)
	test(Int16(0), "[%d, 0]", Int16Kind)
	test(Int32(0), "[%d, 0]", Int32Kind)
	test(Int64(0), "[%d, 0]", Int64Kind)
	test(Float32(0), "[%d, 0]", Float32Kind)
	test(Float64(0), "[%d, 0]", Float64Kind)

	test(NewString("hi"), `[%d, "hi"]`, StringKind)

	blob := NewMemoryBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	test(blob, `[%d, "AAE="]`, BlobKind)
}

func TestReadListOfInt32(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson("[%d, %d, [0, 1, 2, 3]]", ListKind, Int32Kind)
	r := newJsonArrayReader(a, cs)

	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int32Kind))

	l := r.readTopLevelValue()
	l2 := NewList(Int32(0), Int32(1), Int32(2), Int32(3))
	l2.t = tr
	assert.True(l2.Equals(l))
}

func TestReadListOfValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson(`[%d, %d, [%d, 1, %d, "hi", %d, true]]`, ListKind, ValueKind, Int32Kind, StringKind, BoolKind)
	r := newJsonArrayReader(a, cs)
	l := r.readTopLevelValue()
	assert.True(NewList(Int32(1), NewString("hi"), Bool(true)).Equals(l))
}

func TestReadValueListOfInt8(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson(`[%d, %d, %d, [0, 1, 2]]`, ValueKind, ListKind, Int8Kind)
	r := newJsonArrayReader(a, cs)

	tr := MakeCompoundType(ListKind, MakePrimitiveType(Int8Kind))

	l := r.readTopLevelValue()
	l2 := NewList(Int8(0), Int8(1), Int8(2))
	l2.t = tr
	assert.True(l2.Equals(l))
}

func TestReadMapOfInt64ToFloat64(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson("[%d, %d, %d, [0, 1, 2, 3]]", MapKind, Int64Kind, Float64Kind)
	r := newJsonArrayReader(a, cs)

	tr := MakeCompoundType(MapKind, MakePrimitiveType(Int64Kind), MakePrimitiveType(Float64Kind))

	m := r.readTopLevelValue()
	m2 := NewMap(Int64(0), Float64(1), Int64(2), Float64(3))
	m2.t = tr
	assert.True(m2.Equals(m))
}

func TestReadValueMapOfUInt64ToUInt32(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson("[%d, %d, %d, %d, [0, 1, 2, 3]]", ValueKind, MapKind, UInt64Kind, UInt32Kind)
	r := newJsonArrayReader(a, cs)

	mapTr := MakeCompoundType(MapKind, MakePrimitiveType(UInt64Kind), MakePrimitiveType(UInt32Kind))

	m := r.readTopLevelValue()
	m2 := NewMap(UInt64(0), UInt32(1), UInt64(2), UInt32(3))
	m2.t = mapTr
	assert.True(m2.Equals(m))
}

func TestReadSetOfUInt8(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson("[%d, %d, [0, 3, 1, 2]]", SetKind, UInt8Kind)
	r := newJsonArrayReader(a, cs)

	tr := MakeCompoundType(SetKind, MakePrimitiveType(UInt8Kind))

	s := r.readTopLevelValue()
	s2 := NewSet(UInt8(0), UInt8(1), UInt8(2), UInt8(3))
	s2.t = tr
	assert.True(s2.Equals(s))
}

func TestReadValueSetOfUInt16(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	a := parseJson("[%d, %d, %d, [3, 0, 1, 2]]", ValueKind, SetKind, UInt16Kind)
	r := newJsonArrayReader(a, cs)

	setTr := MakeCompoundType(SetKind, MakePrimitiveType(UInt16Kind))

	s := r.readTopLevelValue()
	s2 := NewSet(UInt16(0), UInt16(1), UInt16(2), UInt16(3))
	s2.t = setTr
	assert.True(s2.Equals(s))
}

func TestReadCompoundBlob(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	r1 := ref.Parse("sha1-0000000000000000000000000000000000000001")
	r2 := ref.Parse("sha1-0000000000000000000000000000000000000002")
	r3 := ref.Parse("sha1-0000000000000000000000000000000000000003")
	a := parseJson(`[%d, %d, ["%s", 20, "%s", 40, "%s", 60]]`, MetaSequenceKind, BlobKind, r1, r2, r3)
	r := newJsonArrayReader(a, cs)

	m := r.readTopLevelValue()
	_, ok := m.(compoundBlob)
	assert.True(ok)
	m2 := newCompoundBlob([]metaTuple{{r1, UInt64(20)}, {r2, UInt64(40)}, {r3, UInt64(60)}}, cs)

	assert.True(m.Type().Equals(m2.Type()))
	assert.Equal(m.Ref().String(), m2.Ref().String())
}

func TestReadStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typ := MakeStructType("A1", []Field{
		Field{"x", MakePrimitiveType(Int16Kind), false},
		Field{"s", MakePrimitiveType(StringKind), false},
		Field{"b", MakePrimitiveType(BoolKind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJson(`[%d, "%s", 0, 42, "hi", true]`, UnresolvedKind, pkgRef.String())
	r := newJsonArrayReader(a, cs)

	v := r.readTopLevelValue().(Struct)
	assert.True(v.Get("x").Equals(Int16(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))
	assert.True(v.Get("b").Equals(Bool(true)))
}

func TestReadStructUnion(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typ := MakeStructType("A2", []Field{
		Field{"x", MakePrimitiveType(Float32Kind), false},
	}, Choices{
		Field{"b", MakePrimitiveType(BoolKind), false},
		Field{"s", MakePrimitiveType(StringKind), false},
	})
	pkg := NewPackage([]Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJson(`[%d, "%s", 0, 42, 1, "hi"]`, UnresolvedKind, pkgRef.String())
	r := newJsonArrayReader(a, cs)

	v := r.readTopLevelValue().(Struct)
	assert.True(v.Get("x").Equals(Float32(42)))
	assert.Equal(uint32(1), v.UnionIndex())
	assert.True(v.UnionValue().Equals(NewString("hi")))

	x, ok := v.MaybeGet("x")
	assert.True(ok)
	assert.True(x.Equals(Float32(42)))

	s, ok := v.MaybeGet("s")
	assert.True(ok)
	assert.True(s.Equals(NewString("hi")))
	assert.True(v.UnionValue().Equals(s))
}

func TestReadStructOptional(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typ := MakeStructType("A3", []Field{
		Field{"x", MakePrimitiveType(Float32Kind), false},
		Field{"s", MakePrimitiveType(StringKind), true},
		Field{"b", MakePrimitiveType(BoolKind), true},
	}, Choices{})
	pkg := NewPackage([]Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJson(`[%d, "%s", 0, 42, false, true, false]`, UnresolvedKind, pkgRef.String())
	r := newJsonArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("x").Equals(Float32(42)))
	_, ok := v.MaybeGet("s")
	assert.False(ok)
	assert.Panics(func() { v.Get("s") })
	b, ok := v.MaybeGet("b")
	assert.True(ok)
	assert.True(b.Equals(Bool(false)))
}

func TestReadStructWithList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	// struct A4 {
	//   b: Bool
	//   l: List(Int32)
	//   s: String
	// }

	typ := MakeStructType("A4", []Field{
		Field{"b", MakePrimitiveType(BoolKind), false},
		Field{"l", MakeCompoundType(ListKind, MakePrimitiveType(Int32Kind)), false},
		Field{"s", MakePrimitiveType(StringKind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJson(`[%d, "%s", 0, true, [0, 1, 2], "hi"]`, UnresolvedKind, pkgRef.String())
	r := newJsonArrayReader(a, cs)
	l32Tr := MakeCompoundType(ListKind, MakePrimitiveType(Int32Kind))
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("b").Equals(Bool(true)))
	l := NewList(Int32(0), Int32(1), Int32(2))
	l.t = l32Tr
	assert.True(v.Get("l").Equals(l))
	assert.True(v.Get("s").Equals(NewString("hi")))
}

func TestReadStructWithValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	// struct A5 {
	//   b: Bool
	//   v: Value
	//   s: String
	// }

	typ := MakeStructType("A5", []Field{
		Field{"b", MakePrimitiveType(BoolKind), false},
		Field{"v", MakePrimitiveType(ValueKind), false},
		Field{"s", MakePrimitiveType(StringKind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJson(`[%d, "%s", 0, true, %d, 42, "hi"]`, UnresolvedKind, pkgRef.String(), UInt8Kind)
	r := newJsonArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("b").Equals(Bool(true)))
	assert.True(v.Get("v").Equals(UInt8(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))
}

func TestReadValueStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	// struct A1 {
	//   x: Float32
	//   b: Bool
	//   s: String
	// }

	typ := MakeStructType("A1", []Field{
		Field{"x", MakePrimitiveType(Int16Kind), false},
		Field{"s", MakePrimitiveType(StringKind), false},
		Field{"b", MakePrimitiveType(BoolKind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJson(`[%d, %d, "%s", 0, 42, "hi", true]`, ValueKind, UnresolvedKind, pkgRef.String())
	r := newJsonArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("x").Equals(Int16(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))
	assert.True(v.Get("b").Equals(Bool(true)))
}

func TestReadEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeEnumType("E", "a", "b", "c")
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJson(`[%d, "%s", 0, 1]`, UnresolvedKind, pkgRef.String())
	r := newJsonArrayReader(a, cs)

	v := r.readTopLevelValue().(Enum)
	assert.Equal(uint32(1), v.v)
}

func TestReadValueEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeEnumType("E", "a", "b", "c")
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJson(`[%d, %d, "%s", 0, 1]`, ValueKind, UnresolvedKind, pkgRef.String())
	r := newJsonArrayReader(a, cs)

	v := r.readTopLevelValue().(Enum)
	assert.Equal(uint32(1), v.v)
}

func TestReadRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	r := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	a := parseJson(`[%d, %d, "%s"]`, RefKind, UInt32Kind, r.String())
	reader := newJsonArrayReader(a, cs)
	v := reader.readTopLevelValue()
	assert.True(NewRef(r).Equals(v))
}

func TestReadValueRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	r := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	a := parseJson(`[%d, %d, %d, "%s"]`, ValueKind, RefKind, UInt32Kind, r.String())
	reader := newJsonArrayReader(a, cs)
	v := reader.readTopLevelValue()
	assert.True(NewRef(r).Equals(v))
}

func TestReadStructWithEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	// enum E {
	//   a
	//   b
	// }
	// struct A1 {
	//   x: Float32
	//   e: E
	//   s: String
	// }

	structTref := MakeStructType("A1", []Field{
		Field{"x", MakePrimitiveType(Int16Kind), false},
		Field{"e", MakeType(ref.Ref{}, 1), false},
		Field{"b", MakePrimitiveType(BoolKind), false},
	}, Choices{})
	enumTref := MakeEnumType("E", "a", "b", "c")
	pkg := NewPackage([]Type{structTref, enumTref}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJson(`[%d, "%s", 0, 42, 1, true]`, UnresolvedKind, pkgRef.String())
	r := newJsonArrayReader(a, cs)
	enumTr := MakeType(pkgRef, 1)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("x").Equals(Int16(42)))
	assert.True(v.Get("e").Equals(Enum{1, enumTr}))
	assert.True(v.Get("b").Equals(Bool(true)))
}

func TestReadStructWithBlob(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	// struct A5 {
	//   b: Blob
	// }

	typ := MakeStructType("A5", []Field{
		Field{"b", MakePrimitiveType(BlobKind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJson(`[%d, "%s", 0, "AAE="]`, UnresolvedKind, pkgRef.String())
	r := newJsonArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	blob := NewMemoryBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	assert.True(v.Get("b").Equals(blob))
}

func TestReadTypeValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	test := func(expected Type, json string, vs ...interface{}) {
		a := parseJson(json, vs...)
		r := newJsonArrayReader(a, cs)
		tr := r.readTopLevelValue()
		assert.True(expected.Equals(tr))
	}

	test(MakePrimitiveType(Int32Kind),
		`[%d, %d]`, TypeKind, Int32Kind)
	test(MakeCompoundType(ListKind, MakePrimitiveType(BoolKind)),
		`[%d, %d, [%d]]`, TypeKind, ListKind, BoolKind)
	test(MakeCompoundType(MapKind, MakePrimitiveType(BoolKind), MakePrimitiveType(StringKind)),
		`[%d, %d, [%d, %d]]`, TypeKind, MapKind, BoolKind, StringKind)
	test(MakeEnumType("E", "a", "b", "c"),
		`[%d, %d, "E", ["a", "b", "c"]]`, TypeKind, EnumKind)

	test(MakeStructType("S", []Field{
		Field{"x", MakePrimitiveType(Int16Kind), false},
		Field{"v", MakePrimitiveType(ValueKind), true},
	}, Choices{}),
		`[%d, %d, "S", ["x", %d, false, "v", %d, true], []]`, TypeKind, StructKind, Int16Kind, ValueKind)

	test(MakeStructType("S", []Field{}, Choices{
		Field{"x", MakePrimitiveType(Int16Kind), false},
		Field{"v", MakePrimitiveType(ValueKind), false},
	}),
		`[%d, %d, "S", [], ["x", %d, false, "v", %d, false]]`, TypeKind, StructKind, Int16Kind, ValueKind)

	pkgRef := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	test(MakeType(pkgRef, 123), `[%d, %d, "%s", 123]`, TypeKind, UnresolvedKind, pkgRef.String())

	test(MakeStructType("S", []Field{
		Field{"e", MakeType(pkgRef, 123), false},
		Field{"x", MakePrimitiveType(Int64Kind), false},
	}, Choices{}),
		`[%d, %d, "S", ["e", %d, "%s", 123, false, "x", %d, false], []]`, TypeKind, StructKind, UnresolvedKind, pkgRef.String(), Int64Kind)

	test(MakeUnresolvedType("ns", "n"), `[%d, %d, "%s", -1, "ns", "n"]`, TypeKind, UnresolvedKind, ref.Ref{}.String())
}

func TestReadPackage(t *testing.T) {
	cs := chunks.NewMemoryStore()
	pkg := NewPackage([]Type{
		MakeStructType("EnumStruct",
			[]Field{
				Field{"hand", MakeType(ref.Ref{}, 1), false},
			},
			Choices{},
		),
		MakeEnumType("Handedness", "right", "left", "switch"),
	}, []ref.Ref{})

	// struct Package {
	// 	Dependencies: Set(Ref(Package))
	// 	Types: List(Type)
	// }

	a := []interface{}{
		float64(PackageKind),
		[]interface{}{ // Types
			float64(StructKind), "EnumStruct", []interface{}{
				"hand", float64(UnresolvedKind), "sha1-0000000000000000000000000000000000000000", float64(1), false,
			}, []interface{}{},
			float64(EnumKind), "Handedness", []interface{}{"right", "left", "switch"},
		},
		[]interface{}{}, // Dependencies
	}
	r := newJsonArrayReader(a, cs)
	pkg2 := r.readTopLevelValue().(Package)
	assert.True(t, pkg.Equals(pkg2))
}

func TestReadPackage2(t *testing.T) {
	cs := chunks.NewMemoryStore()

	rr := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	setTref := MakeCompoundType(SetKind, MakePrimitiveType(UInt32Kind))
	pkg := NewPackage([]Type{setTref}, []ref.Ref{rr})

	a := []interface{}{float64(PackageKind), []interface{}{float64(SetKind), []interface{}{float64(UInt32Kind)}}, []interface{}{rr.String()}}
	r := newJsonArrayReader(a, cs)
	v := r.readTopLevelValue().(Package)
	assert.True(t, pkg.Equals(v))
}

func TestReadPackageThroughChunkSource(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]Type{
		MakeStructType("S", []Field{
			Field{"X", MakePrimitiveType(Int32Kind), false},
		}, Choices{}),
	}, []ref.Ref{})
	// Don't register
	pkgRef := WriteValue(pkg, cs)

	a := parseJson(`[%d, "%s", 0, 42]`, UnresolvedKind, pkgRef.String())
	r := newJsonArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("X").Equals(Int32(42)))
}
