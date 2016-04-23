package types

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := []interface{}{float64(1), "hi", true}
	r := newJSONArrayReader(a, cs)

	assert.Equal(float64(1), r.read().(float64))
	assert.False(r.atEnd())

	assert.Equal("hi", r.readString())
	assert.False(r.atEnd())

	assert.Equal(true, r.readBool())
	assert.True(r.atEnd())
}

func parseJSON(s string, vs ...interface{}) (v []interface{}) {
	dec := json.NewDecoder(strings.NewReader(fmt.Sprintf(s, vs...)))
	dec.Decode(&v)
	return
}

func TestReadTypeAsTag(t *testing.T) {
	cs := NewTestValueStore()

	test := func(expected *Type, s string, vs ...interface{}) {
		a := parseJSON(s, vs...)
		r := newJSONArrayReader(a, cs)
		tr := r.readTypeAsTag()
		assert.True(t, expected.Equals(tr))
	}

	test(BoolType, "[%d, true]", BoolKind)
	test(TypeType, "[%d, %d]", TypeKind, BoolKind)
	test(MakeListType(BoolType), "[%d, %d, true, false]", ListKind, BoolKind)

	pkgRef := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	test(MakeType(pkgRef, 42), `[%d, "%s", "42"]`, UnresolvedKind, pkgRef.String())

	test(TypeType, `[%d, %d, "%s", "12"]`, TypeKind, TypeKind, pkgRef.String())
}

func TestReadPrimitives(t *testing.T) {
	assert := assert.New(t)

	cs := NewTestValueStore()

	test := func(expected Value, s string, vs ...interface{}) {
		a := parseJSON(s, vs...)
		r := newJSONArrayReader(a, cs)
		v := r.readTopLevelValue()
		assert.True(expected.Equals(v))
	}

	test(Bool(true), "[%d, true]", BoolKind)
	test(Bool(false), "[%d, false]", BoolKind)

	test(Uint8(0), `[%d, "0"]`, Uint8Kind)
	test(Uint16(0), `[%d, "0"]`, Uint16Kind)
	test(Uint32(0), `[%d, "0"]`, Uint32Kind)
	test(Uint64(0), `[%d, "0"]`, Uint64Kind)
	test(Int8(0), `[%d, "0"]`, Int8Kind)
	test(Int16(0), `[%d, "0"]`, Int16Kind)
	test(Int32(0), `[%d, "0"]`, Int32Kind)
	test(Int64(0), `[%d, "0"]`, Int64Kind)
	test(Float32(0), `[%d, "0"]`, Float32Kind)
	test(Float64(0), `[%d, "0"]`, Float64Kind)

	test(NewString("hi"), `[%d, "hi"]`, StringKind)

	blob := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	test(blob, `[%d, false, "AAE="]`, BlobKind)
}

func TestReadListOfInt32(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, false, ["0", "1", "2", "3"]]`, ListKind, Int32Kind)
	r := newJSONArrayReader(a, cs)

	tr := MakeListType(Int32Type)

	l := r.readTopLevelValue()
	l2 := NewTypedList(tr, Int32(0), Int32(1), Int32(2), Int32(3))
	assert.True(l2.Equals(l))
}

func TestReadListOfValue(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, false, [%d, "1", %d, "hi", %d, true]]`, ListKind, ValueKind, Int32Kind, StringKind, BoolKind)
	r := newJSONArrayReader(a, cs)
	l := r.readTopLevelValue()
	assert.True(NewList(Int32(1), NewString("hi"), Bool(true)).Equals(l))
}

func TestReadValueListOfInt8(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, %d, false, ["0", "1", "2"]]`, ValueKind, ListKind, Int8Kind)
	r := newJSONArrayReader(a, cs)

	tr := MakeListType(Int8Type)

	l := r.readTopLevelValue()
	l2 := NewTypedList(tr, Int8(0), Int8(1), Int8(2))
	assert.True(l2.Equals(l))
}

func TestReadCompoundList(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	tr := MakeListType(Int32Type)
	leaf1 := newListLeaf(tr, Int32(0))
	leaf2 := newListLeaf(tr, Int32(1), Int32(2), Int32(3))
	l2 := buildCompoundList([]metaTuple{
		newMetaTuple(Uint64(1), leaf1, Ref{}, 1),
		newMetaTuple(Uint64(4), leaf2, Ref{}, 4),
	}, tr, cs)

	a := parseJSON(`[%d, %d, true, ["%s", "1", "1", "%s", "4", "4"]]`, ListKind, Int32Kind, leaf1.Ref(), leaf2.Ref())
	r := newJSONArrayReader(a, cs)
	l := r.readTopLevelValue()

	assert.True(l2.Equals(l))
}

func TestReadCompoundSet(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	tr := MakeSetType(Int32Type)
	leaf1 := newSetLeaf(tr, Int32(0), Int32(1))
	leaf2 := newSetLeaf(tr, Int32(2), Int32(3), Int32(4))
	l2 := buildCompoundSet([]metaTuple{
		newMetaTuple(Int32(1), leaf1, Ref{}, 2),
		newMetaTuple(Int32(4), leaf2, Ref{}, 3),
	}, tr, cs)

	a := parseJSON(`[%d, %d, true, ["%s", "1", "2", "%s", "4", "3"]]`, SetKind, Int32Kind, leaf1.Ref(), leaf2.Ref())
	r := newJSONArrayReader(a, cs)
	l := r.readTopLevelValue()

	assert.True(l2.Equals(l))
}

func TestReadMapOfInt64ToFloat64(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, %d, false, ["0", "1", "2", "3"]]`, MapKind, Int64Kind, Float64Kind)
	r := newJSONArrayReader(a, cs)

	tr := MakeMapType(Int64Type, Float64Type)

	m := r.readTopLevelValue()
	m2 := NewTypedMap(tr, Int64(0), Float64(1), Int64(2), Float64(3))
	assert.True(m2.Equals(m))
}

func TestReadValueMapOfUint64ToUint32(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, %d, %d, false, ["0", "1", "2", "3"]]`, ValueKind, MapKind, Uint64Kind, Uint32Kind)
	r := newJSONArrayReader(a, cs)

	mapTr := MakeMapType(Uint64Type, Uint32Type)

	m := r.readTopLevelValue()
	m2 := NewTypedMap(mapTr, Uint64(0), Uint32(1), Uint64(2), Uint32(3))
	assert.True(m2.Equals(m))
}

func TestReadSetOfUint8(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, false, ["0", "1", "2", "3"]]`, SetKind, Uint8Kind)
	r := newJSONArrayReader(a, cs)

	tr := MakeSetType(Uint8Type)

	s := r.readTopLevelValue()
	s2 := NewTypedSet(tr, Uint8(0), Uint8(1), Uint8(2), Uint8(3))
	assert.True(s2.Equals(s))
}

func TestReadValueSetOfUint16(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, %d, false, ["0", "1", "2", "3"]]`, ValueKind, SetKind, Uint16Kind)
	r := newJSONArrayReader(a, cs)

	setTr := MakeSetType(Uint16Type)

	s := r.readTopLevelValue()
	s2 := NewTypedSet(setTr, Uint16(0), Uint16(1), Uint16(2), Uint16(3))
	assert.True(s2.Equals(s))
}

func TestReadCompoundBlob(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	r1 := ref.Parse("sha1-0000000000000000000000000000000000000001")
	r2 := ref.Parse("sha1-0000000000000000000000000000000000000002")
	r3 := ref.Parse("sha1-0000000000000000000000000000000000000003")
	a := parseJSON(`[%d, true, ["%s", "20", "20", "%s", "40", "40", "%s", "60", "60"]]`, BlobKind, r1, r2, r3)
	r := newJSONArrayReader(a, cs)

	m := r.readTopLevelValue()
	_, ok := m.(compoundBlob)
	assert.True(ok)
	m2 := newCompoundBlob([]metaTuple{
		newMetaTuple(Uint64(20), nil, NewTypedRef(MakeRefType(typeForBlob), r1), 20),
		newMetaTuple(Uint64(40), nil, NewTypedRef(MakeRefType(typeForBlob), r2), 40),
		newMetaTuple(Uint64(60), nil, NewTypedRef(MakeRefType(typeForBlob), r3), 60),
	}, cs)

	assert.True(m.Type().Equals(m2.Type()))
	assert.Equal(m.Ref().String(), m2.Ref().String())
}

func TestReadStruct(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	typ := MakeStructType("A1", []Field{
		Field{"x", Int16Type, false},
		Field{"s", StringType, false},
		Field{"b", BoolType, false},
	}, []Field{})
	pkg := NewPackage([]*Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJSON(`[%d, "%s", "0", "42", "hi", true]`, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)

	v := r.readTopLevelValue().(Struct)
	assert.True(v.Get("x").Equals(Int16(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))
	assert.True(v.Get("b").Equals(Bool(true)))
}

func TestReadStructUnion(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	typ := MakeStructType("A2", []Field{
		Field{"x", Float32Type, false},
	}, []Field{
		Field{"b", BoolType, false},
		Field{"s", StringType, false},
	})
	pkg := NewPackage([]*Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJSON(`[%d, "%s", "0", "42", "1", "hi"]`, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)

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
	cs := NewTestValueStore()

	typ := MakeStructType("A3", []Field{
		Field{"x", Float32Type, false},
		Field{"s", StringType, true},
		Field{"b", BoolType, true},
	}, []Field{})
	pkg := NewPackage([]*Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJSON(`[%d, "%s", "0", "42", false, true, false]`, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)
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
	cs := NewTestValueStore()

	// struct A4 {
	//   b: Bool
	//   l: List(Int32)
	//   s: String
	// }

	typ := MakeStructType("A4", []Field{
		Field{"b", BoolType, false},
		Field{"l", MakeListType(Int32Type), false},
		Field{"s", StringType, false},
	}, []Field{})
	pkg := NewPackage([]*Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJSON(`[%d, "%s", "0", true, false, ["0", "1", "2"], "hi"]`, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)
	l32Tr := MakeListType(Int32Type)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("b").Equals(Bool(true)))
	l := NewTypedList(l32Tr, Int32(0), Int32(1), Int32(2))
	assert.True(v.Get("l").Equals(l))
	assert.True(v.Get("s").Equals(NewString("hi")))
}

func TestReadStructWithValue(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	// struct A5 {
	//   b: Bool
	//   v: Value
	//   s: String
	// }

	typ := MakeStructType("A5", []Field{
		Field{"b", BoolType, false},
		Field{"v", ValueType, false},
		Field{"s", StringType, false},
	}, []Field{})
	pkg := NewPackage([]*Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJSON(`[%d, "%s", "0", true, %d, "42", "hi"]`, UnresolvedKind, pkgRef.String(), Uint8Kind)
	r := newJSONArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("b").Equals(Bool(true)))
	assert.True(v.Get("v").Equals(Uint8(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))
}

func TestReadValueStruct(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	// struct A1 {
	//   x: Float32
	//   b: Bool
	//   s: String
	// }

	typ := MakeStructType("A1", []Field{
		Field{"x", Int16Type, false},
		Field{"s", StringType, false},
		Field{"b", BoolType, false},
	}, []Field{})
	pkg := NewPackage([]*Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJSON(`[%d, %d, "%s", "0", "42", "hi", true]`, ValueKind, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("x").Equals(Int16(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))
	assert.True(v.Get("b").Equals(Bool(true)))
}

func TestReadRef(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	r := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	a := parseJSON(`[%d, %d, "%s"]`, RefKind, Uint32Kind, r.String())
	reader := newJSONArrayReader(a, cs)
	v := reader.readTopLevelValue()
	tr := MakeRefType(Uint32Type)
	assert.True(refFromType(r, tr).Equals(v))
}

func TestReadValueRef(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	r := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	a := parseJSON(`[%d, %d, %d, "%s"]`, ValueKind, RefKind, Uint32Kind, r.String())
	reader := newJSONArrayReader(a, cs)
	v := reader.readTopLevelValue()
	tr := MakeRefType(Uint32Type)
	assert.True(refFromType(r, tr).Equals(v))
}

func TestReadStructWithBlob(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	// struct A5 {
	//   b: Blob
	// }

	typ := MakeStructType("A5", []Field{
		Field{"b", BlobType, false},
	}, []Field{})
	pkg := NewPackage([]*Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJSON(`[%d, "%s", "0", false, "AAE="]`, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	blob := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	assert.True(v.Get("b").Equals(blob))
}

func TestReadTypeValue(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	test := func(expected *Type, json string, vs ...interface{}) {
		a := parseJSON(json, vs...)
		r := newJSONArrayReader(a, cs)
		tr := r.readTopLevelValue()
		assert.True(expected.Equals(tr))
	}

	test(Int32Type,
		`[%d, %d]`, TypeKind, Int32Kind)
	test(MakeListType(BoolType),
		`[%d, %d, [%d]]`, TypeKind, ListKind, BoolKind)
	test(MakeMapType(BoolType, StringType),
		`[%d, %d, [%d, %d]]`, TypeKind, MapKind, BoolKind, StringKind)

	test(MakeStructType("S", []Field{
		Field{"x", Int16Type, false},
		Field{"v", ValueType, true},
	}, []Field{}),
		`[%d, %d, "S", ["x", %d, false, "v", %d, true], []]`, TypeKind, StructKind, Int16Kind, ValueKind)

	test(MakeStructType("S", []Field{}, []Field{
		Field{"x", Int16Type, false},
		Field{"v", ValueType, false},
	}),
		`[%d, %d, "S", [], ["x", %d, false, "v", %d, false]]`, TypeKind, StructKind, Int16Kind, ValueKind)

	pkgRef := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	test(MakeType(pkgRef, 123), `[%d, %d, "%s", "123"]`, TypeKind, UnresolvedKind, pkgRef.String())

	test(MakeStructType("S", []Field{
		Field{"e", MakeType(pkgRef, 123), false},
		Field{"x", Int64Type, false},
	}, []Field{}),
		`[%d, %d, "S", ["e", %d, "%s", "123", false, "x", %d, false], []]`, TypeKind, StructKind, UnresolvedKind, pkgRef.String(), Int64Kind)

	test(MakeUnresolvedType("ns", "n"), `[%d, %d, "%s", "-1", "ns", "n"]`, TypeKind, UnresolvedKind, ref.Ref{}.String())
}

func TestReadPackage2(t *testing.T) {
	cs := NewTestValueStore()

	rr := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	setTref := MakeSetType(Uint32Type)
	pkg := NewPackage([]*Type{setTref}, []ref.Ref{rr})

	a := []interface{}{float64(PackageKind), []interface{}{float64(SetKind), []interface{}{float64(Uint32Kind)}}, []interface{}{rr.String()}}
	r := newJSONArrayReader(a, cs)
	v := r.readTopLevelValue().(Package)
	assert.True(t, pkg.Equals(v))
}

func TestReadPackageThroughChunkSource(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	pkg := NewPackage([]*Type{
		MakeStructType("S", []Field{
			Field{"X", Int32Type, false},
		}, []Field{}),
	}, []ref.Ref{})
	// Don't register
	pkgRef := cs.WriteValue(pkg).TargetRef()

	a := parseJSON(`[%d, "%s", "0", "42"]`, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("X").Equals(Int32(42)))
}
