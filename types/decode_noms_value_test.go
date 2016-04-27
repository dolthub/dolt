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

	a := []interface{}{Number(1), "hi", true}
	r := newJSONArrayReader(a, cs)

	assert.Equal(Number(1), r.read().(Number))
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
	test(Number(0), `[%d, "0"]`, NumberKind)
	test(NewString("hi"), `[%d, "hi"]`, StringKind)

	blob := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	test(blob, `[%d, false, "AAE="]`, BlobKind)
}

func TestReadListOfNumber(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, false, ["0", "1", "2", "3"]]`, ListKind, NumberKind)
	r := newJSONArrayReader(a, cs)

	tr := MakeListType(NumberType)

	l := r.readTopLevelValue()
	l2 := NewTypedList(tr, Number(0), Number(1), Number(2), Number(3))
	assert.True(l2.Equals(l))
}

func TestReadListOfValue(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, false, [%d, "1", %d, "hi", %d, true]]`, ListKind, ValueKind, NumberKind, StringKind, BoolKind)
	r := newJSONArrayReader(a, cs)
	l := r.readTopLevelValue()
	assert.True(NewList(Number(1), NewString("hi"), Bool(true)).Equals(l))
}

func TestReadValueListOfNumber(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, %d, false, ["0", "1", "2"]]`, ValueKind, ListKind, NumberKind)
	r := newJSONArrayReader(a, cs)

	tr := MakeListType(NumberType)

	l := r.readTopLevelValue()
	l2 := NewTypedList(tr, Number(0), Number(1), Number(2))
	assert.True(l2.Equals(l))
}

func TestReadCompoundList(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	tr := MakeListType(NumberType)
	leaf1 := newListLeaf(tr, Number(0))
	leaf2 := newListLeaf(tr, Number(1), Number(2), Number(3))
	l2 := buildCompoundList([]metaTuple{
		newMetaTuple(Number(1), leaf1, Ref{}, 1),
		newMetaTuple(Number(4), leaf2, Ref{}, 4),
	}, tr, cs)

	a := parseJSON(`[%d, %d, true, ["%s", "1", "1", "%s", "4", "4"]]`, ListKind, NumberKind, leaf1.Ref(), leaf2.Ref())
	r := newJSONArrayReader(a, cs)
	l := r.readTopLevelValue()

	assert.True(l2.Equals(l))
}

func TestReadCompoundSet(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	tr := MakeSetType(NumberType)
	leaf1 := newSetLeaf(tr, Number(0), Number(1))
	leaf2 := newSetLeaf(tr, Number(2), Number(3), Number(4))
	l2 := buildCompoundSet([]metaTuple{
		newMetaTuple(Number(1), leaf1, Ref{}, 2),
		newMetaTuple(Number(4), leaf2, Ref{}, 3),
	}, tr, cs)

	a := parseJSON(`[%d, %d, true, ["%s", "1", "2", "%s", "4", "3"]]`, SetKind, NumberKind, leaf1.Ref(), leaf2.Ref())
	r := newJSONArrayReader(a, cs)
	l := r.readTopLevelValue()

	assert.True(l2.Equals(l))
}

func TestReadMapOfNumberToNumber(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, %d, false, ["0", "1", "2", "3"]]`, MapKind, NumberKind, NumberKind)
	r := newJSONArrayReader(a, cs)

	tr := MakeMapType(NumberType, NumberType)

	m := r.readTopLevelValue()
	m2 := NewTypedMap(tr, Number(0), Number(1), Number(2), Number(3))
	assert.True(m2.Equals(m))
}

func TestReadValueMapOfNumberToNumber(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, %d, %d, false, ["0", "1", "2", "3"]]`, ValueKind, MapKind, NumberKind, NumberKind)
	r := newJSONArrayReader(a, cs)

	tr := MakeMapType(NumberType, NumberType)

	m := r.readTopLevelValue()
	m2 := NewTypedMap(tr, Number(0), Number(1), Number(2), Number(3))
	assert.True(m2.Equals(m))
}

func TestReadSetOfNumber(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, false, ["0", "1", "2", "3"]]`, SetKind, NumberKind)
	r := newJSONArrayReader(a, cs)

	tr := MakeSetType(NumberType)

	s := r.readTopLevelValue()
	s2 := NewTypedSet(tr, Number(0), Number(1), Number(2), Number(3))
	assert.True(s2.Equals(s))
}

func TestReadValueSetOfNumber(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	a := parseJSON(`[%d, %d, %d, false, ["0", "1", "2", "3"]]`, ValueKind, SetKind, NumberKind)
	r := newJSONArrayReader(a, cs)

	setTr := MakeSetType(NumberType)

	s := r.readTopLevelValue()
	s2 := NewTypedSet(setTr, Number(0), Number(1), Number(2), Number(3))
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
		newMetaTuple(Number(20), nil, NewTypedRef(MakeRefType(typeForBlob), r1), 20),
		newMetaTuple(Number(40), nil, NewTypedRef(MakeRefType(typeForBlob), r2), 40),
		newMetaTuple(Number(60), nil, NewTypedRef(MakeRefType(typeForBlob), r3), 60),
	}, cs)

	assert.True(m.Type().Equals(m2.Type()))
	assert.Equal(m.Ref().String(), m2.Ref().String())
}

func TestReadStruct(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	typ := MakeStructType("A1", []Field{
		Field{"x", NumberType, false},
		Field{"s", StringType, false},
		Field{"b", BoolType, false},
	}, []Field{})
	pkg := NewPackage([]*Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJSON(`[%d, "%s", "0", "42", "hi", true]`, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)

	v := r.readTopLevelValue().(Struct)
	assert.True(v.Get("x").Equals(Number(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))
	assert.True(v.Get("b").Equals(Bool(true)))
}

func TestReadStructUnion(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	typ := MakeStructType("A2", []Field{
		Field{"x", NumberType, false},
	}, []Field{
		Field{"b", BoolType, false},
		Field{"s", StringType, false},
	})
	pkg := NewPackage([]*Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJSON(`[%d, "%s", "0", "42", "1", "hi"]`, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)

	v := r.readTopLevelValue().(Struct)
	assert.True(v.Get("x").Equals(Number(42)))
	assert.Equal(uint64(Number(1)), uint64(v.UnionIndex()))
	assert.True(v.UnionValue().Equals(NewString("hi")))

	x, ok := v.MaybeGet("x")
	assert.True(ok)
	assert.True(x.Equals(Number(42)))

	s, ok := v.MaybeGet("s")
	assert.True(ok)
	assert.True(s.Equals(NewString("hi")))
	assert.True(v.UnionValue().Equals(s))
}

func TestReadStructOptional(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	typ := MakeStructType("A3", []Field{
		Field{"x", NumberType, false},
		Field{"s", StringType, true},
		Field{"b", BoolType, true},
	}, []Field{})
	pkg := NewPackage([]*Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJSON(`[%d, "%s", "0", "42", false, true, false]`, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("x").Equals(Number(42)))
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
	//   l: List(Number)
	//   s: String
	// }

	typ := MakeStructType("A4", []Field{
		Field{"b", BoolType, false},
		Field{"l", MakeListType(NumberType), false},
		Field{"s", StringType, false},
	}, []Field{})
	pkg := NewPackage([]*Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJSON(`[%d, "%s", "0", true, false, ["0", "1", "2"], "hi"]`, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)
	l32Tr := MakeListType(NumberType)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("b").Equals(Bool(true)))
	l := NewTypedList(l32Tr, Number(0), Number(1), Number(2))
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

	a := parseJSON(`[%d, "%s", "0", true, %d, "42", "hi"]`, UnresolvedKind, pkgRef.String(), NumberKind)
	r := newJSONArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("b").Equals(Bool(true)))
	assert.True(v.Get("v").Equals(Number(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))
}

func TestReadValueStruct(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	// struct A1 {
	//   x: Number
	//   b: Bool
	//   s: String
	// }

	typ := MakeStructType("A1", []Field{
		Field{"x", NumberType, false},
		Field{"s", StringType, false},
		Field{"b", BoolType, false},
	}, []Field{})
	pkg := NewPackage([]*Type{typ}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	a := parseJSON(`[%d, %d, "%s", "0", "42", "hi", true]`, ValueKind, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("x").Equals(Number(42)))
	assert.True(v.Get("s").Equals(NewString("hi")))
	assert.True(v.Get("b").Equals(Bool(true)))
}

func TestReadRef(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	r := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	a := parseJSON(`[%d, %d, "%s"]`, RefKind, NumberKind, r.String())
	reader := newJSONArrayReader(a, cs)
	v := reader.readTopLevelValue()
	tr := MakeRefType(NumberType)
	assert.True(NewTypedRef(tr, r).Equals(v))
}

func TestReadValueRef(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	r := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	a := parseJSON(`[%d, %d, %d, "%s"]`, ValueKind, RefKind, NumberKind, r.String())
	reader := newJSONArrayReader(a, cs)
	v := reader.readTopLevelValue()
	tr := MakeRefType(NumberType)
	assert.True(NewTypedRef(tr, r).Equals(v))
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

	test(NumberType,
		`[%d, %d]`, TypeKind, NumberKind)
	test(MakeListType(BoolType),
		`[%d, %d, [%d]]`, TypeKind, ListKind, BoolKind)
	test(MakeMapType(BoolType, StringType),
		`[%d, %d, [%d, %d]]`, TypeKind, MapKind, BoolKind, StringKind)

	test(MakeStructType("S", []Field{
		Field{"x", NumberType, false},
		Field{"v", ValueType, true},
	}, []Field{}),
		`[%d, %d, "S", ["x", %d, false, "v", %d, true], []]`, TypeKind, StructKind, NumberKind, ValueKind)

	test(MakeStructType("S", []Field{}, []Field{
		Field{"x", NumberType, false},
		Field{"v", ValueType, false},
	}),
		`[%d, %d, "S", [], ["x", %d, false, "v", %d, false]]`, TypeKind, StructKind, NumberKind, ValueKind)

	pkgRef := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	test(MakeType(pkgRef, 123), `[%d, %d, "%s", "123"]`, TypeKind, UnresolvedKind, pkgRef.String())

	test(MakeStructType("S", []Field{
		Field{"e", MakeType(pkgRef, 123), false},
		Field{"x", NumberType, false},
	}, []Field{}),
		`[%d, %d, "S", ["e", %d, "%s", "123", false, "x", %d, false], []]`, TypeKind, StructKind, UnresolvedKind, pkgRef.String(), NumberKind)

	test(MakeUnresolvedType("ns", "n"), `[%d, %d, "%s", "-1", "ns", "n"]`, TypeKind, UnresolvedKind, ref.Ref{}.String())
}

func TestReadPackage2(t *testing.T) {
	cs := NewTestValueStore()

	rr := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	setTref := MakeSetType(NumberType)
	pkg := NewPackage([]*Type{setTref}, []ref.Ref{rr})

	a := []interface{}{float64(PackageKind), []interface{}{float64(SetKind), []interface{}{float64(NumberKind)}}, []interface{}{rr.String()}}
	r := newJSONArrayReader(a, cs)
	v := r.readTopLevelValue().(Package)
	assert.True(t, pkg.Equals(v))
}

func TestReadPackageThroughChunkSource(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	pkg := NewPackage([]*Type{
		MakeStructType("S", []Field{
			Field{"X", NumberType, false},
		}, []Field{}),
	}, []ref.Ref{})
	// Don't register
	pkgRef := cs.WriteValue(pkg).TargetRef()

	a := parseJSON(`[%d, "%s", "0", "42"]`, UnresolvedKind, pkgRef.String())
	r := newJSONArrayReader(a, cs)
	v := r.readTopLevelValue().(Struct)

	assert.True(v.Get("X").Equals(Number(42)))
}
