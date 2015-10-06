package types

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/ref"
)

type testNomsValue struct {
	Value
	t TypeRef
}

func (nv testNomsValue) NomsValue() Value {
	return nv.Value
}

func (nv testNomsValue) TypeRef() TypeRef {
	return nv.t
}

func TestWrite(t *testing.T) {
	assert := assert.New(t)

	w := newJsonArrayWriter()
	w.writeTypeRef(MakePrimitiveTypeRef(UInt64Kind))

	assert.EqualValues([]interface{}{UInt64Kind}, *w)
}

func TestWritePrimitives(t *testing.T) {
	assert := assert.New(t)

	f := func(k NomsKind, v Value, ex interface{}) {
		w := newJsonArrayWriter()
		tref := MakePrimitiveTypeRef(k)
		w.writeTypeRef(tref)
		w.writeValue(tref, v, nil)
		assert.EqualValues([]interface{}{k, ex}, *w)
	}

	f(BoolKind, Bool(true), true)
	f(BoolKind, Bool(false), false)

	f(UInt8Kind, UInt8(0), uint8(0))
	f(UInt16Kind, UInt16(0), uint16(0))
	f(UInt32Kind, UInt32(0), uint32(0))
	f(UInt64Kind, UInt64(0), uint64(0))
	f(Int8Kind, Int8(0), int8(0))
	f(Int16Kind, Int16(0), int16(0))
	f(Int32Kind, Int32(0), int32(0))
	f(Int64Kind, Int64(0), int64(0))
	f(Float32Kind, Float32(0), float32(0))
	f(Float64Kind, Float64(0), float64(0))

	f(StringKind, NewString("hi"), "hi")
}

func TestWriteList(t *testing.T) {
	assert := assert.New(t)

	tref := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(Int32Kind))
	v := NewList(Int32(0), Int32(1), Int32(2), Int32(3))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeList(tref, v, nil)
	assert.EqualValues([]interface{}{ListKind, Int32Kind, int32(0), int32(1), int32(2), int32(3)}, *w)
}

func TestWriteListOfList(t *testing.T) {
	assert := assert.New(t)

	it := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(Int16Kind))
	tref := MakeCompoundTypeRef("", ListKind, it)
	v := NewList(NewList(Int16(0)), NewList(Int16(1), Int16(2), Int16(3)))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeList(tref, v, nil)
	assert.EqualValues([]interface{}{ListKind, ListKind, Int16Kind, []interface{}{int16(0)}, []interface{}{int16(1), int16(2), int16(3)}}, *w)
}

func TestWriteSet(t *testing.T) {
	assert := assert.New(t)

	tref := MakeCompoundTypeRef("", SetKind, MakePrimitiveTypeRef(UInt32Kind))
	v := NewSet(UInt32(0), UInt32(1), UInt32(2), UInt32(3))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeSet(tref, v, nil)
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{SetKind, UInt32Kind, uint32(3), uint32(1), uint32(0), uint32(2)}, *w)
}

func TestWriteSetOfSet(t *testing.T) {
	assert := assert.New(t)

	st := MakeCompoundTypeRef("", SetKind, MakePrimitiveTypeRef(Int32Kind))
	tref := MakeCompoundTypeRef("", SetKind, st)
	v := NewSet(NewSet(Int32(0)), NewSet(Int32(1), Int32(2), Int32(3)))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeSet(tref, v, nil)
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{SetKind, SetKind, Int32Kind, []interface{}{int32(0)}, []interface{}{int32(1), int32(3), int32(2)}}, *w)
}

func TestWriteMap(t *testing.T) {
	assert := assert.New(t)

	tref := MakeCompoundTypeRef("", MapKind, MakePrimitiveTypeRef(StringKind), MakePrimitiveTypeRef(BoolKind))
	v := NewMap(NewString("a"), Bool(false), NewString("b"), Bool(true))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeMap(tref, v, nil)
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MapKind, StringKind, BoolKind, "a", false, "b", true}, *w)
}

func TestWriteMapOfMap(t *testing.T) {
	assert := assert.New(t)

	kt := MakeCompoundTypeRef("", MapKind, MakePrimitiveTypeRef(StringKind), MakePrimitiveTypeRef(Int64Kind))
	vt := MakeCompoundTypeRef("", SetKind, MakePrimitiveTypeRef(BoolKind))
	tref := MakeCompoundTypeRef("", MapKind, kt, vt)
	v := NewMap(NewMap(NewString("a"), Int64(0)), NewSet(Bool(true)))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeMap(tref, v, nil)
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MapKind, MapKind, StringKind, Int64Kind, SetKind, BoolKind, []interface{}{"a", int64(0)}, []interface{}{true}}, *w)
}

func TestWriteEmptyStruct(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("S",
		MakeStructTypeRef("S", []Field{}, Choices{})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	v := NewMap()

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeExternal(tref, v, &pkg)
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S"}, *w)
}

func TestWriteStruct(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("S",
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int8Kind), false},
			Field{"b", MakePrimitiveTypeRef(BoolKind), false},
		}, Choices{})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	v := NewMap(NewString("x"), Int8(42), NewString("b"), Bool(true))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeExternal(tref, v, &pkg)
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", int8(42), true}, *w)
}

func TestWriteStructOptionalField(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("S",
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int8Kind), true},
			Field{"b", MakePrimitiveTypeRef(BoolKind), false},
		}, Choices{})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	v := NewMap(NewString("x"), Int8(42), NewString("b"), Bool(true))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeExternal(tref, v, &pkg)
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", true, int8(42), true}, *w)

	v = NewMap(NewString("b"), Bool(true))

	w = newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeExternal(tref, v, &pkg)
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", false, true}, *w)
}

func TestWriteStructWithUnion(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("S",
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int8Kind), false},
		}, Choices{
			Field{"b", MakePrimitiveTypeRef(BoolKind), false},
			Field{"s", MakePrimitiveTypeRef(StringKind), false},
		})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	v := NewMap(NewString("x"), Int8(42), NewString("$unionIndex"), UInt32(1), NewString("$unionValue"), NewString("hi"))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeExternal(tref, v, &pkg)
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", int8(42), uint32(1), "hi"}, *w)

	v = NewMap(NewString("x"), Int8(42), NewString("$unionIndex"), UInt32(0), NewString("$unionValue"), Bool(true))

	w = newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeExternal(tref, v, &pkg)
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", int8(42), uint32(0), true}, *w)
}

func TestWriteStructWithList(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("S",
		MakeStructTypeRef("S", []Field{
			Field{"l", MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(StringKind)), false},
		}, Choices{})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	v := NewMap(NewString("l"), NewList(NewString("a"), NewString("b")))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeExternal(tref, v, &pkg)
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", []interface{}{"a", "b"}}, *w)

	v = NewMap(NewString("l"), NewList())
	w = newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeExternal(tref, v, &pkg)
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", []interface{}{}}, *w)
}

func TestWriteStructWithStruct(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("S2",
		MakeStructTypeRef("S2", []Field{
			Field{"x", MakePrimitiveTypeRef(Int32Kind), false},
		}, Choices{})).Set("S",
		MakeStructTypeRef("S", []Field{
			Field{"s", MakeTypeRef("S2", ref.Ref{}), false},
		}, Choices{})))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("S", pkgRef)
	v := NewMap(NewString("s"), NewMap(NewString("x"), Int32(42)))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeExternal(tref, v, &pkg)
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "S", int32(42)}, *w)
}

func TestWriteEnum(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("E",
		MakeEnumTypeRef("E", "a", "b", "c")))
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef("E", pkgRef)
	v := UInt32(1)

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeExternal(tref, v, &pkg)
	assert.EqualValues([]interface{}{TypeRefKind, pkgRef.String(), "E", uint32(1)}, *w)
}

func TestWriteListOfEnum(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("E",
		MakeEnumTypeRef("E", "a", "b", "c")))
	pkgRef := RegisterPackage(&pkg)
	et := MakeTypeRef("E", pkgRef)
	tref := MakeCompoundTypeRef("", ListKind, et)
	v := NewList(UInt32(0), UInt32(1), UInt32(2))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeList(tref, v, &pkg)
	assert.EqualValues([]interface{}{ListKind, TypeRefKind, pkgRef.String(), "E", uint32(0), uint32(1), uint32(2)}, *w)
}

func TestWriteListOfValue(t *testing.T) {
	assert := assert.New(t)

	tref := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(ValueKind))
	v := NewList(
		Bool(true),
		UInt8(1),
		UInt16(1),
		UInt32(1),
		UInt64(1),
		Int8(1),
		Int16(1),
		Int32(1),
		Int64(1),
		Float32(1),
		Float64(1),
		NewString("hi"),
	)

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeList(tref, v, nil)

	assert.EqualValues([]interface{}{ListKind, ValueKind,
		BoolKind, true,
		UInt8Kind, uint8(1),
		UInt16Kind, uint16(1),
		UInt32Kind, uint32(1),
		UInt64Kind, uint64(1),
		Int8Kind, int8(1),
		Int16Kind, int16(1),
		Int32Kind, int32(1),
		Int64Kind, int64(1),
		Float32Kind, float32(1),
		Float64Kind, float64(1),
		StringKind, "hi",
	}, *w)
}

func TestWriteListOfValueWithStruct(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage().SetNamedTypes(NewMapOfStringToTypeRef().Set("S",
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int32Kind), false},
		}, Choices{})))
	pkgRef := RegisterPackage(&pkg)

	tref := MakeCompoundTypeRef("", ListKind, MakePrimitiveTypeRef(ValueKind))
	st := MakeTypeRef("S", pkgRef)
	v := NewList(NewMap(NewString("$type"), st, NewString("x"), Int32(42)))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	w.writeList(tref, v, &pkg)
	assert.EqualValues([]interface{}{ListKind, ValueKind, TypeRefKind, pkgRef.String(), "S", int32(42)}, *w)
}

func TestWriteRef(t *testing.T) {
	assert := assert.New(t)

	tref := MakeCompoundTypeRef("", RefKind, MakePrimitiveTypeRef(UInt32Kind))

	w := newJsonArrayWriter()
	w.writeTypeRef(tref)
	r := ref.Parse("sha1-a9993e364706816aba3e25717850c26c9cd0d89d")
	w.writeRef(r)

	assert.EqualValues([]interface{}{RefKind, UInt32Kind, r.String()}, *w)
}
