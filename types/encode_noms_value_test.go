package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func TestWritePrimitives(t *testing.T) {
	assert := assert.New(t)

	f := func(k NomsKind, v Value, ex interface{}) {
		cs := chunks.NewMemoryStore()
		w := newJsonArrayWriter(cs)
		w.writeTopLevelValue(v)
		assert.EqualValues([]interface{}{k, ex}, w.toArray())
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

	blob, err := NewMemoryBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	assert.NoError(err)
	f(BlobKind, blob, "AAE=")
}

type testList struct {
	List
	t TypeRef
}

func (l testList) TypeRef() TypeRef {
	return l.t
}

func (l testList) InternalImplementation() List {
	return l.List
}

func TestWriteList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	tref := MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(Int32Kind))
	v := NewList(Int32(0), Int32(1), Int32(2), Int32(3))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testList{List: v, t: tref})
	assert.EqualValues([]interface{}{ListKind, Int32Kind, []interface{}{int32(0), int32(1), int32(2), int32(3)}}, w.toArray())
}

func TestWriteListOfList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	it := MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(Int16Kind))
	tref := MakeCompoundTypeRef(ListKind, it)
	v := NewList(NewList(Int16(0)), NewList(Int16(1), Int16(2), Int16(3)))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testList{List: v, t: tref})
	assert.EqualValues([]interface{}{ListKind, ListKind, Int16Kind,
		[]interface{}{[]interface{}{int16(0)}, []interface{}{int16(1), int16(2), int16(3)}}}, w.toArray())
}

type testSet struct {
	Set
	t TypeRef
}

func (s testSet) TypeRef() TypeRef {
	return s.t
}

func (s testSet) InternalImplementation() Set {
	return s.Set
}

func TestWriteSet(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	tref := MakeCompoundTypeRef(SetKind, MakePrimitiveTypeRef(UInt32Kind))
	v := NewSet(UInt32(3), UInt32(1), UInt32(2), UInt32(0))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testSet{Set: v, t: tref})
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{SetKind, UInt32Kind, []interface{}{uint32(1), uint32(3), uint32(0), uint32(2)}}, w.toArray())
}

func TestWriteSetOfSet(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	st := MakeCompoundTypeRef(SetKind, MakePrimitiveTypeRef(Int32Kind))
	tref := MakeCompoundTypeRef(SetKind, st)
	v := NewSet(NewSet(Int32(0)), NewSet(Int32(1), Int32(2), Int32(3)))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testSet{Set: v, t: tref})
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{SetKind, SetKind, Int32Kind, []interface{}{[]interface{}{int32(1), int32(3), int32(2)}, []interface{}{int32(0)}}}, w.toArray())
}

type testMap struct {
	Map
	t TypeRef
}

func (m testMap) TypeRef() TypeRef {
	return m.t
}

func (m testMap) InternalImplementation() Map {
	return m.Map
}

func TestWriteMap(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	tref := MakeCompoundTypeRef(MapKind, MakePrimitiveTypeRef(StringKind), MakePrimitiveTypeRef(BoolKind))
	v := NewMap(NewString("a"), Bool(false), NewString("b"), Bool(true))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testMap{Map: v, t: tref})
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MapKind, StringKind, BoolKind, []interface{}{"a", false, "b", true}}, w.toArray())
}

func TestWriteMapOfMap(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	kt := MakeCompoundTypeRef(MapKind, MakePrimitiveTypeRef(StringKind), MakePrimitiveTypeRef(Int64Kind))
	vt := MakeCompoundTypeRef(SetKind, MakePrimitiveTypeRef(BoolKind))
	tref := MakeCompoundTypeRef(MapKind, kt, vt)
	v := NewMap(NewMap(NewString("a"), Int64(0)), NewSet(Bool(true)))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testMap{Map: v, t: tref})
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MapKind, MapKind, StringKind, Int64Kind, SetKind, BoolKind, []interface{}{[]interface{}{"a", int64(0)}, []interface{}{true}}}, w.toArray())
}

func TestWriteEmptyStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]TypeRef{
		MakeStructTypeRef("S", []Field{}, Choices{})}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef(pkgRef, 0)
	v := NewMap()

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testMap{Map: v, t: tref})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0)}, w.toArray())
}

func TestWriteStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]TypeRef{
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int8Kind), false},
			Field{"b", MakePrimitiveTypeRef(BoolKind), false},
		}, Choices{})}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef(pkgRef, 0)
	v := NewMap(NewString("x"), Int8(42), NewString("b"), Bool(true))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testMap{Map: v, t: tref})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), int8(42), true}, w.toArray())
}

func TestWriteStructOptionalField(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]TypeRef{
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int8Kind), true},
			Field{"b", MakePrimitiveTypeRef(BoolKind), false},
		}, Choices{})}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef(pkgRef, 0)
	v := NewMap(NewString("x"), Int8(42), NewString("b"), Bool(true))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testMap{Map: v, t: tref})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), true, int8(42), true}, w.toArray())

	v = NewMap(NewString("b"), Bool(true))

	w = newJsonArrayWriter(cs)
	w.writeTopLevelValue(testMap{Map: v, t: tref})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), false, true}, w.toArray())
}

func TestWriteStructWithUnion(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]TypeRef{
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int8Kind), false},
		}, Choices{
			Field{"b", MakePrimitiveTypeRef(BoolKind), false},
			Field{"s", MakePrimitiveTypeRef(StringKind), false},
		})}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef(pkgRef, 0)
	v := NewMap(NewString("x"), Int8(42), NewString("$unionIndex"), UInt32(1), NewString("$unionValue"), NewString("hi"))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testMap{Map: v, t: tref})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), int8(42), uint32(1), "hi"}, w.toArray())

	v = NewMap(NewString("x"), Int8(42), NewString("$unionIndex"), UInt32(0), NewString("$unionValue"), Bool(true))

	w = newJsonArrayWriter(cs)
	w.writeTopLevelValue(testMap{Map: v, t: tref})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), int8(42), uint32(0), true}, w.toArray())
}

func TestWriteStructWithList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]TypeRef{
		MakeStructTypeRef("S", []Field{
			Field{"l", MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(StringKind)), false},
		}, Choices{})}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef(pkgRef, 0)
	v := NewMap(NewString("l"), NewList(NewString("a"), NewString("b")))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testMap{Map: v, t: tref})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), []interface{}{"a", "b"}}, w.toArray())

	v = NewMap(NewString("l"), NewList())
	w = newJsonArrayWriter(cs)
	w.writeTopLevelValue(testMap{Map: v, t: tref})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), []interface{}{}}, w.toArray())
}

func TestWriteStructWithStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]TypeRef{
		MakeStructTypeRef("S2", []Field{
			Field{"x", MakePrimitiveTypeRef(Int32Kind), false},
		}, Choices{}),
		MakeStructTypeRef("S", []Field{
			Field{"s", MakeTypeRef(ref.Ref{}, 0), false},
		}, Choices{})}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef(pkgRef, 1)
	v := NewMap(NewString("s"), NewMap(NewString("x"), Int32(42)))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testMap{Map: v, t: tref})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(1), int32(42)}, w.toArray())
}

func TestWriteStructWithBlob(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]TypeRef{
		MakeStructTypeRef("S", []Field{
			Field{"b", MakePrimitiveTypeRef(BlobKind), false},
		}, Choices{})}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef(pkgRef, 0)
	b, _ := NewMemoryBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	v := NewMap(NewString("b"), b)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testMap{Map: v, t: tref})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), "AAE="}, w.toArray())
}

type testEnum struct {
	UInt32
	t TypeRef
}

func (e testEnum) TypeRef() TypeRef {
	return e.t
}

func (e testEnum) InternalImplementation() uint32 {
	return uint32(e.UInt32)
}

func TestWriteEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]TypeRef{
		MakeEnumTypeRef("E", "a", "b", "c")}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef(pkgRef, 0)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testEnum{UInt32: UInt32(1), t: tref})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), uint32(1)}, w.toArray())
}

func TestWriteListOfEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]TypeRef{
		MakeEnumTypeRef("E", "a", "b", "c")}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	et := MakeTypeRef(pkgRef, 0)
	tref := MakeCompoundTypeRef(ListKind, et)
	v := NewList(testEnum{UInt32(0), et}, testEnum{UInt32(1), et}, testEnum{UInt32(2), et})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testList{List: v, t: tref})
	assert.EqualValues([]interface{}{ListKind, UnresolvedKind, pkgRef.String(), int16(0), []interface{}{uint32(0), uint32(1), uint32(2)}}, w.toArray())
}

func TestWriteListOfValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	tref := MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(ValueKind))
	blob, _ := NewMemoryBlob(bytes.NewBuffer([]byte{0x01}))
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
		blob,
	)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testList{List: v, t: tref})

	assert.EqualValues([]interface{}{ListKind, ValueKind, []interface{}{
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
		BlobKind, "AQ==",
	}}, w.toArray())
}

func TestWriteListOfValueWithStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]TypeRef{
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int32Kind), false},
		}, Choices{})}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	tref := MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(ValueKind))
	st := MakeTypeRef(pkgRef, 0)
	v := NewList(testMap{Map: NewMap(NewString("x"), Int32(42)), t: st})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testList{List: v, t: tref})
	assert.EqualValues([]interface{}{ListKind, ValueKind, []interface{}{UnresolvedKind, pkgRef.String(), int16(0), int32(42)}}, w.toArray())
}

func TestWriteListOfValueWithTypeRefs(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]TypeRef{
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int32Kind), false},
		}, Choices{})}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	tref := MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(ValueKind))
	v := NewList(
		Bool(true),
		MakePrimitiveTypeRef(Int32Kind),
		MakePrimitiveTypeRef(TypeRefKind),
		MakeTypeRef(pkgRef, 0),
	)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testList{List: v, t: tref})
	assert.EqualValues([]interface{}{ListKind, ValueKind, []interface{}{
		BoolKind, true,
		TypeRefKind, Int32Kind,
		TypeRefKind, TypeRefKind,
		TypeRefKind, UnresolvedKind, pkgRef.String(), int16(0),
	}}, w.toArray())
}

type testRef struct {
	Value
	t TypeRef
}

func (r testRef) TypeRef() TypeRef {
	return r.t
}

func (r testRef) TargetRef() ref.Ref {
	return r.Value.(Ref).TargetRef()
}

func TestWriteRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	tref := MakeCompoundTypeRef(RefKind, MakePrimitiveTypeRef(UInt32Kind))
	r := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	v := NewRef(r)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testRef{Value: v, t: tref})
	assert.EqualValues([]interface{}{RefKind, UInt32Kind, r.String()}, w.toArray())
}

func TestWriteTypeRefValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	test := func(expected []interface{}, v TypeRef) {
		w := newJsonArrayWriter(cs)
		w.writeTopLevelValue(v)
		assert.EqualValues(expected, w.toArray())
	}

	test([]interface{}{TypeRefKind, Int32Kind}, MakePrimitiveTypeRef(Int32Kind))
	test([]interface{}{TypeRefKind, ListKind, []interface{}{BoolKind}},
		MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(BoolKind)))
	test([]interface{}{TypeRefKind, MapKind, []interface{}{BoolKind, StringKind}},
		MakeCompoundTypeRef(MapKind, MakePrimitiveTypeRef(BoolKind), MakePrimitiveTypeRef(StringKind)))
	test([]interface{}{TypeRefKind, EnumKind, "E", []interface{}{"a", "b", "c"}},
		MakeEnumTypeRef("E", "a", "b", "c"))

	test([]interface{}{TypeRefKind, StructKind, "S", []interface{}{"x", Int16Kind, false, "v", ValueKind, true}, []interface{}{}},
		MakeStructTypeRef("S", []Field{
			Field{"x", MakePrimitiveTypeRef(Int16Kind), false},
			Field{"v", MakePrimitiveTypeRef(ValueKind), true},
		}, Choices{}))

	test([]interface{}{TypeRefKind, StructKind, "S", []interface{}{}, []interface{}{"x", Int16Kind, false, "v", ValueKind, false}},
		MakeStructTypeRef("S", []Field{}, Choices{
			Field{"x", MakePrimitiveTypeRef(Int16Kind), false},
			Field{"v", MakePrimitiveTypeRef(ValueKind), false},
		}))

	pkgRef := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	test([]interface{}{TypeRefKind, UnresolvedKind, pkgRef.String(), int16(123)},
		MakeTypeRef(pkgRef, 123))

	test([]interface{}{TypeRefKind, StructKind, "S", []interface{}{"e", UnresolvedKind, pkgRef.String(), int16(123), false, "x", Int64Kind, false}, []interface{}{}},
		MakeStructTypeRef("S", []Field{
			Field{"e", MakeTypeRef(pkgRef, 123), false},
			Field{"x", MakePrimitiveTypeRef(Int64Kind), false},
		}, Choices{}))

	test([]interface{}{TypeRefKind, UnresolvedKind, ref.Ref{}.String(), int16(-1), "ns", "n"},
		MakeUnresolvedTypeRef("ns", "n"))
}

func TestWriteListOfTypeRefs(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	tref := MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(TypeRefKind))
	v := NewList(MakePrimitiveTypeRef(BoolKind), MakeEnumTypeRef("E", "a", "b", "c"), MakePrimitiveTypeRef(StringKind))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testList{List: v, t: tref})
	assert.EqualValues([]interface{}{ListKind, TypeRefKind, []interface{}{BoolKind, EnumKind, "E", []interface{}{"a", "b", "c"}, StringKind}}, w.toArray())
}

func TestWritePackage(t *testing.T) {
	cs := chunks.NewMemoryStore()
	pkg := NewPackage([]TypeRef{
		MakeStructTypeRef("EnumStruct",
			[]Field{
				Field{"hand", MakeTypeRef(ref.Ref{}, 1), false},
			},
			Choices{},
		),
		MakeEnumTypeRef("Handedness", "right", "left", "switch"),
	}, []ref.Ref{})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(pkg)

	// struct Package {
	// 	Dependencies: Set(Ref(Package))
	// 	Types: List(TypeRef)
	// }

	exp := []interface{}{
		PackageKind,
		[]interface{}{
			StructKind, "EnumStruct", []interface{}{
				"hand", UnresolvedKind, "sha1-0000000000000000000000000000000000000000", int16(1), false,
			}, []interface{}{},
			EnumKind, "Handedness", []interface{}{"right", "left", "switch"},
		},
		[]interface{}{}, // Dependencies
	}

	assert.EqualValues(t, exp, w.toArray())
}

func TestWritePackage2(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	setTref := MakeCompoundTypeRef(SetKind, MakePrimitiveTypeRef(UInt32Kind))
	r := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	v := Package{[]TypeRef{setTref}, []ref.Ref{r}, &ref.Ref{}}

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{PackageKind, []interface{}{SetKind, []interface{}{UInt32Kind}}, []interface{}{r.String()}}, w.toArray())
}
