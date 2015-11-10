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

func TestWriteList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	tref := MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(Int32Kind))
	v := NewList(Int32(0), Int32(1), Int32(2), Int32(3))
	v.t = tref

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, Int32Kind, []interface{}{int32(0), int32(1), int32(2), int32(3)}}, w.toArray())
}

func TestWriteListOfList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	it := MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(Int16Kind))
	tref := MakeCompoundTypeRef(ListKind, it)
	l1 := NewList(Int16(0))
	l1.t = it
	l2 := NewList(Int16(1), Int16(2), Int16(3))
	l2.t = it
	v := NewList(l1, l2)
	v.t = tref

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, ListKind, Int16Kind,
		[]interface{}{[]interface{}{int16(0)}, []interface{}{int16(1), int16(2), int16(3)}}}, w.toArray())
}

func TestWriteSet(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	tref := MakeCompoundTypeRef(SetKind, MakePrimitiveTypeRef(UInt32Kind))
	v := NewSet(UInt32(3), UInt32(1), UInt32(2), UInt32(0))
	v.t = tref

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{SetKind, UInt32Kind, []interface{}{uint32(1), uint32(3), uint32(0), uint32(2)}}, w.toArray())
}

func TestWriteSetOfSet(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	st := MakeCompoundTypeRef(SetKind, MakePrimitiveTypeRef(Int32Kind))
	tref := MakeCompoundTypeRef(SetKind, st)
	v := NewSet(NewSet(Int32(0)), NewSet(Int32(1), Int32(2), Int32(3)))
	v.t = tref

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{SetKind, SetKind, Int32Kind, []interface{}{[]interface{}{int32(1), int32(3), int32(2)}, []interface{}{int32(0)}}}, w.toArray())
}

func TestWriteMap(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	tref := MakeCompoundTypeRef(MapKind, MakePrimitiveTypeRef(StringKind), MakePrimitiveTypeRef(BoolKind))
	v := NewMap(NewString("a"), Bool(false), NewString("b"), Bool(true))
	v.t = tref

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
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
	v.t = tref

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MapKind, MapKind, StringKind, Int64Kind, SetKind, BoolKind, []interface{}{[]interface{}{"a", int64(0)}, []interface{}{true}}}, w.toArray())
}

func TestWriteCompoundBlob(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	r1 := ref.Parse("sha1-0000000000000000000000000000000000000001")
	r2 := ref.Parse("sha1-0000000000000000000000000000000000000002")
	r3 := ref.Parse("sha1-0000000000000000000000000000000000000003")

	v := newCompoundBlob([]metaTuple{{r1, UInt64(20)}, {r2, UInt64(40)}, {r3, UInt64(60)}}, cs)
	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)

	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MetaSequenceKind, BlobKind, []interface{}{r1.String(), uint64(20), r2.String(), uint64(40), r3.String(), uint64(60)}}, w.toArray())
}

func TestWriteEmptyStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructTypeRef("S", []Field{}, Choices{})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typeRef := MakeTypeRef(pkgRef, 0)
	v := NewStruct(typeRef, typeDef, nil)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0)}, w.toArray())
}

func TestWriteStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructTypeRef("S", []Field{
		Field{"x", MakePrimitiveTypeRef(Int8Kind), false},
		Field{"b", MakePrimitiveTypeRef(BoolKind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typeRef := MakeTypeRef(pkgRef, 0)
	v := NewStruct(typeRef, typeDef, structData{"x": Int8(42), "b": Bool(true)})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), int8(42), true}, w.toArray())
}

func TestWriteStructOptionalField(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructTypeRef("S", []Field{
		Field{"x", MakePrimitiveTypeRef(Int8Kind), true},
		Field{"b", MakePrimitiveTypeRef(BoolKind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typeRef := MakeTypeRef(pkgRef, 0)
	v := NewStruct(typeRef, typeDef, structData{"x": Int8(42), "b": Bool(true)})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), true, int8(42), true}, w.toArray())

	v = NewStruct(typeRef, typeDef, structData{"b": Bool(true)})

	w = newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), false, true}, w.toArray())
}

func TestWriteStructWithUnion(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructTypeRef("S", []Field{
		Field{"x", MakePrimitiveTypeRef(Int8Kind), false},
	}, Choices{
		Field{"b", MakePrimitiveTypeRef(BoolKind), false},
		Field{"s", MakePrimitiveTypeRef(StringKind), false},
	})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typeRef := MakeTypeRef(pkgRef, 0)
	v := NewStruct(typeRef, typeDef, structData{"x": Int8(42), "s": NewString("hi")})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), int8(42), uint32(1), "hi"}, w.toArray())

	v = NewStruct(typeRef, typeDef, structData{"x": Int8(42), "b": Bool(true)})

	w = newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), int8(42), uint32(0), true}, w.toArray())
}

func TestWriteStructWithList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructTypeRef("S", []Field{
		Field{"l", MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(StringKind)), false},
	}, Choices{})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typeRef := MakeTypeRef(pkgRef, 0)

	v := NewStruct(typeRef, typeDef, structData{"l": NewList(NewString("a"), NewString("b"))})
	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), []interface{}{"a", "b"}}, w.toArray())

	v = NewStruct(typeRef, typeDef, structData{"l": NewList()})
	w = newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), []interface{}{}}, w.toArray())
}

func TestWriteStructWithStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	s2TypeDef := MakeStructTypeRef("S2", []Field{
		Field{"x", MakePrimitiveTypeRef(Int32Kind), false},
	}, Choices{})
	sTypeDef := MakeStructTypeRef("S", []Field{
		Field{"s", MakeTypeRef(ref.Ref{}, 0), false},
	}, Choices{})
	pkg := NewPackage([]Type{s2TypeDef, sTypeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	s2TypeRef := MakeTypeRef(pkgRef, 0)
	sTypeRef := MakeTypeRef(pkgRef, 1)

	v := NewStruct(sTypeRef, sTypeDef, structData{"s": NewStruct(s2TypeRef, s2TypeDef, structData{"x": Int32(42)})})
	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(1), int32(42)}, w.toArray())
}

func TestWriteStructWithBlob(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructTypeRef("S", []Field{
		Field{"b", MakePrimitiveTypeRef(BlobKind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typeRef := MakeTypeRef(pkgRef, 0)
	b, _ := NewMemoryBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	v := NewStruct(typeRef, typeDef, structData{"b": b})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), "AAE="}, w.toArray())
}

func TestWriteEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]Type{
		MakeEnumTypeRef("E", "a", "b", "c")}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	tref := MakeTypeRef(pkgRef, 0)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(Enum{1, tref})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), uint32(1)}, w.toArray())
}

func TestWriteListOfEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]Type{
		MakeEnumTypeRef("E", "a", "b", "c")}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	et := MakeTypeRef(pkgRef, 0)
	tref := MakeCompoundTypeRef(ListKind, et)
	v := NewList(Enum{0, et}, Enum{1, et}, Enum{2, et})
	v.t = tref

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
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
	v.t = tref

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)

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

	typeDef := MakeStructTypeRef("S", []Field{
		Field{"x", MakePrimitiveTypeRef(Int32Kind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	listTypeRef := MakeCompoundTypeRef(ListKind, MakePrimitiveTypeRef(ValueKind))
	structTypeRef := MakeTypeRef(pkgRef, 0)
	v := NewList(NewStruct(structTypeRef, typeDef, structData{"x": Int32(42)}))
	v.t = listTypeRef

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, ValueKind, []interface{}{UnresolvedKind, pkgRef.String(), int16(0), int32(42)}}, w.toArray())
}

func TestWriteListOfValueWithTypeRefs(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]Type{
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
	v.t = tref

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, ValueKind, []interface{}{
		BoolKind, true,
		TypeRefKind, Int32Kind,
		TypeRefKind, TypeRefKind,
		TypeRefKind, UnresolvedKind, pkgRef.String(), int16(0),
	}}, w.toArray())
}

type testRef struct {
	Value
	t Type
}

func (r testRef) Type() Type {
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

	test := func(expected []interface{}, v Type) {
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
	v.t = tref

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, TypeRefKind, []interface{}{BoolKind, EnumKind, "E", []interface{}{"a", "b", "c"}, StringKind}}, w.toArray())
}

func TestWritePackage(t *testing.T) {
	cs := chunks.NewMemoryStore()
	pkg := NewPackage([]Type{
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
	// 	Types: List(Type)
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
	v := Package{[]Type{setTref}, []ref.Ref{r}, &ref.Ref{}}

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{PackageKind, []interface{}{SetKind, []interface{}{UInt32Kind}}, []interface{}{r.String()}}, w.toArray())
}
