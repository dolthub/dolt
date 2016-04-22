package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestWritePrimitives(t *testing.T) {
	assert := assert.New(t)

	f := func(k NomsKind, v Value, ex interface{}) {

		w := newJSONArrayWriter(NewTestValueStore())
		w.writeTopLevelValue(v)
		assert.EqualValues([]interface{}{k, ex}, w.toArray())
	}

	f(BoolKind, Bool(true), true)
	f(BoolKind, Bool(false), false)

	f(Uint8Kind, Uint8(0), "0")
	f(Uint16Kind, Uint16(0), "0")
	f(Uint32Kind, Uint32(0), "0")
	f(Uint64Kind, Uint64(0), "0")
	f(Int8Kind, Int8(0), "0")
	f(Int16Kind, Int16(0), "0")
	f(Int32Kind, Int32(0), "0")
	f(Int64Kind, Int64(0), "0")
	f(Float32Kind, Float32(0), "0")
	f(Float64Kind, Float64(0), "0")

	f(Int64Kind, Int64(1e18), "1000000000000000000")
	f(Uint64Kind, Uint64(1e19), "10000000000000000000")
	f(Float64Kind, Float64(float64(1e19)), "10000000000000000000")
	f(Float64Kind, Float64(float64(1e20)), "1e+20")

	f(StringKind, NewString("hi"), "hi")
}

func TestWriteSimpleBlob(t *testing.T) {
	assert := assert.New(t)
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(NewBlob(bytes.NewBuffer([]byte{0x00, 0x01})))
	assert.EqualValues([]interface{}{BlobKind, false, "AAE="}, w.toArray())
}

func TestWriteList(t *testing.T) {
	assert := assert.New(t)

	typ := MakeCompoundType(ListKind, MakePrimitiveType(Int32Kind))
	v := NewTypedList(typ, Int32(0), Int32(1), Int32(2), Int32(3))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, Int32Kind, false, []interface{}{"0", "1", "2", "3"}}, w.toArray())
}

func TestWriteListOfList(t *testing.T) {
	assert := assert.New(t)

	it := MakeCompoundType(ListKind, MakePrimitiveType(Int16Kind))
	typ := MakeCompoundType(ListKind, it)
	l1 := NewTypedList(it, Int16(0))
	l2 := NewTypedList(it, Int16(1), Int16(2), Int16(3))
	v := NewTypedList(typ, l1, l2)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, ListKind, Int16Kind, false, []interface{}{false, []interface{}{"0"}, false, []interface{}{"1", "2", "3"}}}, w.toArray())
}

func TestWriteSet(t *testing.T) {
	assert := assert.New(t)

	typ := MakeCompoundType(SetKind, MakePrimitiveType(Uint32Kind))
	v := NewTypedSet(typ, Uint32(3), Uint32(1), Uint32(2), Uint32(0))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{SetKind, Uint32Kind, false, []interface{}{"0", "1", "2", "3"}}, w.toArray())
}

func TestWriteSetOfSet(t *testing.T) {
	assert := assert.New(t)

	st := MakeCompoundType(SetKind, MakePrimitiveType(Int32Kind))
	typ := MakeCompoundType(SetKind, st)
	v := NewTypedSet(typ, NewTypedSet(st, Int32(0)), NewTypedSet(st, Int32(1), Int32(2), Int32(3)))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{SetKind, SetKind, Int32Kind, false, []interface{}{false, []interface{}{"1", "2", "3"}, false, []interface{}{"0"}}}, w.toArray())
}

func TestWriteMap(t *testing.T) {
	assert := assert.New(t)

	typ := MakeCompoundType(MapKind, MakePrimitiveType(StringKind), MakePrimitiveType(BoolKind))
	v := newMapLeaf(typ, mapEntry{NewString("a"), Bool(false)}, mapEntry{NewString("b"), Bool(true)})

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{MapKind, StringKind, BoolKind, false, []interface{}{"a", false, "b", true}}, w.toArray())
}

func TestWriteMapOfMap(t *testing.T) {
	assert := assert.New(t)

	kt := MakeCompoundType(MapKind, MakePrimitiveType(StringKind), MakePrimitiveType(Int64Kind))
	vt := MakeCompoundType(SetKind, MakePrimitiveType(BoolKind))
	typ := MakeCompoundType(MapKind, kt, vt)
	v := NewTypedMap(typ, NewTypedMap(kt, NewString("a"), Int64(0)), NewTypedSet(vt, Bool(true)))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MapKind, MapKind, StringKind, Int64Kind, SetKind, BoolKind, false, []interface{}{false, []interface{}{"a", "0"}, false, []interface{}{true}}}, w.toArray())
}

func TestWriteCompoundBlob(t *testing.T) {
	assert := assert.New(t)

	r1 := ref.Parse("sha1-0000000000000000000000000000000000000001")
	r2 := ref.Parse("sha1-0000000000000000000000000000000000000002")
	r3 := ref.Parse("sha1-0000000000000000000000000000000000000003")

	v := newCompoundBlob([]metaTuple{
		newMetaTuple(Uint64(20), nil, NewTypedRef(MakeRefType(typeForBlob), r1), 20),
		newMetaTuple(Uint64(40), nil, NewTypedRef(MakeRefType(typeForBlob), r2), 40),
		newMetaTuple(Uint64(60), nil, NewTypedRef(MakeRefType(typeForBlob), r3), 60),
	}, NewTestValueStore())
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)

	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{BlobKind, true, []interface{}{r1.String(), "20", "20", r2.String(), "40", "40", r3.String(), "60", "60"}}, w.toArray())
}

func TestWriteEmptyStruct(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S", []Field{}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	v := NewStruct(typ, typeDef, nil)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "0"}, w.toArray())
}

func TestWriteStruct(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S", []Field{
		Field{"x", MakePrimitiveType(Int8Kind), false},
		Field{"b", MakePrimitiveType(BoolKind), false},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	v := NewStruct(typ, typeDef, structData{"x": Int8(42), "b": Bool(true)})

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "0", "42", true}, w.toArray())
}

func TestWriteStructOptionalField(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S", []Field{
		Field{"x", MakePrimitiveType(Int8Kind), true},
		Field{"b", MakePrimitiveType(BoolKind), false},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	v := NewStruct(typ, typeDef, structData{"x": Int8(42), "b": Bool(true)})

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "0", true, "42", true}, w.toArray())

	v = NewStruct(typ, typeDef, structData{"b": Bool(true)})

	w = newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "0", false, true}, w.toArray())
}

func TestWriteStructWithUnion(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S", []Field{
		Field{"x", MakePrimitiveType(Int8Kind), false},
	}, []Field{
		Field{"b", MakePrimitiveType(BoolKind), false},
		Field{"s", MakePrimitiveType(StringKind), false},
	})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	v := NewStruct(typ, typeDef, structData{"x": Int8(42), "s": NewString("hi")})

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "0", "42", "1", "hi"}, w.toArray())

	v = NewStruct(typ, typeDef, structData{"x": Int8(42), "b": Bool(true)})

	w = newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "0", "42", "0", true}, w.toArray())
}

func TestWriteStructWithList(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S", []Field{
		Field{"l", MakeCompoundType(ListKind, MakePrimitiveType(StringKind)), false},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)

	v := NewStruct(typ, typeDef, structData{"l": NewList(NewString("a"), NewString("b"))})
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "0", false, []interface{}{"a", "b"}}, w.toArray())

	v = NewStruct(typ, typeDef, structData{"l": NewList()})
	w = newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "0", false, []interface{}{}}, w.toArray())
}

func TestWriteStructWithStruct(t *testing.T) {
	assert := assert.New(t)

	s2TypeDef := MakeStructType("S2", []Field{
		Field{"x", MakePrimitiveType(Int32Kind), false},
	}, []Field{})
	sTypeDef := MakeStructType("S", []Field{
		Field{"s", MakeType(ref.Ref{}, 0), false},
	}, []Field{})
	pkg := NewPackage([]*Type{s2TypeDef, sTypeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	s2Type := MakeType(pkgRef, 0)
	sType := MakeType(pkgRef, 1)

	v := NewStruct(sType, sTypeDef, structData{"s": NewStruct(s2Type, s2TypeDef, structData{"x": Int32(42)})})
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "1", "42"}, w.toArray())
}

func TestWriteStructWithBlob(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S", []Field{
		Field{"b", MakePrimitiveType(BlobKind), false},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	b := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	v := NewStruct(typ, typeDef, structData{"b": b})

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "0", false, "AAE="}, w.toArray())
}

func TestWriteCompoundList(t *testing.T) {
	assert := assert.New(t)

	ltr := MakeCompoundType(ListKind, MakePrimitiveType(Int32Kind))
	leaf1 := newListLeaf(ltr, Int32(0))
	leaf2 := newListLeaf(ltr, Int32(1), Int32(2), Int32(3))
	cl := buildCompoundList([]metaTuple{
		newMetaTuple(Uint64(1), leaf1, Ref{}, 1),
		newMetaTuple(Uint64(4), leaf2, Ref{}, 4),
	}, ltr, NewTestValueStore())

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(cl)
	assert.EqualValues([]interface{}{ListKind, Int32Kind, true, []interface{}{leaf1.Ref().String(), "1", "1", leaf2.Ref().String(), "4", "4"}}, w.toArray())
}

func TestWriteCompoundSet(t *testing.T) {
	assert := assert.New(t)

	ltr := MakeCompoundType(SetKind, MakePrimitiveType(Int32Kind))
	leaf1 := newSetLeaf(ltr, Int32(0), Int32(1))
	leaf2 := newSetLeaf(ltr, Int32(2), Int32(3), Int32(4))
	cl := buildCompoundSet([]metaTuple{
		newMetaTuple(Int32(1), leaf1, Ref{}, 2),
		newMetaTuple(Int32(4), leaf2, Ref{}, 3),
	}, ltr, NewTestValueStore())

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(cl)
	assert.EqualValues([]interface{}{SetKind, Int32Kind, true, []interface{}{leaf1.Ref().String(), "1", "2", leaf2.Ref().String(), "4", "3"}}, w.toArray())
}

func TestWriteListOfValue(t *testing.T) {
	assert := assert.New(t)

	typ := MakeCompoundType(ListKind, MakePrimitiveType(ValueKind))
	blob := NewBlob(bytes.NewBuffer([]byte{0x01}))
	v := NewTypedList(typ,
		Bool(true),
		Uint8(1),
		Uint16(1),
		Uint32(1),
		Uint64(1),
		Int8(1),
		Int16(1),
		Int32(1),
		Int64(1),
		Float32(1),
		Float64(1),
		NewString("hi"),
		blob,
	)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)

	assert.EqualValues([]interface{}{ListKind, ValueKind, false, []interface{}{
		BoolKind, true,
		Uint8Kind, "1",
		Uint16Kind, "1",
		Uint32Kind, "1",
		Uint64Kind, "1",
		Int8Kind, "1",
		Int16Kind, "1",
		Int32Kind, "1",
		Int64Kind, "1",
		Float32Kind, "1",
		Float64Kind, "1",
		StringKind, "hi",
		BlobKind, false, "AQ==",
	}}, w.toArray())
}

func TestWriteListOfValueWithStruct(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S", []Field{
		Field{"x", MakePrimitiveType(Int32Kind), false},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	listType := MakeCompoundType(ListKind, MakePrimitiveType(ValueKind))
	structType := MakeType(pkgRef, 0)
	v := NewTypedList(listType, NewStruct(structType, typeDef, structData{"x": Int32(42)}))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, ValueKind, false, []interface{}{UnresolvedKind, pkgRef.String(), "0", "42"}}, w.toArray())
}

func TestWriteListOfValueWithType(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage([]*Type{
		MakeStructType("S", []Field{
			Field{"x", MakePrimitiveType(Int32Kind), false},
		}, []Field{})}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	typ := MakeCompoundType(ListKind, MakePrimitiveType(ValueKind))
	v := NewTypedList(typ,
		Bool(true),
		MakePrimitiveType(Int32Kind),
		MakePrimitiveType(TypeKind),
		MakeType(pkgRef, 0),
	)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, ValueKind, false, []interface{}{
		BoolKind, true,
		TypeKind, Int32Kind,
		TypeKind, TypeKind,
		TypeKind, UnresolvedKind, pkgRef.String(), "0",
	}}, w.toArray())
}

type testRef struct {
	Value
	t *Type
}

func (r testRef) Type() *Type {
	return r.t
}

func (r testRef) TargetRef() ref.Ref {
	return r.Value.(Ref).TargetRef()
}

func TestWriteRef(t *testing.T) {
	assert := assert.New(t)

	typ := MakeCompoundType(RefKind, MakePrimitiveType(Uint32Kind))
	r := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	v := NewRef(r)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(testRef{Value: v, t: typ})
	assert.EqualValues([]interface{}{RefKind, Uint32Kind, r.String()}, w.toArray())
}

func TestWriteTypeValue(t *testing.T) {
	assert := assert.New(t)

	test := func(expected []interface{}, v *Type) {
		w := newJSONArrayWriter(NewTestValueStore())
		w.writeTopLevelValue(v)
		assert.EqualValues(expected, w.toArray())
	}

	test([]interface{}{TypeKind, Int32Kind}, MakePrimitiveType(Int32Kind))
	test([]interface{}{TypeKind, ListKind, []interface{}{BoolKind}},
		MakeCompoundType(ListKind, MakePrimitiveType(BoolKind)))
	test([]interface{}{TypeKind, MapKind, []interface{}{BoolKind, StringKind}},
		MakeCompoundType(MapKind, MakePrimitiveType(BoolKind), MakePrimitiveType(StringKind)))

	test([]interface{}{TypeKind, StructKind, "S", []interface{}{"x", Int16Kind, false, "v", ValueKind, true}, []interface{}{}},
		MakeStructType("S", []Field{
			Field{"x", MakePrimitiveType(Int16Kind), false},
			Field{"v", MakePrimitiveType(ValueKind), true},
		}, []Field{}))

	test([]interface{}{TypeKind, StructKind, "S", []interface{}{}, []interface{}{"x", Int16Kind, false, "v", ValueKind, false}},
		MakeStructType("S", []Field{}, []Field{
			Field{"x", MakePrimitiveType(Int16Kind), false},
			Field{"v", MakePrimitiveType(ValueKind), false},
		}))

	pkgRef := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	test([]interface{}{TypeKind, UnresolvedKind, pkgRef.String(), "123"},
		MakeType(pkgRef, 123))

	test([]interface{}{TypeKind, StructKind, "S", []interface{}{"e", UnresolvedKind, pkgRef.String(), "123", false, "x", Int64Kind, false}, []interface{}{}},
		MakeStructType("S", []Field{
			Field{"e", MakeType(pkgRef, 123), false},
			Field{"x", MakePrimitiveType(Int64Kind), false},
		}, []Field{}))

	test([]interface{}{TypeKind, UnresolvedKind, ref.Ref{}.String(), "-1", "ns", "n"},
		MakeUnresolvedType("ns", "n"))
}

func TestWriteListOfTypes(t *testing.T) {
	assert := assert.New(t)

	typ := MakeCompoundType(ListKind, MakePrimitiveType(TypeKind))
	v := NewTypedList(typ, MakePrimitiveType(BoolKind), MakePrimitiveType(StringKind))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, TypeKind, false, []interface{}{BoolKind, StringKind}}, w.toArray())
}

func TestWritePackage(t *testing.T) {
	assert := assert.New(t)

	setTref := MakeCompoundType(SetKind, MakePrimitiveType(Uint32Kind))
	r := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	v := Package{[]*Type{setTref}, []ref.Ref{r}, &ref.Ref{}}

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{PackageKind, []interface{}{SetKind, []interface{}{Uint32Kind}}, []interface{}{r.String()}}, w.toArray())
}
