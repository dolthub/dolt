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

	f(NumberKind, Number(0), "0")
	f(NumberKind, Number(1e18), "1000000000000000000")
	f(NumberKind, Number(1e19), "10000000000000000000")
	f(NumberKind, Number(float64(1e19)), "10000000000000000000")
	f(NumberKind, Number(float64(1e20)), "1e+20")

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

	typ := MakeListType(NumberType)
	v := NewTypedList(typ, Number(0), Number(1), Number(2), Number(3))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, NumberKind, false, []interface{}{"0", "1", "2", "3"}}, w.toArray())
}

func TestWriteListOfList(t *testing.T) {
	assert := assert.New(t)

	it := MakeListType(NumberType)
	typ := MakeListType(it)
	l1 := NewTypedList(it, Number(0))
	l2 := NewTypedList(it, Number(1), Number(2), Number(3))
	v := NewTypedList(typ, l1, l2)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, ListKind, NumberKind, false, []interface{}{false, []interface{}{"0"}, false, []interface{}{"1", "2", "3"}}}, w.toArray())
}

func TestWriteSet(t *testing.T) {
	assert := assert.New(t)

	typ := MakeSetType(NumberType)
	v := NewTypedSet(typ, Number(3), Number(1), Number(2), Number(0))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{SetKind, NumberKind, false, []interface{}{"0", "1", "2", "3"}}, w.toArray())
}

func TestWriteSetOfSet(t *testing.T) {
	assert := assert.New(t)

	st := MakeSetType(NumberType)
	typ := MakeSetType(st)
	v := NewTypedSet(typ, NewTypedSet(st, Number(0)), NewTypedSet(st, Number(1), Number(2), Number(3)))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{SetKind, SetKind, NumberKind, false, []interface{}{false, []interface{}{"1", "2", "3"}, false, []interface{}{"0"}}}, w.toArray())
}

func TestWriteMap(t *testing.T) {
	assert := assert.New(t)

	typ := MakeMapType(StringType, BoolType)
	v := newMapLeaf(typ, mapEntry{NewString("a"), Bool(false)}, mapEntry{NewString("b"), Bool(true)})

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{MapKind, StringKind, BoolKind, false, []interface{}{"a", false, "b", true}}, w.toArray())
}

func TestWriteMapOfMap(t *testing.T) {
	assert := assert.New(t)

	kt := MakeMapType(StringType, NumberType)
	vt := MakeSetType(BoolType)
	typ := MakeMapType(kt, vt)
	v := NewTypedMap(typ, NewTypedMap(kt, NewString("a"), Number(0)), NewTypedSet(vt, Bool(true)))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MapKind, MapKind, StringKind, NumberKind, SetKind, BoolKind, false, []interface{}{false, []interface{}{"a", "0"}, false, []interface{}{true}}}, w.toArray())
}

func TestWriteCompoundBlob(t *testing.T) {
	assert := assert.New(t)

	r1 := ref.Parse("sha1-0000000000000000000000000000000000000001")
	r2 := ref.Parse("sha1-0000000000000000000000000000000000000002")
	r3 := ref.Parse("sha1-0000000000000000000000000000000000000003")

	v := newCompoundBlob([]metaTuple{
		newMetaTuple(Number(20), nil, NewTypedRef(MakeRefType(typeForBlob), r1), 20),
		newMetaTuple(Number(40), nil, NewTypedRef(MakeRefType(typeForBlob), r2), 40),
		newMetaTuple(Number(60), nil, NewTypedRef(MakeRefType(typeForBlob), r3), 60),
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
		Field{"x", NumberType, false},
		Field{"b", BoolType, false},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	v := NewStruct(typ, typeDef, structData{"x": Number(42), "b": Bool(true)})

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "0", "42", true}, w.toArray())
}

func TestWriteStructOptionalField(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S", []Field{
		Field{"x", NumberType, true},
		Field{"b", BoolType, false},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	v := NewStruct(typ, typeDef, structData{"x": Number(42), "b": Bool(true)})

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
		Field{"x", NumberType, false},
	}, []Field{
		Field{"b", BoolType, false},
		Field{"s", StringType, false},
	})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	v := NewStruct(typ, typeDef, structData{"x": Number(42), "s": NewString("hi")})

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "0", "42", "1", "hi"}, w.toArray())

	v = NewStruct(typ, typeDef, structData{"x": Number(42), "b": Bool(true)})

	w = newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "0", "42", "0", true}, w.toArray())
}

func TestWriteStructWithList(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S", []Field{
		Field{"l", MakeListType(StringType), false},
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
		Field{"x", NumberType, false},
	}, []Field{})
	sTypeDef := MakeStructType("S", []Field{
		Field{"s", MakeType(ref.Ref{}, 0), false},
	}, []Field{})
	pkg := NewPackage([]*Type{s2TypeDef, sTypeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	s2Type := MakeType(pkgRef, 0)
	sType := MakeType(pkgRef, 1)

	v := NewStruct(sType, sTypeDef, structData{"s": NewStruct(s2Type, s2TypeDef, structData{"x": Number(42)})})
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), "1", "42"}, w.toArray())
}

func TestWriteStructWithBlob(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S", []Field{
		Field{"b", BlobType, false},
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

	ltr := MakeListType(NumberType)
	leaf1 := newListLeaf(ltr, Number(0))
	leaf2 := newListLeaf(ltr, Number(1), Number(2), Number(3))
	cl := buildCompoundList([]metaTuple{
		newMetaTuple(Number(1), leaf1, Ref{}, 1),
		newMetaTuple(Number(4), leaf2, Ref{}, 4),
	}, ltr, NewTestValueStore())

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(cl)
	assert.EqualValues([]interface{}{ListKind, NumberKind, true, []interface{}{leaf1.Ref().String(), "1", "1", leaf2.Ref().String(), "4", "4"}}, w.toArray())
}

func TestWriteCompoundSet(t *testing.T) {
	assert := assert.New(t)

	ltr := MakeSetType(NumberType)
	leaf1 := newSetLeaf(ltr, Number(0), Number(1))
	leaf2 := newSetLeaf(ltr, Number(2), Number(3), Number(4))
	cl := buildCompoundSet([]metaTuple{
		newMetaTuple(Number(1), leaf1, Ref{}, 2),
		newMetaTuple(Number(4), leaf2, Ref{}, 3),
	}, ltr, NewTestValueStore())

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(cl)
	assert.EqualValues([]interface{}{SetKind, NumberKind, true, []interface{}{leaf1.Ref().String(), "1", "2", leaf2.Ref().String(), "4", "3"}}, w.toArray())
}

func TestWriteListOfValue(t *testing.T) {
	assert := assert.New(t)

	typ := MakeListType(ValueType)
	blob := NewBlob(bytes.NewBuffer([]byte{0x01}))
	v := NewTypedList(typ,
		Bool(true),
		Number(1),
		NewString("hi"),
		blob,
	)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)

	assert.EqualValues([]interface{}{ListKind, ValueKind, false, []interface{}{
		BoolKind, true,
		NumberKind, "1",
		StringKind, "hi",
		BlobKind, false, "AQ==",
	}}, w.toArray())
}

func TestWriteListOfValueWithStruct(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S", []Field{
		Field{"x", NumberType, false},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	listType := MakeListType(ValueType)
	structType := MakeType(pkgRef, 0)
	v := NewTypedList(listType, NewStruct(structType, typeDef, structData{"x": Number(42)}))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, ValueKind, false, []interface{}{UnresolvedKind, pkgRef.String(), "0", "42"}}, w.toArray())
}

func TestWriteListOfValueWithType(t *testing.T) {
	assert := assert.New(t)

	pkg := NewPackage([]*Type{
		MakeStructType("S", []Field{
			Field{"x", NumberType, false},
		}, []Field{})}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	typ := MakeListType(ValueType)
	v := NewTypedList(typ,
		Bool(true),
		NumberType,
		TypeType,
		MakeType(pkgRef, 0),
	)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, ValueKind, false, []interface{}{
		BoolKind, true,
		TypeKind, NumberKind,
		TypeKind, TypeKind,
		TypeKind, UnresolvedKind, pkgRef.String(), "0",
	}}, w.toArray())
}

func TestWriteRef(t *testing.T) {
	assert := assert.New(t)

	typ := MakeRefType(NumberType)
	r := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	v := NewTypedRef(typ, r)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{RefKind, NumberKind, r.String()}, w.toArray())
}

func TestWriteTypeValue(t *testing.T) {
	assert := assert.New(t)

	test := func(expected []interface{}, v *Type) {
		w := newJSONArrayWriter(NewTestValueStore())
		w.writeTopLevelValue(v)
		assert.EqualValues(expected, w.toArray())
	}

	test([]interface{}{TypeKind, NumberKind}, NumberType)
	test([]interface{}{TypeKind, ListKind, []interface{}{BoolKind}},
		MakeListType(BoolType))
	test([]interface{}{TypeKind, MapKind, []interface{}{BoolKind, StringKind}},
		MakeMapType(BoolType, StringType))

	test([]interface{}{TypeKind, StructKind, "S", []interface{}{"x", NumberKind, false, "v", ValueKind, true}, []interface{}{}},
		MakeStructType("S", []Field{
			Field{"x", NumberType, false},
			Field{"v", ValueType, true},
		}, []Field{}))

	test([]interface{}{TypeKind, StructKind, "S", []interface{}{}, []interface{}{"x", NumberKind, false, "v", ValueKind, false}},
		MakeStructType("S", []Field{}, []Field{
			Field{"x", NumberType, false},
			Field{"v", ValueType, false},
		}))

	pkgRef := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	test([]interface{}{TypeKind, UnresolvedKind, pkgRef.String(), "123"},
		MakeType(pkgRef, 123))

	test([]interface{}{TypeKind, StructKind, "S", []interface{}{"e", UnresolvedKind, pkgRef.String(), "123", false, "x", NumberKind, false}, []interface{}{}},
		MakeStructType("S", []Field{
			Field{"e", MakeType(pkgRef, 123), false},
			Field{"x", NumberType, false},
		}, []Field{}))

	test([]interface{}{TypeKind, UnresolvedKind, ref.Ref{}.String(), "-1", "ns", "n"},
		MakeUnresolvedType("ns", "n"))
}

func TestWriteListOfTypes(t *testing.T) {
	assert := assert.New(t)

	typ := MakeListType(TypeType)
	v := NewTypedList(typ, BoolType, StringType)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, TypeKind, false, []interface{}{BoolKind, StringKind}}, w.toArray())
}

func TestWritePackage(t *testing.T) {
	assert := assert.New(t)

	setTref := MakeSetType(NumberType)
	r := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	v := Package{[]*Type{setTref}, []ref.Ref{r}, &ref.Ref{}}

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{PackageKind, []interface{}{SetKind, []interface{}{NumberKind}}, []interface{}{r.String()}}, w.toArray())
}
