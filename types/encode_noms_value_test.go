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
		w.writeValue(v)
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
	w.writeValue(NewBlob(bytes.NewBuffer([]byte{0x00, 0x01})))
	assert.EqualValues([]interface{}{BlobKind, false, "AAE="}, w.toArray())
}

func TestWriteList(t *testing.T) {
	assert := assert.New(t)

	typ := MakeListType(NumberType)
	v := NewTypedList(typ, Number(0), Number(1), Number(2), Number(3))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{ListKind, NumberKind, false, []interface{}{NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"}}, w.toArray())
}

func TestWriteListOfList(t *testing.T) {
	assert := assert.New(t)

	it := MakeListType(NumberType)
	typ := MakeListType(it)
	l1 := NewTypedList(it, Number(0))
	l2 := NewTypedList(it, Number(1), Number(2), Number(3))
	v := NewTypedList(typ, l1, l2)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	// List<List<Number>([[0], [1, 2, 3]])
	assert.EqualValues([]interface{}{ListKind, ListKind, NumberKind, false, []interface{}{
		ListKind, NumberKind, false, []interface{}{NumberKind, "0"},
		ListKind, NumberKind, false, []interface{}{NumberKind, "1", NumberKind, "2", NumberKind, "3"}}}, w.toArray())
}

func TestWriteSet(t *testing.T) {
	assert := assert.New(t)

	typ := MakeSetType(NumberType)
	v := NewTypedSet(typ, Number(3), Number(1), Number(2), Number(0))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{SetKind, NumberKind, false, []interface{}{NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"}}, w.toArray())
}

func TestWriteSetOfSet(t *testing.T) {
	assert := assert.New(t)

	st := MakeSetType(NumberType)
	typ := MakeSetType(st)
	v := NewTypedSet(typ, NewTypedSet(st, Number(0)), NewTypedSet(st, Number(1), Number(2), Number(3)))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{SetKind, SetKind, NumberKind, false, []interface{}{
		SetKind, NumberKind, false, []interface{}{NumberKind, "1", NumberKind, "2", NumberKind, "3"},
		SetKind, NumberKind, false, []interface{}{NumberKind, "0"}}}, w.toArray())
}

func TestWriteMap(t *testing.T) {
	assert := assert.New(t)

	typ := MakeMapType(StringType, BoolType)
	v := newMapLeaf(typ, mapEntry{NewString("a"), Bool(false)}, mapEntry{NewString("b"), Bool(true)})

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{MapKind, StringKind, BoolKind, false, []interface{}{
		StringKind, "a", BoolKind, false, StringKind, "b", BoolKind, true}}, w.toArray())
}

func TestWriteMapOfMap(t *testing.T) {
	assert := assert.New(t)

	// Map<Map<String, Number>, Set<Bool>>
	kt := MakeMapType(StringType, NumberType)
	vt := MakeSetType(BoolType)
	typ := MakeMapType(kt, vt)
	// { {"a": 0}: {true} }
	v := NewTypedMap(typ, NewTypedMap(kt, NewString("a"), Number(0)), NewTypedSet(vt, Bool(true)))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MapKind, MapKind, StringKind, NumberKind, SetKind, BoolKind, false, []interface{}{
		MapKind, StringKind, NumberKind, false, []interface{}{StringKind, "a", NumberKind, "0"},
		SetKind, BoolKind, false, []interface{}{BoolKind, true}}}, w.toArray())
}

func TestWriteCompoundBlob(t *testing.T) {
	assert := assert.New(t)

	r1 := ref.Parse("sha1-0000000000000000000000000000000000000001")
	r2 := ref.Parse("sha1-0000000000000000000000000000000000000002")
	r3 := ref.Parse("sha1-0000000000000000000000000000000000000003")

	v := newCompoundBlob([]metaTuple{
		newMetaTuple(Number(20), nil, NewTypedRef(RefOfBlobType, r1), 20),
		newMetaTuple(Number(40), nil, NewTypedRef(RefOfBlobType, r2), 40),
		newMetaTuple(Number(60), nil, NewTypedRef(RefOfBlobType, r3), 60),
	}, NewTestValueStore())
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)

	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{BlobKind, true, []interface{}{r1.String(), NumberKind, "20", "20", r2.String(), NumberKind, "40", "40", r3.String(), NumberKind, "60", "60"}}, w.toArray())
}

func TestWriteEmptyStruct(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S", TypeMap{})
	v := NewStruct(typ, nil)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{StructKind, "S", []interface{}{}}, w.toArray())
}

func TestWriteStruct(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S", TypeMap{
		"x": NumberType,
		"b": BoolType,
	})
	v := NewStruct(typ, structData{"x": Number(42), "b": Bool(true)})

	// struct S {x: Number, b: Bool}({x: 42, b: true})
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{StructKind, "S", []interface{}{"b", BoolKind, "x", NumberKind}, BoolKind, true, NumberKind, "42"}, w.toArray())
}

func TestWriteStructWithList(t *testing.T) {
	assert := assert.New(t)

	listType := MakeListType(StringType)
	typ := MakeStructType("S", TypeMap{
		"l": listType,
	})

	// struct S {l: List<String>}({l: ["a", "b"]})
	v := NewStruct(typ, structData{"l": NewTypedList(listType, NewString("a"), NewString("b"))})
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{StructKind, "S", []interface{}{"l", ListKind, StringKind},
		ListKind, StringKind, false, []interface{}{StringKind, "a", StringKind, "b"}}, w.toArray())

	// struct S {l: List<String>}({l: []})
	v = NewStruct(typ, structData{"l": NewTypedList(listType)})
	w = newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{StructKind, "S", []interface{}{"l", ListKind, StringKind},
		ListKind, StringKind, false, []interface{}{}}, w.toArray())
}

func TestWriteStructWithStruct(t *testing.T) {
	assert := assert.New(t)

	// struct S2 {
	//   x: Number
	// }
	// struct S {
	//   s: S2
	// }

	s2Type := MakeStructType("S2", TypeMap{
		"x": NumberType,
	})
	sType := MakeStructType("S", TypeMap{
		"s": MakeStructType("S2", TypeMap{
			"x": NumberType,
		}),
	})

	// {s: {x: 42}}
	v := NewStruct(sType, structData{"s": NewStruct(s2Type, structData{"x": Number(42)})})
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{StructKind, "S", []interface{}{"s", StructKind, "S2", []interface{}{"x", NumberKind}}, StructKind, "S2", []interface{}{"x", NumberKind}, NumberKind, "42"}, w.toArray())
}

func TestWriteStructWithBlob(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S", TypeMap{
		"b": BlobType,
	})
	b := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	v := NewStruct(typ, structData{"b": b})

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{StructKind, "S", []interface{}{"b", BlobKind}, BlobKind, false, "AAE="}, w.toArray())
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
	w.writeValue(cl)
	assert.EqualValues([]interface{}{ListKind, NumberKind, true, []interface{}{leaf1.Ref().String(), NumberKind, "1", "1", leaf2.Ref().String(), NumberKind, "4", "4"}}, w.toArray())
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
	w.writeValue(cl)
	assert.EqualValues([]interface{}{SetKind, NumberKind, true, []interface{}{leaf1.Ref().String(), NumberKind, "1", "2", leaf2.Ref().String(), NumberKind, "4", "3"}}, w.toArray())
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
	w.writeValue(v)

	assert.EqualValues([]interface{}{ListKind, ValueKind, false, []interface{}{
		BoolKind, true,
		NumberKind, "1",
		StringKind, "hi",
		BlobKind, false, "AQ==",
	}}, w.toArray())
}

func TestWriteListOfValueWithStruct(t *testing.T) {
	assert := assert.New(t)

	structType := MakeStructType("S", TypeMap{
		"x": NumberType,
	})
	listType := MakeListType(ValueType)
	v := NewTypedList(listType, NewStruct(structType, structData{"x": Number(42)}))

	// List<struct S {x: Number}>([{x: 42}])
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{ListKind, ValueKind, false, []interface{}{
		StructKind, "S", []interface{}{"x", NumberKind}, NumberKind, "42"}}, w.toArray())
}

func TestWriteListOfValueWithType(t *testing.T) {
	assert := assert.New(t)

	structType := MakeStructType("S", TypeMap{
		"x": NumberType,
	})

	typ := MakeListType(ValueType)
	v := NewTypedList(typ,
		Bool(true),
		NumberType,
		TypeType,
		structType,
	)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{ListKind, ValueKind, false, []interface{}{
		BoolKind, true,
		TypeKind, NumberKind,
		TypeKind, TypeKind,
		TypeKind, StructKind, "S", []interface{}{"x", NumberKind}}}, w.toArray())
}

func TestWriteRef(t *testing.T) {
	assert := assert.New(t)

	typ := MakeRefType(NumberType)
	r := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	v := NewTypedRef(typ, r)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{RefKind, NumberKind, r.String()}, w.toArray())
}

func TestWriteTypeValue(t *testing.T) {
	assert := assert.New(t)

	test := func(expected []interface{}, v *Type) {
		w := newJSONArrayWriter(NewTestValueStore())
		w.writeValue(v)
		assert.EqualValues(expected, w.toArray())
	}

	test([]interface{}{TypeKind, NumberKind}, NumberType)
	test([]interface{}{TypeKind, ListKind, []interface{}{BoolKind}},
		MakeListType(BoolType))
	test([]interface{}{TypeKind, MapKind, []interface{}{BoolKind, StringKind}},
		MakeMapType(BoolType, StringType))

	test([]interface{}{TypeKind, StructKind, "S", []interface{}{"v", ValueKind, "x", NumberKind}},
		MakeStructType("S", TypeMap{
			"x": NumberType,
			"v": ValueType,
		}))
}

func TestWriteListOfTypes(t *testing.T) {
	assert := assert.New(t)

	typ := MakeListType(TypeType)
	v := NewTypedList(typ, BoolType, StringType)

	// List<Type>([Bool, String])
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{ListKind, TypeKind, false, []interface{}{TypeKind, BoolKind, TypeKind, StringKind}}, w.toArray())
}

func TestWriteRecursiveStruct(t *testing.T) {
	assert := assert.New(t)

	// struct A6 {
	//   cs: List<A6>
	//   v: Number
	// }

	structType := MakeStructType("A6", TypeMap{
		"v":  NumberType,
		"cs": nil,
	})
	listType := MakeListType(structType)
	// Mutate...

	structType.Desc.(StructDesc).Fields["cs"] = listType

	NewTypedList(listType)

	// {v: 42, cs: [{v: 555, cs: []}]}
	v := NewStruct(structType, structData{
		"v": Number(42),
		"cs": NewTypedList(listType, NewStruct(structType, structData{
			"v":  Number(555),
			"cs": NewTypedList(listType),
		})),
	})

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{
		StructKind, "A6", []interface{}{
			"cs", ListKind, ParentKind, uint8(0),
			"v", NumberKind,
		},
		ListKind, StructKind, "A6", []interface{}{
			"cs", ListKind, ParentKind, uint8(0),
			"v", NumberKind,
		}, false, []interface{}{
			StructKind, "A6", []interface{}{
				"cs", ListKind, ParentKind, uint8(0),
				"v", NumberKind,
			}, ListKind, StructKind, "A6", []interface{}{
				"cs", ListKind, ParentKind, uint8(0),
				"v", NumberKind,
			}, false, []interface{}{}, NumberKind, "555",
		}, NumberKind, "42",
	}, w.toArray())

}
