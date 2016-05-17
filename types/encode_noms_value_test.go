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

	v := NewList(Number(0), Number(1), Number(2), Number(3))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{ListKind, NumberKind, false, []interface{}{NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"}}, w.toArray())
}

func TestWriteListOfList(t *testing.T) {
	assert := assert.New(t)

	l1 := NewList(Number(0))
	l2 := NewList(Number(1), Number(2), Number(3))
	v := NewList(l1, l2)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	// List<List<Number>([[0], [1, 2, 3]])
	assert.EqualValues([]interface{}{ListKind, ListKind, NumberKind, false, []interface{}{
		ListKind, NumberKind, false, []interface{}{NumberKind, "0"},
		ListKind, NumberKind, false, []interface{}{NumberKind, "1", NumberKind, "2", NumberKind, "3"}}}, w.toArray())
}

func TestWriteSet(t *testing.T) {
	assert := assert.New(t)

	v := NewSet(Number(3), Number(1), Number(2), Number(0))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{SetKind, NumberKind, false, []interface{}{NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"}}, w.toArray())
}

func TestWriteSetOfSet(t *testing.T) {
	assert := assert.New(t)

	v := NewSet(NewSet(Number(0)), NewSet(Number(1), Number(2), Number(3)))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{SetKind, SetKind, NumberKind, false, []interface{}{
		SetKind, NumberKind, false, []interface{}{NumberKind, "1", NumberKind, "2", NumberKind, "3"},
		SetKind, NumberKind, false, []interface{}{NumberKind, "0"}}}, w.toArray())
}

func TestWriteMap(t *testing.T) {
	assert := assert.New(t)

	v := newMap(newMapLeafSequence(nil, mapEntry{NewString("a"), Bool(false)}, mapEntry{NewString("b"), Bool(true)}))

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{MapKind, StringKind, BoolKind, false, []interface{}{
		StringKind, "a", BoolKind, false, StringKind, "b", BoolKind, true}}, w.toArray())
}

func TestWriteMapOfMap(t *testing.T) {
	assert := assert.New(t)

	// Map<Map<String, Number>, Set<Bool>>
	// { {"a": 0}: {true} }
	v := NewMap(NewMap(NewString("a"), Number(0)), NewSet(Bool(true)))

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

	v := newBlob(newBlobMetaSequence([]metaTuple{
		newMetaTuple(Number(20), nil, constructRef(RefOfBlobType, r1, 11), 20),
		newMetaTuple(Number(40), nil, constructRef(RefOfBlobType, r2, 22), 40),
		newMetaTuple(Number(60), nil, constructRef(RefOfBlobType, r3, 33), 60),
	}, NewTestValueStore()))
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)

	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{
		BlobKind, true, []interface{}{
			RefKind, BlobKind, r1.String(), "11", NumberKind, "20", "20",
			RefKind, BlobKind, r2.String(), "22", NumberKind, "40", "40",
			RefKind, BlobKind, r3.String(), "33", NumberKind, "60", "60",
		},
	}, w.toArray())
}

func TestWriteEmptyStruct(t *testing.T) {
	assert := assert.New(t)

	v := NewStruct("S", nil)
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{StructKind, "S", []interface{}{}}, w.toArray())
}

func TestWriteStruct(t *testing.T) {
	assert := assert.New(t)

	v := NewStruct("S", structData{"x": Number(42), "b": Bool(true)})
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{StructKind, "S", []interface{}{"b", BoolKind, "x", NumberKind}, BoolKind, true, NumberKind, "42"}, w.toArray())
}

func TestWriteStructWithList(t *testing.T) {
	assert := assert.New(t)

	// struct S {l: List<String>}({l: ["a", "b"]})
	v := NewStruct("S", structData{"l": NewList(NewString("a"), NewString("b"))})
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{StructKind, "S", []interface{}{"l", ListKind, StringKind},
		ListKind, StringKind, false, []interface{}{StringKind, "a", StringKind, "b"}}, w.toArray())

	// struct S {l: List<>}({l: []})
	v = NewStruct("S", structData{"l": NewList()})
	w = newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{StructKind, "S", []interface{}{"l", ListKind, UnionKind, uint16(0)},
		ListKind, UnionKind, uint16(0), false, []interface{}{}}, w.toArray())
}

func TestWriteStructWithStruct(t *testing.T) {
	assert := assert.New(t)

	// struct S2 {
	//   x: Number
	// }
	// struct S {
	//   s: S2
	// }

	// {s: {x: 42}}
	v := NewStruct("S", structData{"s": NewStruct("S2", structData{"x": Number(42)})})
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{StructKind, "S", []interface{}{"s", StructKind, "S2", []interface{}{"x", NumberKind}}, StructKind, "S2", []interface{}{"x", NumberKind}, NumberKind, "42"}, w.toArray())
}

func TestWriteStructWithBlob(t *testing.T) {
	assert := assert.New(t)

	b := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	v := NewStruct("S", structData{"b": b})
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{StructKind, "S", []interface{}{"b", BlobKind}, BlobKind, false, "AAE="}, w.toArray())
}

func TestWriteCompoundList(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()

	list1 := newList(newListLeafSequence(cs, Number(0)))
	list2 := newList(newListLeafSequence(cs, Number(1), Number(2), Number(3)))
	cl := newList(newListMetaSequence([]metaTuple{
		newMetaTuple(Number(1), list1, NewRef(list1), 1),
		newMetaTuple(Number(4), list2, NewRef(list2), 4),
	}, cs))

	w := newJSONArrayWriter(cs)
	w.writeValue(cl)
	assert.EqualValues([]interface{}{
		ListKind, NumberKind, true, []interface{}{
			RefKind, ListKind, NumberKind, list1.Ref().String(), "1", NumberKind, "1", "1",
			RefKind, ListKind, NumberKind, list2.Ref().String(), "1", NumberKind, "4", "4",
		},
	}, w.toArray())
}

func TestWriteCompoundSet(t *testing.T) {
	assert := assert.New(t)
	cs := NewTestValueStore()
	set1 := newSet(newSetLeafSequence(cs, Number(0), Number(1)))
	set2 := newSet(newSetLeafSequence(cs, Number(2), Number(3), Number(4)))
	cl := newSet(newSetMetaSequence([]metaTuple{
		newMetaTuple(Number(1), set1, NewRef(set1), 2),
		newMetaTuple(Number(4), set2, NewRef(set2), 3),
	}, cs))

	w := newJSONArrayWriter(cs)
	w.writeValue(cl)
	assert.EqualValues([]interface{}{
		SetKind, NumberKind, true, []interface{}{
			RefKind, SetKind, NumberKind, set1.Ref().String(), "1", NumberKind, "1", "2",
			RefKind, SetKind, NumberKind, set2.Ref().String(), "1", NumberKind, "4", "3",
		},
	}, w.toArray())
}

func TestWriteListOfValue(t *testing.T) {
	assert := assert.New(t)

	v := NewList(
		NewString("0"),
		Number(1),
		NewString("2"),
		Bool(true),
	)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)

	assert.EqualValues([]interface{}{ListKind, UnionKind, uint16(3), BoolKind, NumberKind, StringKind, false, []interface{}{
		StringKind, "0",
		NumberKind, "1",
		StringKind, "2",
		BoolKind, true,
	}}, w.toArray())
}

func TestWriteListOfValueWithStruct(t *testing.T) {
	assert := assert.New(t)

	v := NewList(NewStruct("S", structData{"x": Number(42)}))
	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{ListKind,
		StructKind, "S", []interface{}{"x", NumberKind}, false, []interface{}{
			StructKind, "S", []interface{}{"x", NumberKind}, NumberKind, "42"}}, w.toArray())
}

func TestWriteListOfValueWithType(t *testing.T) {
	assert := assert.New(t)

	structType := MakeStructType("S", TypeMap{
		"x": NumberType,
	})

	v := NewList(
		Bool(true),
		NumberType,
		TypeType,
		structType,
	)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{ListKind, UnionKind, uint16(2), BoolKind, TypeKind, false, []interface{}{
		BoolKind, true,
		TypeKind, NumberKind,
		TypeKind, TypeKind,
		TypeKind, StructKind, "S", []interface{}{"x", NumberKind}}}, w.toArray())
}

func TestWriteRef(t *testing.T) {
	assert := assert.New(t)

	typ := MakeRefType(NumberType)
	r := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	v := constructRef(typ, r, 4)

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{RefKind, NumberKind, r.String(), "4"}, w.toArray())
}

func TestWriteTypeValue(t *testing.T) {
	assert := assert.New(t)

	test := func(expected []interface{}, v *Type) {
		w := newJSONArrayWriter(NewTestValueStore())
		w.writeValue(v)
		assert.EqualValues(expected, w.toArray())
	}

	test([]interface{}{TypeKind, NumberKind}, NumberType)
	test([]interface{}{TypeKind, ListKind, BoolKind}, MakeListType(BoolType))
	test([]interface{}{TypeKind, MapKind, BoolKind, StringKind}, MakeMapType(BoolType, StringType))

	test([]interface{}{TypeKind, StructKind, "S", []interface{}{"v", ValueKind, "x", NumberKind}},
		MakeStructType("S", TypeMap{
			"x": NumberType,
			"v": ValueType,
		}))

	test([]interface{}{TypeKind, UnionKind, uint16(0)}, MakeUnionType())
	test([]interface{}{TypeKind, UnionKind, uint16(2), NumberKind, StringKind}, MakeUnionType(NumberType, StringType))
	test([]interface{}{TypeKind, ListKind, UnionKind, uint16(0)}, MakeListType(MakeUnionType()))
}

func TestWriteListOfTypes(t *testing.T) {
	assert := assert.New(t)

	v := NewList(BoolType, StringType)

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

	// {v: 42, cs: [{v: 555, cs: []}]}
	v := NewStructWithType(structType, structData{
		"v":  Number(42),
		"cs": NewList(),
	})

	w := newJSONArrayWriter(NewTestValueStore())
	w.writeValue(v)
	assert.EqualValues([]interface{}{
		StructKind, "A6", []interface{}{
			"cs", ListKind, ParentKind, uint8(0),
			"v", NumberKind,
		},
		ListKind, UnionKind, uint16(0), false, []interface{}{},
		NumberKind, "42",
	}, w.toArray())
}

func TestWriteUnionList(t *testing.T) {
	assert := assert.New(t)

	w := newJSONArrayWriter(NewTestValueStore())
	v := NewList(NewString("hi"), Number(42))
	w.writeValue(v)
	assert.Equal([]interface{}{ListKind, UnionKind, uint16(2), NumberKind, StringKind,
		false, []interface{}{StringKind, "hi", NumberKind, "42"}}, w.toArray())
}

func TestWriteEmptyUnionList(t *testing.T) {
	assert := assert.New(t)

	w := newJSONArrayWriter(NewTestValueStore())
	v := NewList()
	w.writeValue(v)
	assert.Equal([]interface{}{ListKind, UnionKind, uint16(0), false, []interface{}{}}, w.toArray())
}
