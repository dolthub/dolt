// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/d"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func assertInvalid(tt *testing.T, t *Type, v Value) {
	assert := assert.New(tt)
	assert.Panics(func() {
		assertSubtype(t, v)
	})
}

func assertAll(tt *testing.T, t *Type, v Value) {
	allTypes := []*Type{
		BoolType,
		FloaTType,
		StringType,
		BlobType,
		TypeType,
		ValueType,
		UUIDType,
		IntType,
		UintType,
	}

	for _, at := range allTypes {
		if at == ValueType || t.Equals(at) {
			assertSubtype(at, v)
		} else {
			assertInvalid(tt, at, v)
		}
	}
}

func TestAssertTypePrimitives(t *testing.T) {
	assertSubtype(BoolType, Bool(true))
	assertSubtype(BoolType, Bool(false))
	assertSubtype(FloaTType, Float(42))
	assertSubtype(StringType, String("abc"))
	assertSubtype(UUIDType, UUID(uuid.Must(uuid.NewUUID())))
	assertSubtype(IntType, Int(-1))
	assertSubtype(UintType, Uint(0xffffffffffffffff))

	assertInvalid(t, BoolType, Float(1))
	assertInvalid(t, BoolType, String("abc"))
	assertInvalid(t, FloaTType, Bool(true))
	assertInvalid(t, StringType, UUID(uuid.Must(uuid.NewUUID())))
	assertInvalid(t, UUIDType, String("abs"))
	assertInvalid(t, IntType, Float(-1))
	assertInvalid(t, UintType, Float(500))
}

func TestAssertTypeValue(t *testing.T) {
	vs := newTestValueStore()

	assertSubtype(ValueType, Bool(true))
	assertSubtype(ValueType, Float(1))
	assertSubtype(ValueType, String("abc"))
	l := NewList(vs, Float(0), Float(1), Float(2), Float(3))
	assertSubtype(ValueType, l)
}

func TestAssertTypeBlob(t *testing.T) {
	vs := newTestValueStore()

	blob := NewBlob(vs, bytes.NewBuffer([]byte{0x00, 0x01}))
	assertAll(t, BlobType, blob)
}

func TestAssertTypeList(tt *testing.T) {
	vs := newTestValueStore()

	listOfNumberType := MakeListType(FloaTType)
	l := NewList(vs, Float(0), Float(1), Float(2), Float(3))
	assertSubtype(listOfNumberType, l)
	assertAll(tt, listOfNumberType, l)
	assertSubtype(MakeListType(ValueType), l)
}

func TestAssertTypeMap(tt *testing.T) {
	vs := newTestValueStore()

	mapOfNumberToStringType := MakeMapType(FloaTType, StringType)
	m := NewMap(vs, Float(0), String("a"), Float(2), String("b"))
	assertSubtype(mapOfNumberToStringType, m)
	assertAll(tt, mapOfNumberToStringType, m)
	assertSubtype(MakeMapType(ValueType, ValueType), m)
}

func TestAssertTypeSet(tt *testing.T) {
	vs := newTestValueStore()

	setOfNumberType := MakeSetType(FloaTType)
	s := NewSet(vs, Float(0), Float(1), Float(2), Float(3))
	assertSubtype(setOfNumberType, s)
	assertAll(tt, setOfNumberType, s)
	assertSubtype(MakeSetType(ValueType), s)
}

func TestAssertTypeType(tt *testing.T) {
	t := MakeSetType(FloaTType)
	assertSubtype(TypeType, t)
	assertAll(tt, TypeType, t)
	assertSubtype(ValueType, t)
}

func TestAssertTypeStruct(tt *testing.T) {
	t := MakeStructType("Struct", StructField{"x", BoolType, false})

	v := NewStruct("Struct", StructData{"x": Bool(true)})
	assertSubtype(t, v)
	assertAll(tt, t, v)
	assertSubtype(ValueType, v)
}

func TestAssertTypeUnion(tt *testing.T) {
	vs := newTestValueStore()

	assertSubtype(MakeUnionType(FloaTType), Float(42))
	assertSubtype(MakeUnionType(FloaTType, StringType), Float(42))
	assertSubtype(MakeUnionType(FloaTType, StringType), String("hi"))
	assertSubtype(MakeUnionType(FloaTType, StringType, BoolType), Float(555))
	assertSubtype(MakeUnionType(FloaTType, StringType, BoolType), String("hi"))
	assertSubtype(MakeUnionType(FloaTType, StringType, BoolType), Bool(true))

	lt := MakeListType(MakeUnionType(FloaTType, StringType))
	assertSubtype(lt, NewList(vs, Float(1), String("hi"), Float(2), String("bye")))

	st := MakeSetType(StringType)
	assertSubtype(MakeUnionType(st, FloaTType), Float(42))
	assertSubtype(MakeUnionType(st, FloaTType), NewSet(vs, String("a"), String("b")))

	assertInvalid(tt, MakeUnionType(), Float(42))
	assertInvalid(tt, MakeUnionType(StringType), Float(42))
	assertInvalid(tt, MakeUnionType(StringType, BoolType), Float(42))
	assertInvalid(tt, MakeUnionType(st, StringType), Float(42))
	assertInvalid(tt, MakeUnionType(st, FloaTType), NewSet(vs, Float(1), Float(2)))
}

func TestAssertConcreteTypeIsUnion(tt *testing.T) {
	assert.True(tt, IsSubtype(
		MakeStructTypeFromFields("", FieldMap{}),
		MakeUnionType(
			MakeStructTypeFromFields("", FieldMap{"foo": StringType}),
			MakeStructTypeFromFields("", FieldMap{"bar": StringType}))))

	assert.False(tt, IsSubtype(
		MakeStructTypeFromFields("", FieldMap{}),
		MakeUnionType(MakeStructTypeFromFields("", FieldMap{"foo": StringType}),
			FloaTType)))

	assert.True(tt, IsSubtype(
		MakeUnionType(
			MakeStructTypeFromFields("", FieldMap{"foo": StringType}),
			MakeStructTypeFromFields("", FieldMap{"bar": StringType})),
		MakeUnionType(
			MakeStructTypeFromFields("", FieldMap{"foo": StringType, "bar": StringType}),
			MakeStructTypeFromFields("", FieldMap{"bar": StringType}))))

	assert.False(tt, IsSubtype(
		MakeUnionType(
			MakeStructTypeFromFields("", FieldMap{"foo": StringType}),
			MakeStructTypeFromFields("", FieldMap{"bar": StringType})),
		MakeUnionType(
			MakeStructTypeFromFields("", FieldMap{"foo": StringType, "bar": StringType}),
			FloaTType)))
}

func TestAssertTypeEmptyListUnion(tt *testing.T) {
	vs := newTestValueStore()

	lt := MakeListType(MakeUnionType())
	assertSubtype(lt, NewList(vs))
}

func TestAssertTypeEmptyList(tt *testing.T) {
	vs := newTestValueStore()

	lt := MakeListType(FloaTType)
	assertSubtype(lt, NewList(vs))

	// List<> not a subtype of List<Float>
	assertInvalid(tt, MakeListType(MakeUnionType()), NewList(vs, Float(1)))
}

func TestAssertTypeEmptySet(tt *testing.T) {
	vs := newTestValueStore()

	st := MakeSetType(FloaTType)
	assertSubtype(st, NewSet(vs))

	// Set<> not a subtype of Set<Float>
	assertInvalid(tt, MakeSetType(MakeUnionType()), NewSet(vs, Float(1)))
}

func TestAssertTypeEmptyMap(tt *testing.T) {
	vs := newTestValueStore()

	mt := MakeMapType(FloaTType, StringType)
	assertSubtype(mt, NewMap(vs))

	// Map<> not a subtype of Map<Float, Float>
	assertInvalid(tt, MakeMapType(MakeUnionType(), MakeUnionType()), NewMap(vs, Float(1), Float(2)))
}

func TestAssertTypeStructSubtypeByName(tt *testing.T) {
	namedT := MakeStructType("Name", StructField{"x", FloaTType, false})
	anonT := MakeStructType("", StructField{"x", FloaTType, false})
	namedV := NewStruct("Name", StructData{"x": Float(42)})
	name2V := NewStruct("foo", StructData{"x": Float(42)})
	anonV := NewStruct("", StructData{"x": Float(42)})

	assertSubtype(namedT, namedV)
	assertInvalid(tt, namedT, name2V)
	assertInvalid(tt, namedT, anonV)

	assertSubtype(anonT, namedV)
	assertSubtype(anonT, name2V)
	assertSubtype(anonT, anonV)
}

func TestAssertTypeStructSubtypeExtraFields(tt *testing.T) {
	at := MakeStructType("")
	bt := MakeStructType("", StructField{"x", FloaTType, false})
	ct := MakeStructType("", StructField{"s", StringType, false}, StructField{"x", FloaTType, false})
	av := NewStruct("", StructData{})
	bv := NewStruct("", StructData{"x": Float(1)})
	cv := NewStruct("", StructData{"x": Float(2), "s": String("hi")})

	assertSubtype(at, av)
	assertInvalid(tt, bt, av)
	assertInvalid(tt, ct, av)

	assertSubtype(at, bv)
	assertSubtype(bt, bv)
	assertInvalid(tt, ct, bv)

	assertSubtype(at, cv)
	assertSubtype(bt, cv)
	assertSubtype(ct, cv)
}

func TestAssertTypeStructSubtype(tt *testing.T) {
	vs := newTestValueStore()

	c1 := NewStruct("Commit", StructData{
		"value":   Float(1),
		"parents": NewSet(vs),
	})
	t1 := MakeStructType("Commit",
		StructField{"parents", MakeSetType(MakeUnionType()), false},
		StructField{"value", FloaTType, false},
	)
	assertSubtype(t1, c1)

	t11 := MakeStructType("Commit",
		StructField{"parents", MakeSetType(MakeRefType(MakeCycleType("Commit"))), false},
		StructField{"value", FloaTType, false},
	)
	assertSubtype(t11, c1)

	c2 := NewStruct("Commit", StructData{
		"value":   Float(2),
		"parents": NewSet(vs, NewRef(c1)),
	})
	assertSubtype(t11, c2)
}

func TestAssertTypeCycleUnion(tt *testing.T) {
	// struct S {
	//   x: Cycle<S>,
	//   y: Float,
	// }
	t1 := MakeStructType("S",
		StructField{"x", MakeCycleType("S"), false},
		StructField{"y", FloaTType, false},
	)
	// struct S {
	//   x: Cycle<S>,
	//   y: Float | String,
	// }
	t2 := MakeStructType("S",
		StructField{"x", MakeCycleType("S"), false},
		StructField{"y", MakeUnionType(FloaTType, StringType), false},
	)

	assert.True(tt, IsSubtype(t2, t1))
	assert.False(tt, IsSubtype(t1, t2))

	// struct S {
	//   x: Cycle<S> | Float,
	//   y: Float | String,
	// }
	t3 := MakeStructType("S",
		StructField{"x", MakeUnionType(MakeCycleType("S"), FloaTType), false},
		StructField{"y", MakeUnionType(FloaTType, StringType), false},
	)

	assert.True(tt, IsSubtype(t3, t1))
	assert.False(tt, IsSubtype(t1, t3))

	assert.True(tt, IsSubtype(t3, t2))
	assert.False(tt, IsSubtype(t2, t3))

	// struct S {
	//   x: Cycle<S> | Float,
	//   y: Float,
	// }
	t4 := MakeStructType("S",
		StructField{"x", MakeUnionType(MakeCycleType("S"), FloaTType), false},
		StructField{"y", FloaTType, false},
	)

	assert.True(tt, IsSubtype(t4, t1))
	assert.False(tt, IsSubtype(t1, t4))

	assert.False(tt, IsSubtype(t4, t2))
	assert.False(tt, IsSubtype(t2, t4))

	assert.True(tt, IsSubtype(t3, t4))
	assert.False(tt, IsSubtype(t4, t3))

	// struct B {
	//   b: struct C {
	//     c: Cycle<B>,
	//   },
	// }

	// struct C {
	//   c: struct B {
	//     b: Cycle<C>,
	//   },
	// }

	tb := MakeStructType("A",
		StructField{
			"b",
			MakeStructType("B", StructField{"c", MakeCycleType("A"), false}),
			false,
		},
	)
	tc := MakeStructType("A",
		StructField{
			"c",
			MakeStructType("B", StructField{"b", MakeCycleType("A"), false}),
			false,
		},
	)

	assert.False(tt, IsSubtype(tb, tc))
	assert.False(tt, IsSubtype(tc, tb))
}

func TestIsSubtypeEmptySruct(tt *testing.T) {
	// struct {
	//   a: Float,
	//   b: struct {},
	// }
	t1 := MakeStructType("X",
		StructField{"a", FloaTType, false},
		StructField{"b", EmptyStructType, false},
	)

	// struct {
	//   a: Float,
	// }
	t2 := MakeStructType("X", StructField{"a", FloaTType, false})

	assert.False(tt, IsSubtype(t1, t2))
	assert.True(tt, IsSubtype(t2, t1))
}

func TestIsSubtypeCompoundUnion(tt *testing.T) {
	rt := MakeListType(EmptyStructType)

	st1 := MakeStructType("One", StructField{"a", FloaTType, false})
	st2 := MakeStructType("Two", StructField{"b", StringType, false})
	ct := MakeListType(MakeUnionType(st1, st2))

	assert.True(tt, IsSubtype(rt, ct))
	assert.False(tt, IsSubtype(ct, rt))

	ct2 := MakeListType(MakeUnionType(st1, st2, FloaTType))
	assert.False(tt, IsSubtype(rt, ct2))
	assert.False(tt, IsSubtype(ct2, rt))
}

func TestIsSubtypeOptionalFields(tt *testing.T) {
	assert := assert.New(tt)

	s1 := MakeStructType("", StructField{"a", FloaTType, true})
	s2 := MakeStructType("", StructField{"a", FloaTType, false})
	assert.True(IsSubtype(s1, s2))
	assert.False(IsSubtype(s2, s1))

	s3 := MakeStructType("", StructField{"a", StringType, false})
	assert.False(IsSubtype(s1, s3))
	assert.False(IsSubtype(s3, s1))

	s4 := MakeStructType("", StructField{"a", StringType, true})
	assert.False(IsSubtype(s1, s4))
	assert.False(IsSubtype(s4, s1))

	test := func(t1s, t2s string, exp1, exp2 bool) {
		t1 := makeTestStructTypeFromFieldNames(t1s)
		t2 := makeTestStructTypeFromFieldNames(t2s)
		assert.Equal(exp1, IsSubtype(t1, t2))
		assert.Equal(exp2, IsSubtype(t2, t1))
		assert.False(t1.Equals(t2))
	}

	test("n?", "n", true, false)
	test("", "n", true, false)
	test("", "n?", true, true)

	test("a b?", "a", true, true)
	test("a b?", "a b", true, false)
	test("a b? c", "a b c", true, false)
	test("b? c", "a b c", true, false)
	test("b? c", "b c", true, false)

	test("a c e", "a b c d e", true, false)
	test("a c e?", "a b c d e", true, false)
	test("a c? e", "a b c d e", true, false)
	test("a c? e?", "a b c d e", true, false)
	test("a? c e", "a b c d e", true, false)
	test("a? c e?", "a b c d e", true, false)
	test("a? c? e", "a b c d e", true, false)
	test("a? c? e?", "a b c d e", true, false)

	test("a c e?", "a b c d", true, false)
	test("a c? e", "a b d e", true, false)
	test("a c? e?", "a b d", true, false)
	test("a? c e", "b c d e", true, false)
	test("a? c e?", "b c d", true, false)
	test("a? c? e", "b d e", true, false)
	test("a? c? e?", "b d", true, false)

	t1 := MakeStructType("", StructField{"a", BoolType, true})
	t2 := MakeStructType("", StructField{"a", FloaTType, true})
	assert.False(IsSubtype(t1, t2))
	assert.False(IsSubtype(t2, t1))
}

func makeTestStructTypeFromFieldNames(s string) *Type {
	if s == "" {
		return MakeStructType("")
	}

	fs := strings.Split(s, " ")
	fields := make([]StructField, len(fs))
	for i, f := range fs {
		optional := false
		if f[len(f)-1:] == "?" {
			f = f[:len(f)-1]
			optional = true
		}
		fields[i] = StructField{f, BoolType, optional}
	}
	return MakeStructType("", fields...)
}

func makeTestStructFromFieldNames(s string) Struct {
	t := makeTestStructTypeFromFieldNames(s)
	fields := t.Desc.(StructDesc).fields
	d.Chk.NotEmpty(fields)

	fieldNames := make([]string, len(fields))
	for i, field := range fields {
		fieldNames[i] = field.Name
	}
	vals := make([]Value, len(fields))
	for i := range fields {
		vals[i] = Bool(true)
	}

	return newStruct("", fieldNames, vals)
}

func TestIsSubtypeDisallowExtraStructFields(tt *testing.T) {
	assert := assert.New(tt)

	test := func(t1s, t2s string, exp1, exp2 bool) {
		t1 := makeTestStructTypeFromFieldNames(t1s)
		t2 := makeTestStructTypeFromFieldNames(t2s)
		assert.Equal(exp1, IsSubtypeDisallowExtraStructFields(t1, t2))
		assert.Equal(exp2, IsSubtypeDisallowExtraStructFields(t2, t1))
		assert.False(t1.Equals(t2))
	}

	test("n?", "n", true, false)
	test("", "n", false, false)
	test("", "n?", false, true)

	test("a b?", "a", true, false)
	test("a b?", "a b", true, false)
	test("a b? c", "a b c", true, false)
	test("b? c", "a b c", false, false)
	test("b? c", "b c", true, false)

	test("a c e", "a b c d e", false, false)
	test("a c e?", "a b c d e", false, false)
	test("a c? e", "a b c d e", false, false)
	test("a c? e?", "a b c d e", false, false)
	test("a? c e", "a b c d e", false, false)
	test("a? c e?", "a b c d e", false, false)
	test("a? c? e", "a b c d e", false, false)
	test("a? c? e?", "a b c d e", false, false)

	test("a c e?", "a b c d", false, false)
	test("a c? e", "a b d e", false, false)
	test("a c? e?", "a b d", false, false)
	test("a? c e", "b c d e", false, false)
	test("a? c e?", "b c d", false, false)
	test("a? c? e", "b d e", false, false)
	test("a? c? e?", "b d", false, false)
}

func TestIsValueSubtypeOf(tt *testing.T) {
	assert := assert.New(tt)

	vs := newTestValueStore()

	assertTrue := func(v Value, t *Type) {
		assert.True(IsValueSubtypeOf(v, t))
	}

	assertFalse := func(v Value, t *Type) {
		assert.False(IsValueSubtypeOf(v, t))
	}

	allTypes := []struct {
		v Value
		t *Type
	}{
		{Bool(true), BoolType},
		{Float(42), FloaTType},
		{String("s"), StringType},
		{NewEmptyBlob(vs), BlobType},
		{BoolType, TypeType},
		{NewList(vs, Float(42)), MakeListType(FloaTType)},
		{NewSet(vs, Float(42)), MakeSetType(FloaTType)},
		{NewRef(Float(42)), MakeRefType(FloaTType)},
		{NewMap(vs, Float(42), String("a")), MakeMapType(FloaTType, StringType)},
		{NewStruct("A", StructData{}), MakeStructType("A")},
		// Not including CycleType or Union here
	}
	for i, rec := range allTypes {
		for j, rec2 := range allTypes {
			if i == j {
				assertTrue(rec.v, rec.t)
			} else {
				assertFalse(rec.v, rec2.t)
				assertFalse(rec2.v, rec.t)
			}
		}
	}

	for _, rec := range allTypes {
		assertTrue(rec.v, ValueType)
	}

	assertTrue(Bool(true), MakeUnionType(BoolType, FloaTType))
	assertTrue(Float(123), MakeUnionType(BoolType, FloaTType))
	assertFalse(String("abc"), MakeUnionType(BoolType, FloaTType))
	assertFalse(String("abc"), MakeUnionType())

	assertTrue(NewList(vs), MakeListType(FloaTType))
	assertTrue(NewList(vs, Float(0), Float(1), Float(2), Float(3)), MakeListType(FloaTType))
	assertFalse(NewList(vs, Float(0), Float(1), Float(2), Float(3)), MakeListType(BoolType))
	assertTrue(NewList(vs, Float(0), Float(1), Float(2), Float(3)), MakeListType(MakeUnionType(FloaTType, BoolType)))
	assertTrue(NewList(vs, Float(0), Bool(true)), MakeListType(MakeUnionType(FloaTType, BoolType)))
	assertFalse(NewList(vs, Float(0)), MakeListType(MakeUnionType()))
	assertTrue(NewList(vs), MakeListType(MakeUnionType()))

	{
		newChunkedList := func(vals ...Value) List {
			newSequenceMetaTuple := func(v Value) metaTuple {
				seq := newListLeafSequence(vs, v)
				list := newList(seq)
				return newMetaTuple(vs.WriteValue(context.Background(), list), newOrderedKey(v), 1)
			}

			tuples := make([]metaTuple, len(vals))
			for i, v := range vals {
				tuples[i] = newSequenceMetaTuple(v)
			}
			return newList(newListMetaSequence(1, tuples, vs))
		}

		assertTrue(newChunkedList(Float(0), Float(1), Float(2), Float(3)), MakeListType(FloaTType))
		assertFalse(newChunkedList(Float(0), Float(1), Float(2), Float(3)), MakeListType(BoolType))
		assertTrue(newChunkedList(Float(0), Float(1), Float(2), Float(3)), MakeListType(MakeUnionType(FloaTType, BoolType)))
		assertTrue(newChunkedList(Float(0), Bool(true)), MakeListType(MakeUnionType(FloaTType, BoolType)))
		assertFalse(newChunkedList(Float(0)), MakeListType(MakeUnionType()))
	}

	assertTrue(NewSet(vs), MakeSetType(FloaTType))
	assertTrue(NewSet(vs, Float(0), Float(1), Float(2), Float(3)), MakeSetType(FloaTType))
	assertFalse(NewSet(vs, Float(0), Float(1), Float(2), Float(3)), MakeSetType(BoolType))
	assertTrue(NewSet(vs, Float(0), Float(1), Float(2), Float(3)), MakeSetType(MakeUnionType(FloaTType, BoolType)))
	assertTrue(NewSet(vs, Float(0), Bool(true)), MakeSetType(MakeUnionType(FloaTType, BoolType)))
	assertFalse(NewSet(vs, Float(0)), MakeSetType(MakeUnionType()))
	assertTrue(NewSet(vs), MakeSetType(MakeUnionType()))

	{
		newChunkedSet := func(vals ...Value) Set {
			newSequenceMetaTuple := func(v Value) metaTuple {
				seq := newSetLeafSequence(vs, v)
				set := newSet(seq)
				return newMetaTuple(vs.WriteValue(context.Background(), set), newOrderedKey(v), 1)
			}

			tuples := make([]metaTuple, len(vals))
			for i, v := range vals {
				tuples[i] = newSequenceMetaTuple(v)
			}
			return newSet(newSetMetaSequence(1, tuples, vs))
		}
		assertTrue(newChunkedSet(Float(0), Float(1), Float(2), Float(3)), MakeSetType(FloaTType))
		assertFalse(newChunkedSet(Float(0), Float(1), Float(2), Float(3)), MakeSetType(BoolType))
		assertTrue(newChunkedSet(Float(0), Float(1), Float(2), Float(3)), MakeSetType(MakeUnionType(FloaTType, BoolType)))
		assertTrue(newChunkedSet(Float(0), Bool(true)), MakeSetType(MakeUnionType(FloaTType, BoolType)))
		assertFalse(newChunkedSet(Float(0)), MakeSetType(MakeUnionType()))
	}

	assertTrue(NewMap(vs), MakeMapType(FloaTType, StringType))
	assertTrue(NewMap(vs, Float(0), String("a"), Float(1), String("b")), MakeMapType(FloaTType, StringType))
	assertFalse(NewMap(vs, Float(0), String("a"), Float(1), String("b")), MakeMapType(BoolType, StringType))
	assertFalse(NewMap(vs, Float(0), String("a"), Float(1), String("b")), MakeMapType(FloaTType, BoolType))
	assertTrue(NewMap(vs, Float(0), String("a"), Float(1), String("b")), MakeMapType(MakeUnionType(FloaTType, BoolType), StringType))
	assertTrue(NewMap(vs, Float(0), String("a"), Float(1), String("b")), MakeMapType(FloaTType, MakeUnionType(BoolType, StringType)))
	assertTrue(NewMap(vs, Float(0), String("a"), Bool(true), String("b")), MakeMapType(MakeUnionType(FloaTType, BoolType), StringType))
	assertTrue(NewMap(vs, Float(0), String("a"), Float(1), Bool(true)), MakeMapType(FloaTType, MakeUnionType(BoolType, StringType)))
	assertFalse(NewMap(vs, Float(0), String("a")), MakeMapType(MakeUnionType(), StringType))
	assertFalse(NewMap(vs, Float(0), String("a")), MakeMapType(FloaTType, MakeUnionType()))
	assertTrue(NewMap(vs), MakeMapType(MakeUnionType(), MakeUnionType()))

	{
		newChunkedMap := func(vals ...Value) Map {
			newSequenceMetaTuple := func(e mapEntry) metaTuple {
				seq := newMapLeafSequence(vs, e)
				m := newMap(seq)
				return newMetaTuple(vs.WriteValue(context.Background(), m), newOrderedKey(e.key), 1)
			}

			tuples := make([]metaTuple, len(vals)/2)
			for i := 0; i < len(vals); i += 2 {
				tuples[i/2] = newSequenceMetaTuple(mapEntry{vals[i], vals[i+1]})
			}
			return newMap(newMapMetaSequence(1, tuples, vs))
		}

		assertTrue(newChunkedMap(Float(0), String("a"), Float(1), String("b")), MakeMapType(FloaTType, StringType))
		assertFalse(newChunkedMap(Float(0), String("a"), Float(1), String("b")), MakeMapType(BoolType, StringType))
		assertFalse(newChunkedMap(Float(0), String("a"), Float(1), String("b")), MakeMapType(FloaTType, BoolType))
		assertTrue(newChunkedMap(Float(0), String("a"), Float(1), String("b")), MakeMapType(MakeUnionType(FloaTType, BoolType), StringType))
		assertTrue(newChunkedMap(Float(0), String("a"), Float(1), String("b")), MakeMapType(FloaTType, MakeUnionType(BoolType, StringType)))
		assertTrue(newChunkedMap(Float(0), String("a"), Bool(true), String("b")), MakeMapType(MakeUnionType(FloaTType, BoolType), StringType))
		assertTrue(newChunkedMap(Float(0), String("a"), Float(1), Bool(true)), MakeMapType(FloaTType, MakeUnionType(BoolType, StringType)))
		assertFalse(newChunkedMap(Float(0), String("a")), MakeMapType(MakeUnionType(), StringType))
		assertFalse(newChunkedMap(Float(0), String("a")), MakeMapType(FloaTType, MakeUnionType()))
	}

	assertTrue(NewRef(Float(1)), MakeRefType(FloaTType))
	assertFalse(NewRef(Float(1)), MakeRefType(BoolType))
	assertTrue(NewRef(Float(1)), MakeRefType(MakeUnionType(FloaTType, BoolType)))
	assertFalse(NewRef(Float(1)), MakeRefType(MakeUnionType()))

	assertTrue(
		NewStruct("Struct", StructData{"x": Bool(true)}),
		MakeStructType("Struct", StructField{"x", BoolType, false}),
	)
	assertTrue(
		NewStruct("Struct", StructData{"x": Bool(true)}),
		MakeStructType("Struct", StructField{"x", BoolType, true}),
	)
	assertTrue(
		NewStruct("Struct", StructData{"x": Bool(true)}),
		MakeStructType("Struct"),
	)
	assertTrue(
		NewStruct("Struct", StructData{}),
		MakeStructType("Struct"),
	)
	assertFalse(
		NewStruct("", StructData{"x": Bool(true)}),
		MakeStructType("Struct"),
	)
	assertFalse(
		NewStruct("struct", StructData{"x": Bool(true)}), // lower case name
		MakeStructType("Struct"),
	)
	assertTrue(
		NewStruct("Struct", StructData{"x": Bool(true)}),
		MakeStructType("Struct", StructField{"x", MakeUnionType(BoolType, FloaTType), true}),
	)
	assertTrue(
		NewStruct("Struct", StructData{"x": Bool(true)}),
		MakeStructType("Struct", StructField{"y", BoolType, true}),
	)
	assertFalse(
		NewStruct("Struct", StructData{"x": Bool(true)}),
		MakeStructType("Struct", StructField{"x", StringType, true}),
	)

	assertTrue(
		NewStruct("Node", StructData{
			"value": Float(1),
			"children": NewList(vs,
				NewStruct("Node", StructData{
					"value":    Float(2),
					"children": NewList(vs),
				}),
			),
		}),
		MakeStructType("Node",
			StructField{"value", FloaTType, false},
			StructField{"children", MakeListType(MakeCycleType("Node")), false},
		),
	)

	assertFalse( // inner Node has wrong type.
		NewStruct("Node", StructData{
			"value": Float(1),
			"children": NewList(vs,
				NewStruct("Node", StructData{
					"value":    Bool(true),
					"children": NewList(vs),
				}),
			),
		}),
		MakeStructType("Node",
			StructField{"value", FloaTType, false},
			StructField{"children", MakeListType(MakeCycleType("Node")), false},
		),
	)

	{
		node := func(value Value, children ...Value) Value {
			childrenAsRefs := make(ValueSlice, len(children))
			for i, c := range children {
				childrenAsRefs[i] = NewRef(c)
			}
			rv := NewStruct("Node", StructData{
				"value":    value,
				"children": NewList(vs, childrenAsRefs...),
			})
			return rv
		}

		requiredType := MakeStructType("Node",
			StructField{"value", FloaTType, false},
			StructField{"children", MakeListType(MakeRefType(MakeCycleType("Node"))), false},
		)

		assertTrue(
			node(Float(0), node(Float(1)), node(Float(2), node(Float(3)))),
			requiredType,
		)
		assertFalse(
			node(Float(0),
				node(Float(1)),
				node(Float(2), node(String("no"))),
			),
			requiredType,
		)
	}

	{
		t1 := MakeStructType("A",
			StructField{"a", FloaTType, false},
			StructField{"b", MakeCycleType("A"), false},
		)
		t2 := MakeStructType("A",
			StructField{"a", FloaTType, false},
			StructField{"b", MakeCycleType("A"), true},
		)
		v := NewStruct("A", StructData{
			"a": Float(1),
			"b": NewStruct("A", StructData{
				"a": Float(2),
			}),
		})

		assertFalse(v, t1)
		assertTrue(v, t2)
	}

	{
		t := MakeStructType("A",
			StructField{"aa", FloaTType, true},
			StructField{"bb", BoolType, false},
		)
		v := NewStruct("A", StructData{
			"a": Float(1),
			"b": Bool(true),
		})
		assertFalse(v, t)
	}
}

func TestIsValueSubtypeOfDetails(tt *testing.T) {
	a := assert.New(tt)

	test := func(vString, tString string, exp1, exp2 bool) {
		v := makeTestStructFromFieldNames(vString)
		t := makeTestStructTypeFromFieldNames(tString)
		isSub, hasExtra := IsValueSubtypeOfDetails(v, t)
		a.Equal(exp1, isSub, "expected %t for IsSub, received: %t", exp1, isSub)
		if isSub {
			a.Equal(exp2, hasExtra, "expected %t for hasExtra, received: %t", exp2, hasExtra)
		}
	}

	test("x", "x", true, false)
	test("x", "", true, true)
	test("x", "x? y?", true, false)
	test("x z", "x? y?", true, true)
	test("x", "x y", false, false)
}
