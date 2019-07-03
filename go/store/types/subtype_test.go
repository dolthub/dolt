// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/stretchr/testify/assert"
)

func assertInvalid(tt *testing.T, t *Type, v Value) {
	assert := assert.New(tt)
	assert.Panics(func() {
		assertSubtype(context.Background(), Format_7_18, t, v)
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
			assertSubtype(context.Background(), Format_7_18, at, v)
		} else {
			assertInvalid(tt, at, v)
		}
	}
}

func TestAssertTypePrimitives(t *testing.T) {
	assertSubtype(context.Background(), Format_7_18, BoolType, Bool(true))
	assertSubtype(context.Background(), Format_7_18, BoolType, Bool(false))
	assertSubtype(context.Background(), Format_7_18, FloaTType, Float(42))
	assertSubtype(context.Background(), Format_7_18, StringType, String("abc"))
	assertSubtype(context.Background(), Format_7_18, UUIDType, UUID(uuid.Must(uuid.NewUUID())))
	assertSubtype(context.Background(), Format_7_18, IntType, Int(-1))
	assertSubtype(context.Background(), Format_7_18, UintType, Uint(0xffffffffffffffff))

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

	assertSubtype(context.Background(), Format_7_18, ValueType, Bool(true))
	assertSubtype(context.Background(), Format_7_18, ValueType, Float(1))
	assertSubtype(context.Background(), Format_7_18, ValueType, String("abc"))
	// TODO(binformat)
	l := NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))
	assertSubtype(context.Background(), Format_7_18, ValueType, l)
}

func TestAssertTypeBlob(t *testing.T) {
	vs := newTestValueStore()

	blob := NewBlob(context.Background(), vs, bytes.NewBuffer([]byte{0x00, 0x01}))
	assertAll(t, BlobType, blob)
}

func TestAssertTypeList(tt *testing.T) {
	vs := newTestValueStore()

	listOfNumberType := MakeListType(FloaTType)
	// TODO(binformat)
	l := NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))
	assertSubtype(context.Background(), Format_7_18, listOfNumberType, l)
	assertAll(tt, listOfNumberType, l)
	assertSubtype(context.Background(), Format_7_18, MakeListType(ValueType), l)
}

func TestAssertTypeMap(tt *testing.T) {
	vs := newTestValueStore()

	mapOfNumberToStringType := MakeMapType(FloaTType, StringType)
	m := NewMap(context.Background(), vs, Float(0), String("a"), Float(2), String("b"))
	assertSubtype(context.Background(), Format_7_18, mapOfNumberToStringType, m)
	assertAll(tt, mapOfNumberToStringType, m)
	assertSubtype(context.Background(), Format_7_18, MakeMapType(ValueType, ValueType), m)
}

func TestAssertTypeSet(tt *testing.T) {
	vs := newTestValueStore()

	setOfNumberType := MakeSetType(FloaTType)
	s := NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))
	assertSubtype(context.Background(), Format_7_18, setOfNumberType, s)
	assertAll(tt, setOfNumberType, s)
	assertSubtype(context.Background(), Format_7_18, MakeSetType(ValueType), s)
}

func TestAssertTypeType(tt *testing.T) {
	t := MakeSetType(FloaTType)
	assertSubtype(context.Background(), Format_7_18, TypeType, t)
	assertAll(tt, TypeType, t)
	assertSubtype(context.Background(), Format_7_18, ValueType, t)
}

func TestAssertTypeStruct(tt *testing.T) {
	t := MakeStructType("Struct", StructField{"x", BoolType, false})

	v := NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)})
	assertSubtype(context.Background(), Format_7_18, t, v)
	assertAll(tt, t, v)
	assertSubtype(context.Background(), Format_7_18, ValueType, v)
}

func TestAssertTypeUnion(tt *testing.T) {
	vs := newTestValueStore()

	assertSubtype(context.Background(), Format_7_18, MakeUnionType(FloaTType), Float(42))
	assertSubtype(context.Background(), Format_7_18, MakeUnionType(FloaTType, StringType), Float(42))
	assertSubtype(context.Background(), Format_7_18, MakeUnionType(FloaTType, StringType), String("hi"))
	assertSubtype(context.Background(), Format_7_18, MakeUnionType(FloaTType, StringType, BoolType), Float(555))
	assertSubtype(context.Background(), Format_7_18, MakeUnionType(FloaTType, StringType, BoolType), String("hi"))
	assertSubtype(context.Background(), Format_7_18, MakeUnionType(FloaTType, StringType, BoolType), Bool(true))

	lt := MakeListType(MakeUnionType(FloaTType, StringType))
	// TODO(binformat)
	assertSubtype(context.Background(), Format_7_18, lt, NewList(context.Background(), vs, Float(1), String("hi"), Float(2), String("bye")))

	st := MakeSetType(StringType)
	assertSubtype(context.Background(), Format_7_18, MakeUnionType(st, FloaTType), Float(42))
	assertSubtype(context.Background(), Format_7_18, MakeUnionType(st, FloaTType), NewSet(context.Background(), vs, String("a"), String("b")))

	assertInvalid(tt, MakeUnionType(), Float(42))
	assertInvalid(tt, MakeUnionType(StringType), Float(42))
	assertInvalid(tt, MakeUnionType(StringType, BoolType), Float(42))
	assertInvalid(tt, MakeUnionType(st, StringType), Float(42))
	assertInvalid(tt, MakeUnionType(st, FloaTType), NewSet(context.Background(), vs, Float(1), Float(2)))
}

func TestAssertConcreteTypeIsUnion(tt *testing.T) {
	assert.True(tt, IsSubtype(
		Format_7_18,
		MakeStructTypeFromFields("", FieldMap{}),
		MakeUnionType(
			MakeStructTypeFromFields("", FieldMap{"foo": StringType}),
			MakeStructTypeFromFields("", FieldMap{"bar": StringType}))))

	assert.False(tt, IsSubtype(
		Format_7_18,
		MakeStructTypeFromFields("", FieldMap{}),
		MakeUnionType(MakeStructTypeFromFields("", FieldMap{"foo": StringType}),
			FloaTType)))

	assert.True(tt, IsSubtype(
		Format_7_18,
		MakeUnionType(
			MakeStructTypeFromFields("", FieldMap{"foo": StringType}),
			MakeStructTypeFromFields("", FieldMap{"bar": StringType})),
		MakeUnionType(
			MakeStructTypeFromFields("", FieldMap{"foo": StringType, "bar": StringType}),
			MakeStructTypeFromFields("", FieldMap{"bar": StringType}))))

	assert.False(tt, IsSubtype(
		Format_7_18,
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
	// TODO(binformat)
	assertSubtype(context.Background(), Format_7_18, lt, NewList(context.Background(), vs))
}

func TestAssertTypeEmptyList(tt *testing.T) {
	vs := newTestValueStore()

	lt := MakeListType(FloaTType)
	// TODO(binformat)
	assertSubtype(context.Background(), Format_7_18, lt, NewList(context.Background(), vs))

	// List<> not a subtype of List<Float>
	// TODO(binformat)
	assertInvalid(tt, MakeListType(MakeUnionType()), NewList(context.Background(), vs, Float(1)))
}

func TestAssertTypeEmptySet(tt *testing.T) {
	vs := newTestValueStore()

	st := MakeSetType(FloaTType)
	assertSubtype(context.Background(), Format_7_18, st, NewSet(context.Background(), vs))

	// Set<> not a subtype of Set<Float>
	assertInvalid(tt, MakeSetType(MakeUnionType()), NewSet(context.Background(), vs, Float(1)))
}

func TestAssertTypeEmptyMap(tt *testing.T) {
	vs := newTestValueStore()

	mt := MakeMapType(FloaTType, StringType)
	assertSubtype(context.Background(), Format_7_18, mt, NewMap(context.Background(), vs))

	// Map<> not a subtype of Map<Float, Float>
	assertInvalid(tt, MakeMapType(MakeUnionType(), MakeUnionType()), NewMap(context.Background(), vs, Float(1), Float(2)))
}

func TestAssertTypeStructSubtypeByName(tt *testing.T) {
	namedT := MakeStructType("Name", StructField{"x", FloaTType, false})
	anonT := MakeStructType("", StructField{"x", FloaTType, false})
	namedV := NewStruct(Format_7_18, "Name", StructData{"x": Float(42)})
	name2V := NewStruct(Format_7_18, "foo", StructData{"x": Float(42)})
	anonV := NewStruct(Format_7_18, "", StructData{"x": Float(42)})

	assertSubtype(context.Background(), Format_7_18, namedT, namedV)
	assertInvalid(tt, namedT, name2V)
	assertInvalid(tt, namedT, anonV)

	assertSubtype(context.Background(), Format_7_18, anonT, namedV)
	assertSubtype(context.Background(), Format_7_18, anonT, name2V)
	assertSubtype(context.Background(), Format_7_18, anonT, anonV)
}

func TestAssertTypeStructSubtypeExtraFields(tt *testing.T) {
	at := MakeStructType("")
	bt := MakeStructType("", StructField{"x", FloaTType, false})
	ct := MakeStructType("", StructField{"s", StringType, false}, StructField{"x", FloaTType, false})
	av := NewStruct(Format_7_18, "", StructData{})
	bv := NewStruct(Format_7_18, "", StructData{"x": Float(1)})
	cv := NewStruct(Format_7_18, "", StructData{"x": Float(2), "s": String("hi")})

	assertSubtype(context.Background(), Format_7_18, at, av)
	assertInvalid(tt, bt, av)
	assertInvalid(tt, ct, av)

	assertSubtype(context.Background(), Format_7_18, at, bv)
	assertSubtype(context.Background(), Format_7_18, bt, bv)
	assertInvalid(tt, ct, bv)

	assertSubtype(context.Background(), Format_7_18, at, cv)
	assertSubtype(context.Background(), Format_7_18, bt, cv)
	assertSubtype(context.Background(), Format_7_18, ct, cv)
}

func TestAssertTypeStructSubtype(tt *testing.T) {
	vs := newTestValueStore()

	c1 := NewStruct(Format_7_18, "Commit", StructData{
		"value":   Float(1),
		"parents": NewSet(context.Background(), vs),
	})
	t1 := MakeStructType("Commit",
		StructField{"parents", MakeSetType(MakeUnionType()), false},
		StructField{"value", FloaTType, false},
	)
	assertSubtype(context.Background(), Format_7_18, t1, c1)

	t11 := MakeStructType("Commit",
		StructField{"parents", MakeSetType(MakeRefType(MakeCycleType("Commit"))), false},
		StructField{"value", FloaTType, false},
	)
	assertSubtype(context.Background(), Format_7_18, t11, c1)

	c2 := NewStruct(Format_7_18, "Commit", StructData{
		"value":   Float(2),
		"parents": NewSet(context.Background(), vs, NewRef(c1, Format_7_18)),
	})
	assertSubtype(context.Background(), Format_7_18, t11, c2)
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

	assert.True(tt, IsSubtype(Format_7_18, t2, t1))
	assert.False(tt, IsSubtype(Format_7_18, t1, t2))

	// struct S {
	//   x: Cycle<S> | Float,
	//   y: Float | String,
	// }
	t3 := MakeStructType("S",
		StructField{"x", MakeUnionType(MakeCycleType("S"), FloaTType), false},
		StructField{"y", MakeUnionType(FloaTType, StringType), false},
	)

	assert.True(tt, IsSubtype(Format_7_18, t3, t1))
	assert.False(tt, IsSubtype(Format_7_18, t1, t3))

	assert.True(tt, IsSubtype(Format_7_18, t3, t2))
	assert.False(tt, IsSubtype(Format_7_18, t2, t3))

	// struct S {
	//   x: Cycle<S> | Float,
	//   y: Float,
	// }
	t4 := MakeStructType("S",
		StructField{"x", MakeUnionType(MakeCycleType("S"), FloaTType), false},
		StructField{"y", FloaTType, false},
	)

	assert.True(tt, IsSubtype(Format_7_18, t4, t1))
	assert.False(tt, IsSubtype(Format_7_18, t1, t4))

	assert.False(tt, IsSubtype(Format_7_18, t4, t2))
	assert.False(tt, IsSubtype(Format_7_18, t2, t4))

	assert.True(tt, IsSubtype(Format_7_18, t3, t4))
	assert.False(tt, IsSubtype(Format_7_18, t4, t3))

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

	assert.False(tt, IsSubtype(Format_7_18, tb, tc))
	assert.False(tt, IsSubtype(Format_7_18, tc, tb))
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

	assert.False(tt, IsSubtype(Format_7_18, t1, t2))
	assert.True(tt, IsSubtype(Format_7_18, t2, t1))
}

func TestIsSubtypeCompoundUnion(tt *testing.T) {
	rt := MakeListType(EmptyStructType)

	st1 := MakeStructType("One", StructField{"a", FloaTType, false})
	st2 := MakeStructType("Two", StructField{"b", StringType, false})
	ct := MakeListType(MakeUnionType(st1, st2))

	assert.True(tt, IsSubtype(Format_7_18, rt, ct))
	assert.False(tt, IsSubtype(Format_7_18, ct, rt))

	ct2 := MakeListType(MakeUnionType(st1, st2, FloaTType))
	assert.False(tt, IsSubtype(Format_7_18, rt, ct2))
	assert.False(tt, IsSubtype(Format_7_18, ct2, rt))
}

func TestIsSubtypeOptionalFields(tt *testing.T) {
	assert := assert.New(tt)

	s1 := MakeStructType("", StructField{"a", FloaTType, true})
	s2 := MakeStructType("", StructField{"a", FloaTType, false})
	assert.True(IsSubtype(Format_7_18, s1, s2))
	assert.False(IsSubtype(Format_7_18, s2, s1))

	s3 := MakeStructType("", StructField{"a", StringType, false})
	assert.False(IsSubtype(Format_7_18, s1, s3))
	assert.False(IsSubtype(Format_7_18, s3, s1))

	s4 := MakeStructType("", StructField{"a", StringType, true})
	assert.False(IsSubtype(Format_7_18, s1, s4))
	assert.False(IsSubtype(Format_7_18, s4, s1))

	test := func(t1s, t2s string, exp1, exp2 bool) {
		t1 := makeTestStructTypeFromFieldNames(t1s)
		t2 := makeTestStructTypeFromFieldNames(t2s)
		assert.Equal(exp1, IsSubtype(Format_7_18, t1, t2))
		assert.Equal(exp2, IsSubtype(Format_7_18, t2, t1))
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
	assert.False(IsSubtype(Format_7_18, t1, t2))
	assert.False(IsSubtype(Format_7_18, t2, t1))
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

	return newStruct(Format_7_18, "", fieldNames, vals)
}

func TestIsSubtypeDisallowExtraStructFields(tt *testing.T) {
	assert := assert.New(tt)

	test := func(t1s, t2s string, exp1, exp2 bool) {
		t1 := makeTestStructTypeFromFieldNames(t1s)
		t2 := makeTestStructTypeFromFieldNames(t2s)
		assert.Equal(exp1, IsSubtypeDisallowExtraStructFields(Format_7_18, t1, t2))
		assert.Equal(exp2, IsSubtypeDisallowExtraStructFields(Format_7_18, t2, t1))
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
		assert.True(IsValueSubtypeOf(Format_7_18, v, t))
	}

	assertFalse := func(v Value, t *Type) {
		assert.False(IsValueSubtypeOf(Format_7_18, v, t))
	}

	// TODO(binformat)
	allTypes := []struct {
		v Value
		t *Type
	}{
		{Bool(true), BoolType},
		{Float(42), FloaTType},
		{String("s"), StringType},
		{NewEmptyBlob(vs), BlobType},
		{BoolType, TypeType},
		{NewList(context.Background(), vs, Float(42)), MakeListType(FloaTType)},
		{NewSet(context.Background(), vs, Float(42)), MakeSetType(FloaTType)},
		{NewRef(Float(42), Format_7_18), MakeRefType(FloaTType)},
		{NewMap(context.Background(), vs, Float(42), String("a")), MakeMapType(FloaTType, StringType)},
		{NewStruct(Format_7_18, "A", StructData{}), MakeStructType("A")},
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

	// TODO(binformat)
	assertTrue(NewList(context.Background(), vs), MakeListType(FloaTType))
	assertTrue(NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3)), MakeListType(FloaTType))
	assertFalse(NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3)), MakeListType(BoolType))
	assertTrue(NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3)), MakeListType(MakeUnionType(FloaTType, BoolType)))
	assertTrue(NewList(context.Background(), vs, Float(0), Bool(true)), MakeListType(MakeUnionType(FloaTType, BoolType)))
	assertFalse(NewList(context.Background(), vs, Float(0)), MakeListType(MakeUnionType()))
	assertTrue(NewList(context.Background(), vs), MakeListType(MakeUnionType()))

	{
		newChunkedList := func(vals ...Value) List {
			newSequenceMetaTuple := func(v Value) metaTuple {
				seq := newListLeafSequence(vs, Format_7_18, v)
				list := newList(seq, Format_7_18)
				return newMetaTuple(Format_7_18, vs.WriteValue(context.Background(), list), newOrderedKey(v, Format_7_18), 1)
			}

			tuples := make([]metaTuple, len(vals))
			for i, v := range vals {
				tuples[i] = newSequenceMetaTuple(v)
			}
			return newList(newListMetaSequence(1, tuples, Format_7_18, vs), Format_7_18)
		}

		assertTrue(newChunkedList(Float(0), Float(1), Float(2), Float(3)), MakeListType(FloaTType))
		assertFalse(newChunkedList(Float(0), Float(1), Float(2), Float(3)), MakeListType(BoolType))
		assertTrue(newChunkedList(Float(0), Float(1), Float(2), Float(3)), MakeListType(MakeUnionType(FloaTType, BoolType)))
		assertTrue(newChunkedList(Float(0), Bool(true)), MakeListType(MakeUnionType(FloaTType, BoolType)))
		assertFalse(newChunkedList(Float(0)), MakeListType(MakeUnionType()))
	}

	assertTrue(NewSet(context.Background(), vs), MakeSetType(FloaTType))
	assertTrue(NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3)), MakeSetType(FloaTType))
	assertFalse(NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3)), MakeSetType(BoolType))
	assertTrue(NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3)), MakeSetType(MakeUnionType(FloaTType, BoolType)))
	assertTrue(NewSet(context.Background(), vs, Float(0), Bool(true)), MakeSetType(MakeUnionType(FloaTType, BoolType)))
	assertFalse(NewSet(context.Background(), vs, Float(0)), MakeSetType(MakeUnionType()))
	assertTrue(NewSet(context.Background(), vs), MakeSetType(MakeUnionType()))

	{
		newChunkedSet := func(vals ...Value) Set {
			newSequenceMetaTuple := func(v Value) metaTuple {
				seq := newSetLeafSequence(Format_7_18, vs, v)
				set := newSet(Format_7_18, seq)
				return newMetaTuple(Format_7_18, vs.WriteValue(context.Background(), set), newOrderedKey(v, Format_7_18), 1)
			}

			tuples := make([]metaTuple, len(vals))
			for i, v := range vals {
				tuples[i] = newSequenceMetaTuple(v)
			}
			return newSet(Format_7_18, newSetMetaSequence(1, tuples, Format_7_18, vs))
		}
		assertTrue(newChunkedSet(Float(0), Float(1), Float(2), Float(3)), MakeSetType(FloaTType))
		assertFalse(newChunkedSet(Float(0), Float(1), Float(2), Float(3)), MakeSetType(BoolType))
		assertTrue(newChunkedSet(Float(0), Float(1), Float(2), Float(3)), MakeSetType(MakeUnionType(FloaTType, BoolType)))
		assertTrue(newChunkedSet(Float(0), Bool(true)), MakeSetType(MakeUnionType(FloaTType, BoolType)))
		assertFalse(newChunkedSet(Float(0)), MakeSetType(MakeUnionType()))
	}

	assertTrue(NewMap(context.Background(), vs), MakeMapType(FloaTType, StringType))
	assertTrue(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b")), MakeMapType(FloaTType, StringType))
	assertFalse(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b")), MakeMapType(BoolType, StringType))
	assertFalse(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b")), MakeMapType(FloaTType, BoolType))
	assertTrue(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b")), MakeMapType(MakeUnionType(FloaTType, BoolType), StringType))
	assertTrue(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b")), MakeMapType(FloaTType, MakeUnionType(BoolType, StringType)))
	assertTrue(NewMap(context.Background(), vs, Float(0), String("a"), Bool(true), String("b")), MakeMapType(MakeUnionType(FloaTType, BoolType), StringType))
	assertTrue(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), Bool(true)), MakeMapType(FloaTType, MakeUnionType(BoolType, StringType)))
	assertFalse(NewMap(context.Background(), vs, Float(0), String("a")), MakeMapType(MakeUnionType(), StringType))
	assertFalse(NewMap(context.Background(), vs, Float(0), String("a")), MakeMapType(FloaTType, MakeUnionType()))
	assertTrue(NewMap(context.Background(), vs), MakeMapType(MakeUnionType(), MakeUnionType()))

	{
		newChunkedMap := func(vals ...Value) Map {
			newSequenceMetaTuple := func(e mapEntry) metaTuple {
				seq := newMapLeafSequence(Format_7_18, vs, e)
				m := newMap(seq, Format_7_18)
				return newMetaTuple(Format_7_18, vs.WriteValue(context.Background(), m), newOrderedKey(e.key, Format_7_18), 1)
			}

			tuples := make([]metaTuple, len(vals)/2)
			for i := 0; i < len(vals); i += 2 {
				tuples[i/2] = newSequenceMetaTuple(mapEntry{vals[i], vals[i+1]})
			}
			return newMap(newMapMetaSequence(1, tuples, Format_7_18, vs), Format_7_18)
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

	assertTrue(NewRef(Float(1), Format_7_18), MakeRefType(FloaTType))
	assertFalse(NewRef(Float(1), Format_7_18), MakeRefType(BoolType))
	assertTrue(NewRef(Float(1), Format_7_18), MakeRefType(MakeUnionType(FloaTType, BoolType)))
	assertFalse(NewRef(Float(1), Format_7_18), MakeRefType(MakeUnionType()))

	assertTrue(
		NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)}),
		MakeStructType("Struct", StructField{"x", BoolType, false}),
	)
	assertTrue(
		NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)}),
		MakeStructType("Struct", StructField{"x", BoolType, true}),
	)
	assertTrue(
		NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)}),
		MakeStructType("Struct"),
	)
	assertTrue(
		NewStruct(Format_7_18, "Struct", StructData{}),
		MakeStructType("Struct"),
	)
	assertFalse(
		NewStruct(Format_7_18, "", StructData{"x": Bool(true)}),
		MakeStructType("Struct"),
	)
	assertFalse(
		NewStruct(Format_7_18, "struct", StructData{"x": Bool(true)}), // lower case name
		MakeStructType("Struct"),
	)
	assertTrue(
		NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)}),
		MakeStructType("Struct", StructField{"x", MakeUnionType(BoolType, FloaTType), true}),
	)
	assertTrue(
		NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)}),
		MakeStructType("Struct", StructField{"y", BoolType, true}),
	)
	assertFalse(
		NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)}),
		MakeStructType("Struct", StructField{"x", StringType, true}),
	)

	assertTrue(
		NewStruct(Format_7_18, "Node", StructData{
			"value": Float(1),
			// TODO(binformat)
			"children": NewList(context.Background(), vs,
				NewStruct(Format_7_18, "Node", StructData{
					"value":    Float(2),
					"children": NewList(context.Background(), vs),
				}),
			),
		}),
		MakeStructType("Node",
			StructField{"value", FloaTType, false},
			StructField{"children", MakeListType(MakeCycleType("Node")), false},
		),
	)

	assertFalse( // inner Node has wrong type.
		NewStruct(Format_7_18, "Node", StructData{
			"value": Float(1),
			// TODO(binformat)
			"children": NewList(context.Background(), vs,
				NewStruct(Format_7_18, "Node", StructData{
					"value":    Bool(true),
					"children": NewList(context.Background(), vs),
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
				childrenAsRefs[i] = NewRef(c, Format_7_18)
			}
			rv := NewStruct(Format_7_18, "Node", StructData{
				"value":    value,
				"children": NewList(context.Background(), vs, childrenAsRefs...),
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
		v := NewStruct(Format_7_18, "A", StructData{
			"a": Float(1),
			"b": NewStruct(Format_7_18, "A", StructData{
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
		v := NewStruct(Format_7_18, "A", StructData{
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
		isSub, hasExtra := IsValueSubtypeOfDetails(Format_7_18, v, t)
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
