// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/d"
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
		NumberType,
		StringType,
		BlobType,
		TypeType,
		ValueType,
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
	assertSubtype(NumberType, Number(42))
	assertSubtype(StringType, String("abc"))

	assertInvalid(t, BoolType, Number(1))
	assertInvalid(t, BoolType, String("abc"))
	assertInvalid(t, NumberType, Bool(true))
	assertInvalid(t, StringType, Number(42))
}

func TestAssertTypeValue(t *testing.T) {
	vs := newTestValueStore()

	assertSubtype(ValueType, Bool(true))
	assertSubtype(ValueType, Number(1))
	assertSubtype(ValueType, String("abc"))
	l := NewList(vs, Number(0), Number(1), Number(2), Number(3))
	assertSubtype(ValueType, l)
}

func TestAssertTypeBlob(t *testing.T) {
	vs := newTestValueStore()

	blob := NewBlob(vs, bytes.NewBuffer([]byte{0x00, 0x01}))
	assertAll(t, BlobType, blob)
}

func TestAssertTypeList(tt *testing.T) {
	vs := newTestValueStore()

	listOfNumberType := MakeListType(NumberType)
	l := NewList(vs, Number(0), Number(1), Number(2), Number(3))
	assertSubtype(listOfNumberType, l)
	assertAll(tt, listOfNumberType, l)
	assertSubtype(MakeListType(ValueType), l)
}

func TestAssertTypeMap(tt *testing.T) {
	vs := newTestValueStore()

	mapOfNumberToStringType := MakeMapType(NumberType, StringType)
	m := NewMap(vs, Number(0), String("a"), Number(2), String("b"))
	assertSubtype(mapOfNumberToStringType, m)
	assertAll(tt, mapOfNumberToStringType, m)
	assertSubtype(MakeMapType(ValueType, ValueType), m)
}

func TestAssertTypeSet(tt *testing.T) {
	vs := newTestValueStore()

	setOfNumberType := MakeSetType(NumberType)
	s := NewSet(vs, Number(0), Number(1), Number(2), Number(3))
	assertSubtype(setOfNumberType, s)
	assertAll(tt, setOfNumberType, s)
	assertSubtype(MakeSetType(ValueType), s)
}

func TestAssertTypeType(tt *testing.T) {
	t := MakeSetType(NumberType)
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

	assertSubtype(MakeUnionType(NumberType), Number(42))
	assertSubtype(MakeUnionType(NumberType, StringType), Number(42))
	assertSubtype(MakeUnionType(NumberType, StringType), String("hi"))
	assertSubtype(MakeUnionType(NumberType, StringType, BoolType), Number(555))
	assertSubtype(MakeUnionType(NumberType, StringType, BoolType), String("hi"))
	assertSubtype(MakeUnionType(NumberType, StringType, BoolType), Bool(true))

	lt := MakeListType(MakeUnionType(NumberType, StringType))
	assertSubtype(lt, NewList(vs, Number(1), String("hi"), Number(2), String("bye")))

	st := MakeSetType(StringType)
	assertSubtype(MakeUnionType(st, NumberType), Number(42))
	assertSubtype(MakeUnionType(st, NumberType), NewSet(vs, String("a"), String("b")))

	assertInvalid(tt, MakeUnionType(), Number(42))
	assertInvalid(tt, MakeUnionType(StringType), Number(42))
	assertInvalid(tt, MakeUnionType(StringType, BoolType), Number(42))
	assertInvalid(tt, MakeUnionType(st, StringType), Number(42))
	assertInvalid(tt, MakeUnionType(st, NumberType), NewSet(vs, Number(1), Number(2)))
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
			NumberType)))

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
			NumberType)))
}

func TestAssertTypeEmptyListUnion(tt *testing.T) {
	vs := newTestValueStore()

	lt := MakeListType(MakeUnionType())
	assertSubtype(lt, NewList(vs))
}

func TestAssertTypeEmptyList(tt *testing.T) {
	vs := newTestValueStore()

	lt := MakeListType(NumberType)
	assertSubtype(lt, NewList(vs))

	// List<> not a subtype of List<Number>
	assertInvalid(tt, MakeListType(MakeUnionType()), NewList(vs, Number(1)))
}

func TestAssertTypeEmptySet(tt *testing.T) {
	vs := newTestValueStore()

	st := MakeSetType(NumberType)
	assertSubtype(st, NewSet(vs))

	// Set<> not a subtype of Set<Number>
	assertInvalid(tt, MakeSetType(MakeUnionType()), NewSet(vs, Number(1)))
}

func TestAssertTypeEmptyMap(tt *testing.T) {
	vs := newTestValueStore()

	mt := MakeMapType(NumberType, StringType)
	assertSubtype(mt, NewMap(vs))

	// Map<> not a subtype of Map<Number, Number>
	assertInvalid(tt, MakeMapType(MakeUnionType(), MakeUnionType()), NewMap(vs, Number(1), Number(2)))
}

func TestAssertTypeStructSubtypeByName(tt *testing.T) {
	namedT := MakeStructType("Name", StructField{"x", NumberType, false})
	anonT := MakeStructType("", StructField{"x", NumberType, false})
	namedV := NewStruct("Name", StructData{"x": Number(42)})
	name2V := NewStruct("foo", StructData{"x": Number(42)})
	anonV := NewStruct("", StructData{"x": Number(42)})

	assertSubtype(namedT, namedV)
	assertInvalid(tt, namedT, name2V)
	assertInvalid(tt, namedT, anonV)

	assertSubtype(anonT, namedV)
	assertSubtype(anonT, name2V)
	assertSubtype(anonT, anonV)
}

func TestAssertTypeStructSubtypeExtraFields(tt *testing.T) {
	at := MakeStructType("")
	bt := MakeStructType("", StructField{"x", NumberType, false})
	ct := MakeStructType("", StructField{"s", StringType, false}, StructField{"x", NumberType, false})
	av := NewStruct("", StructData{})
	bv := NewStruct("", StructData{"x": Number(1)})
	cv := NewStruct("", StructData{"x": Number(2), "s": String("hi")})

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
		"value":   Number(1),
		"parents": NewSet(vs),
	})
	t1 := MakeStructType("Commit",
		StructField{"parents", MakeSetType(MakeUnionType()), false},
		StructField{"value", NumberType, false},
	)
	assertSubtype(t1, c1)

	t11 := MakeStructType("Commit",
		StructField{"parents", MakeSetType(MakeRefType(MakeCycleType("Commit"))), false},
		StructField{"value", NumberType, false},
	)
	assertSubtype(t11, c1)

	c2 := NewStruct("Commit", StructData{
		"value":   Number(2),
		"parents": NewSet(vs, NewRef(c1)),
	})
	assertSubtype(t11, c2)
}

func TestAssertTypeCycleUnion(tt *testing.T) {
	// struct S {
	//   x: Cycle<S>,
	//   y: Number,
	// }
	t1 := MakeStructType("S",
		StructField{"x", MakeCycleType("S"), false},
		StructField{"y", NumberType, false},
	)
	// struct S {
	//   x: Cycle<S>,
	//   y: Number | String,
	// }
	t2 := MakeStructType("S",
		StructField{"x", MakeCycleType("S"), false},
		StructField{"y", MakeUnionType(NumberType, StringType), false},
	)

	assert.True(tt, IsSubtype(t2, t1))
	assert.False(tt, IsSubtype(t1, t2))

	// struct S {
	//   x: Cycle<S> | Number,
	//   y: Number | String,
	// }
	t3 := MakeStructType("S",
		StructField{"x", MakeUnionType(MakeCycleType("S"), NumberType), false},
		StructField{"y", MakeUnionType(NumberType, StringType), false},
	)

	assert.True(tt, IsSubtype(t3, t1))
	assert.False(tt, IsSubtype(t1, t3))

	assert.True(tt, IsSubtype(t3, t2))
	assert.False(tt, IsSubtype(t2, t3))

	// struct S {
	//   x: Cycle<S> | Number,
	//   y: Number,
	// }
	t4 := MakeStructType("S",
		StructField{"x", MakeUnionType(MakeCycleType("S"), NumberType), false},
		StructField{"y", NumberType, false},
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
	//   a: Number,
	//   b: struct {},
	// }
	t1 := MakeStructType("X",
		StructField{"a", NumberType, false},
		StructField{"b", EmptyStructType, false},
	)

	// struct {
	//   a: Number,
	// }
	t2 := MakeStructType("X", StructField{"a", NumberType, false})

	assert.False(tt, IsSubtype(t1, t2))
	assert.True(tt, IsSubtype(t2, t1))
}

func TestIsSubtypeCompoundUnion(tt *testing.T) {
	rt := MakeListType(EmptyStructType)

	st1 := MakeStructType("One", StructField{"a", NumberType, false})
	st2 := MakeStructType("Two", StructField{"b", StringType, false})
	ct := MakeListType(MakeUnionType(st1, st2))

	assert.True(tt, IsSubtype(rt, ct))
	assert.False(tt, IsSubtype(ct, rt))

	ct2 := MakeListType(MakeUnionType(st1, st2, NumberType))
	assert.False(tt, IsSubtype(rt, ct2))
	assert.False(tt, IsSubtype(ct2, rt))
}

func TestIsSubtypeOptionalFields(tt *testing.T) {
	assert := assert.New(tt)

	s1 := MakeStructType("", StructField{"a", NumberType, true})
	s2 := MakeStructType("", StructField{"a", NumberType, false})
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
	t2 := MakeStructType("", StructField{"a", NumberType, true})
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
		{Number(42), NumberType},
		{String("s"), StringType},
		{NewEmptyBlob(vs), BlobType},
		{BoolType, TypeType},
		{NewList(vs, Number(42)), MakeListType(NumberType)},
		{NewSet(vs, Number(42)), MakeSetType(NumberType)},
		{NewRef(Number(42)), MakeRefType(NumberType)},
		{NewMap(vs, Number(42), String("a")), MakeMapType(NumberType, StringType)},
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

	assertTrue(Bool(true), MakeUnionType(BoolType, NumberType))
	assertTrue(Number(123), MakeUnionType(BoolType, NumberType))
	assertFalse(String("abc"), MakeUnionType(BoolType, NumberType))
	assertFalse(String("abc"), MakeUnionType())

	assertTrue(NewList(vs), MakeListType(NumberType))
	assertTrue(NewList(vs, Number(0), Number(1), Number(2), Number(3)), MakeListType(NumberType))
	assertFalse(NewList(vs, Number(0), Number(1), Number(2), Number(3)), MakeListType(BoolType))
	assertTrue(NewList(vs, Number(0), Number(1), Number(2), Number(3)), MakeListType(MakeUnionType(NumberType, BoolType)))
	assertTrue(NewList(vs, Number(0), Bool(true)), MakeListType(MakeUnionType(NumberType, BoolType)))
	assertFalse(NewList(vs, Number(0)), MakeListType(MakeUnionType()))
	assertTrue(NewList(vs), MakeListType(MakeUnionType()))

	{
		newChunkedList := func(vals ...Value) List {
			newSequenceMetaTuple := func(v Value) metaTuple {
				seq := newListLeafSequence(vs, v)
				list := newList(seq)
				return newMetaTuple(vs.WriteValue(list), newOrderedKey(v), 1)
			}

			tuples := make([]metaTuple, len(vals))
			for i, v := range vals {
				tuples[i] = newSequenceMetaTuple(v)
			}
			return newList(newListMetaSequence(1, tuples, vs))
		}

		assertTrue(newChunkedList(Number(0), Number(1), Number(2), Number(3)), MakeListType(NumberType))
		assertFalse(newChunkedList(Number(0), Number(1), Number(2), Number(3)), MakeListType(BoolType))
		assertTrue(newChunkedList(Number(0), Number(1), Number(2), Number(3)), MakeListType(MakeUnionType(NumberType, BoolType)))
		assertTrue(newChunkedList(Number(0), Bool(true)), MakeListType(MakeUnionType(NumberType, BoolType)))
		assertFalse(newChunkedList(Number(0)), MakeListType(MakeUnionType()))
	}

	assertTrue(NewSet(vs), MakeSetType(NumberType))
	assertTrue(NewSet(vs, Number(0), Number(1), Number(2), Number(3)), MakeSetType(NumberType))
	assertFalse(NewSet(vs, Number(0), Number(1), Number(2), Number(3)), MakeSetType(BoolType))
	assertTrue(NewSet(vs, Number(0), Number(1), Number(2), Number(3)), MakeSetType(MakeUnionType(NumberType, BoolType)))
	assertTrue(NewSet(vs, Number(0), Bool(true)), MakeSetType(MakeUnionType(NumberType, BoolType)))
	assertFalse(NewSet(vs, Number(0)), MakeSetType(MakeUnionType()))
	assertTrue(NewSet(vs), MakeSetType(MakeUnionType()))

	{
		newChunkedSet := func(vals ...Value) Set {
			newSequenceMetaTuple := func(v Value) metaTuple {
				seq := newSetLeafSequence(vs, v)
				set := newSet(seq)
				return newMetaTuple(vs.WriteValue(set), newOrderedKey(v), 1)
			}

			tuples := make([]metaTuple, len(vals))
			for i, v := range vals {
				tuples[i] = newSequenceMetaTuple(v)
			}
			return newSet(newSetMetaSequence(1, tuples, vs))
		}
		assertTrue(newChunkedSet(Number(0), Number(1), Number(2), Number(3)), MakeSetType(NumberType))
		assertFalse(newChunkedSet(Number(0), Number(1), Number(2), Number(3)), MakeSetType(BoolType))
		assertTrue(newChunkedSet(Number(0), Number(1), Number(2), Number(3)), MakeSetType(MakeUnionType(NumberType, BoolType)))
		assertTrue(newChunkedSet(Number(0), Bool(true)), MakeSetType(MakeUnionType(NumberType, BoolType)))
		assertFalse(newChunkedSet(Number(0)), MakeSetType(MakeUnionType()))
	}

	assertTrue(NewMap(vs), MakeMapType(NumberType, StringType))
	assertTrue(NewMap(vs, Number(0), String("a"), Number(1), String("b")), MakeMapType(NumberType, StringType))
	assertFalse(NewMap(vs, Number(0), String("a"), Number(1), String("b")), MakeMapType(BoolType, StringType))
	assertFalse(NewMap(vs, Number(0), String("a"), Number(1), String("b")), MakeMapType(NumberType, BoolType))
	assertTrue(NewMap(vs, Number(0), String("a"), Number(1), String("b")), MakeMapType(MakeUnionType(NumberType, BoolType), StringType))
	assertTrue(NewMap(vs, Number(0), String("a"), Number(1), String("b")), MakeMapType(NumberType, MakeUnionType(BoolType, StringType)))
	assertTrue(NewMap(vs, Number(0), String("a"), Bool(true), String("b")), MakeMapType(MakeUnionType(NumberType, BoolType), StringType))
	assertTrue(NewMap(vs, Number(0), String("a"), Number(1), Bool(true)), MakeMapType(NumberType, MakeUnionType(BoolType, StringType)))
	assertFalse(NewMap(vs, Number(0), String("a")), MakeMapType(MakeUnionType(), StringType))
	assertFalse(NewMap(vs, Number(0), String("a")), MakeMapType(NumberType, MakeUnionType()))
	assertTrue(NewMap(vs), MakeMapType(MakeUnionType(), MakeUnionType()))

	{
		newChunkedMap := func(vals ...Value) Map {
			newSequenceMetaTuple := func(e mapEntry) metaTuple {
				seq := newMapLeafSequence(vs, e)
				m := newMap(seq)
				return newMetaTuple(vs.WriteValue(m), newOrderedKey(e.key), 1)
			}

			tuples := make([]metaTuple, len(vals)/2)
			for i := 0; i < len(vals); i += 2 {
				tuples[i/2] = newSequenceMetaTuple(mapEntry{vals[i], vals[i+1]})
			}
			return newMap(newMapMetaSequence(1, tuples, vs))
		}

		assertTrue(newChunkedMap(Number(0), String("a"), Number(1), String("b")), MakeMapType(NumberType, StringType))
		assertFalse(newChunkedMap(Number(0), String("a"), Number(1), String("b")), MakeMapType(BoolType, StringType))
		assertFalse(newChunkedMap(Number(0), String("a"), Number(1), String("b")), MakeMapType(NumberType, BoolType))
		assertTrue(newChunkedMap(Number(0), String("a"), Number(1), String("b")), MakeMapType(MakeUnionType(NumberType, BoolType), StringType))
		assertTrue(newChunkedMap(Number(0), String("a"), Number(1), String("b")), MakeMapType(NumberType, MakeUnionType(BoolType, StringType)))
		assertTrue(newChunkedMap(Number(0), String("a"), Bool(true), String("b")), MakeMapType(MakeUnionType(NumberType, BoolType), StringType))
		assertTrue(newChunkedMap(Number(0), String("a"), Number(1), Bool(true)), MakeMapType(NumberType, MakeUnionType(BoolType, StringType)))
		assertFalse(newChunkedMap(Number(0), String("a")), MakeMapType(MakeUnionType(), StringType))
		assertFalse(newChunkedMap(Number(0), String("a")), MakeMapType(NumberType, MakeUnionType()))
	}

	assertTrue(NewRef(Number(1)), MakeRefType(NumberType))
	assertFalse(NewRef(Number(1)), MakeRefType(BoolType))
	assertTrue(NewRef(Number(1)), MakeRefType(MakeUnionType(NumberType, BoolType)))
	assertFalse(NewRef(Number(1)), MakeRefType(MakeUnionType()))

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
		MakeStructType("Struct", StructField{"x", MakeUnionType(BoolType, NumberType), true}),
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
			"value": Number(1),
			"children": NewList(vs,
				NewStruct("Node", StructData{
					"value":    Number(2),
					"children": NewList(vs),
				}),
			),
		}),
		MakeStructType("Node",
			StructField{"value", NumberType, false},
			StructField{"children", MakeListType(MakeCycleType("Node")), false},
		),
	)

	assertFalse( // inner Node has wrong type.
		NewStruct("Node", StructData{
			"value": Number(1),
			"children": NewList(vs,
				NewStruct("Node", StructData{
					"value":    Bool(true),
					"children": NewList(vs),
				}),
			),
		}),
		MakeStructType("Node",
			StructField{"value", NumberType, false},
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
			StructField{"value", NumberType, false},
			StructField{"children", MakeListType(MakeRefType(MakeCycleType("Node"))), false},
		)

		assertTrue(
			node(Number(0), node(Number(1)), node(Number(2), node(Number(3)))),
			requiredType,
		)
		assertFalse(
			node(Number(0),
				node(Number(1)),
				node(Number(2), node(String("no"))),
			),
			requiredType,
		)
	}

	{
		t1 := MakeStructType("A",
			StructField{"a", NumberType, false},
			StructField{"b", MakeCycleType("A"), false},
		)
		t2 := MakeStructType("A",
			StructField{"a", NumberType, false},
			StructField{"b", MakeCycleType("A"), true},
		)
		v := NewStruct("A", StructData{
			"a": Number(1),
			"b": NewStruct("A", StructData{
				"a": Number(2),
			}),
		})

		assertFalse(v, t1)
		assertTrue(v, t2)
	}

	{
		t := MakeStructType("A",
			StructField{"aa", NumberType, true},
			StructField{"bb", BoolType, false},
		)
		v := NewStruct("A", StructData{
			"a": Number(1),
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
