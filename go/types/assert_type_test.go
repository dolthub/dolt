// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/testify/assert"
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
	assertSubtype(StringType, NewString("abc"))

	assertInvalid(t, BoolType, Number(1))
	assertInvalid(t, BoolType, NewString("abc"))
	assertInvalid(t, NumberType, Bool(true))
	assertInvalid(t, StringType, Number(42))
}

func TestAssertTypeValue(t *testing.T) {
	assertSubtype(ValueType, Bool(true))
	assertSubtype(ValueType, Number(1))
	assertSubtype(ValueType, NewString("abc"))
	l := NewList(Number(0), Number(1), Number(2), Number(3))
	assertSubtype(ValueType, l)
}

func TestAssertTypeBlob(t *testing.T) {
	blob := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	assertAll(t, BlobType, blob)
}

func TestAssertTypeList(tt *testing.T) {
	listOfNumberType := MakeListType(NumberType)
	l := NewList(Number(0), Number(1), Number(2), Number(3))
	assertSubtype(listOfNumberType, l)
	assertAll(tt, listOfNumberType, l)
	assertSubtype(MakeListType(ValueType), l)
}

func TestAssertTypeMap(tt *testing.T) {
	mapOfNumberToStringType := MakeMapType(NumberType, StringType)
	m := NewMap(Number(0), NewString("a"), Number(2), NewString("b"))
	assertSubtype(mapOfNumberToStringType, m)
	assertAll(tt, mapOfNumberToStringType, m)
	assertSubtype(MakeMapType(ValueType, ValueType), m)
}

func TestAssertTypeSet(tt *testing.T) {
	setOfNumberType := MakeSetType(NumberType)
	s := NewSet(Number(0), Number(1), Number(2), Number(3))
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
	t := MakeStructType("Struct", TypeMap{
		"x": BoolType,
	})

	v := NewStruct("Struct", structData{"x": Bool(true)})
	assertSubtype(t, v)
	assertAll(tt, t, v)
	assertSubtype(ValueType, v)
}

func TestAssertTypeUnion(tt *testing.T) {
	assertSubtype(MakeUnionType(NumberType), Number(42))
	assertSubtype(MakeUnionType(NumberType, StringType), Number(42))
	assertSubtype(MakeUnionType(NumberType, StringType), NewString("hi"))
	assertSubtype(MakeUnionType(NumberType, StringType, BoolType), Number(555))
	assertSubtype(MakeUnionType(NumberType, StringType, BoolType), NewString("hi"))
	assertSubtype(MakeUnionType(NumberType, StringType, BoolType), Bool(true))

	lt := MakeListType(MakeUnionType(NumberType, StringType))
	assertSubtype(lt, NewList(Number(1), NewString("hi"), Number(2), NewString("bye")))

	st := MakeSetType(StringType)
	assertSubtype(MakeUnionType(st, NumberType), Number(42))
	assertSubtype(MakeUnionType(st, NumberType), NewSet(NewString("a"), NewString("b")))

	assertInvalid(tt, MakeUnionType(), Number(42))
	assertInvalid(tt, MakeUnionType(StringType), Number(42))
	assertInvalid(tt, MakeUnionType(StringType, BoolType), Number(42))
	assertInvalid(tt, MakeUnionType(st, StringType), Number(42))
	assertInvalid(tt, MakeUnionType(st, NumberType), NewSet(Number(1), Number(2)))
}

func TestAssertTypeEmptyListUnion(tt *testing.T) {
	lt := MakeListType(MakeUnionType())
	assertSubtype(lt, NewList())
}

func TestAssertTypeEmptyList(tt *testing.T) {
	lt := MakeListType(NumberType)
	assertSubtype(lt, NewList())

	// List<> not a subtype of List<Number>
	assertInvalid(tt, MakeListType(MakeUnionType()), NewList(Number(1)))
}

func TestAssertTypeEmptySet(tt *testing.T) {
	st := MakeSetType(NumberType)
	assertSubtype(st, NewSet())

	// Set<> not a subtype of Set<Number>
	assertInvalid(tt, MakeSetType(MakeUnionType()), NewSet(Number(1)))
}

func TestAssertTypeEmptyMap(tt *testing.T) {
	mt := MakeMapType(NumberType, StringType)
	assertSubtype(mt, NewMap())

	// Map<> not a subtype of Map<Number, Number>
	assertInvalid(tt, MakeMapType(MakeUnionType(), MakeUnionType()), NewMap(Number(1), Number(2)))
}

func TestAssertTypeStructSubtypeByName(tt *testing.T) {
	namedT := MakeStructType("Name", TypeMap{"x": NumberType})
	anonT := MakeStructType("", TypeMap{"x": NumberType})
	namedV := NewStruct("Name", structData{"x": Number(42)})
	name2V := NewStruct("foo", structData{"x": Number(42)})
	anonV := NewStruct("", structData{"x": Number(42)})

	assertSubtype(namedT, namedV)
	assertInvalid(tt, namedT, name2V)
	assertInvalid(tt, namedT, anonV)

	assertSubtype(anonT, namedV)
	assertSubtype(anonT, name2V)
	assertSubtype(anonT, anonV)
}

func TestAssertTypeStructSubtypeExtraFields(tt *testing.T) {
	at := MakeStructType("", TypeMap{})
	bt := MakeStructType("", TypeMap{"x": NumberType})
	ct := MakeStructType("", TypeMap{"x": NumberType, "s": StringType})
	av := NewStruct("", structData{})
	bv := NewStruct("", structData{"x": Number(1)})
	cv := NewStruct("", structData{"x": Number(2), "s": NewString("hi")})

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
	c1 := NewStruct("Commit", structData{
		"value":   Number(1),
		"parents": NewSet(),
	})
	t1 := MakeStructType("Commit", TypeMap{
		"value":   NumberType,
		"parents": MakeSetType(MakeUnionType()),
	})
	assertSubtype(t1, c1)

	t11 := MakeStructType("Commit", TypeMap{
		"value":   NumberType,
		"parents": NumberType, // placeholder MakeSetType(MakeRefType(NumberType /* placeholder */)),
	})
	t11.Desc.(StructDesc).SetField("parents", MakeSetType(MakeRefType(t1)))
	assertSubtype(t11, c1)

	c2 := NewStruct("Commit", structData{
		"value":   Number(2),
		"parents": NewSet(NewRef(c1)),
	})
	assertSubtype(t11, c2)

	// t3 :=
}
