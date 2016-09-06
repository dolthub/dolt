// Copyright 2016 Attic Labs, Inc. All rights reserved.
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
	assertSubtype(StringType, String("abc"))

	assertInvalid(t, BoolType, Number(1))
	assertInvalid(t, BoolType, String("abc"))
	assertInvalid(t, NumberType, Bool(true))
	assertInvalid(t, StringType, Number(42))
}

func TestAssertTypeValue(t *testing.T) {
	assertSubtype(ValueType, Bool(true))
	assertSubtype(ValueType, Number(1))
	assertSubtype(ValueType, String("abc"))
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
	m := NewMap(Number(0), String("a"), Number(2), String("b"))
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
	t := MakeStructType("Struct",
		[]string{"x"}, []*Type{BoolType},
	)

	v := NewStruct("Struct", StructData{"x": Bool(true)})
	assertSubtype(t, v)
	assertAll(tt, t, v)
	assertSubtype(ValueType, v)

	t2 := MakeStructType("Struct",
		[]string{"x", "y"}, []*Type{BoolType, StringType},
	)

	assert.Panics(tt, func() {
		NewStructWithType(t2, ValueSlice{Bool(true)})
	})
	assert.Panics(tt, func() {
		NewStructWithType(t2, ValueSlice{String("foo")})
	})

	assert.Panics(tt, func() {
		NewStructWithType(t2, ValueSlice{String("foo"), Bool(true)})
	})

	assert.Panics(tt, func() {
		NewStructWithType(t2, ValueSlice{Number(1), String("foo")})
	})

	assert.Panics(tt, func() {
		NewStructWithType(t2, ValueSlice{Bool(true), String("foo"), Number(1)})
	})
}

func TestAssertTypeUnion(tt *testing.T) {
	assertSubtype(MakeUnionType(NumberType), Number(42))
	assertSubtype(MakeUnionType(NumberType, StringType), Number(42))
	assertSubtype(MakeUnionType(NumberType, StringType), String("hi"))
	assertSubtype(MakeUnionType(NumberType, StringType, BoolType), Number(555))
	assertSubtype(MakeUnionType(NumberType, StringType, BoolType), String("hi"))
	assertSubtype(MakeUnionType(NumberType, StringType, BoolType), Bool(true))

	lt := MakeListType(MakeUnionType(NumberType, StringType))
	assertSubtype(lt, NewList(Number(1), String("hi"), Number(2), String("bye")))

	st := MakeSetType(StringType)
	assertSubtype(MakeUnionType(st, NumberType), Number(42))
	assertSubtype(MakeUnionType(st, NumberType), NewSet(String("a"), String("b")))

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
	namedT := MakeStructType("Name", []string{"x"}, []*Type{NumberType})
	anonT := MakeStructType("", []string{"x"}, []*Type{NumberType})
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
	at := MakeStructType("", []string{}, []*Type{})
	bt := MakeStructType("", []string{"x"}, []*Type{NumberType})
	ct := MakeStructType("", []string{"s", "x"}, []*Type{StringType, NumberType})
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
	c1 := NewStruct("Commit", StructData{
		"value":   Number(1),
		"parents": NewSet(),
	})
	t1 := MakeStructType("Commit",
		[]string{"parents", "value"},
		[]*Type{MakeSetType(MakeUnionType()), NumberType},
	)
	assertSubtype(t1, c1)

	t11 := MakeStructType("Commit",
		[]string{"parents", "value"},
		[]*Type{MakeSetType(MakeRefType(t1)), NumberType},
	)
	assertSubtype(t11, c1)

	c2 := NewStruct("Commit", StructData{
		"value":   Number(2),
		"parents": NewSet(NewRef(c1)),
	})
	assertSubtype(t11, c2)
}

func TestAssertTypeCycleUnion(tt *testing.T) {
	// struct {
	//   x: Cycle<0>,
	//   y: Number,
	// }
	t1 := MakeStructType("", []string{"x", "y"}, []*Type{
		MakeCycleType(0),
		NumberType,
	})
	// struct {
	//   x: Cycle<0>,
	//   y: Number | String,
	// }
	t2 := MakeStructType("", []string{"x", "y"}, []*Type{
		MakeCycleType(0),
		MakeUnionType(NumberType, StringType),
	})

	assert.True(tt, isSubtype(t2, t1, nil))
	assert.False(tt, isSubtype(t1, t2, nil))

	// struct {
	//   x: Cycle<0> | Number,
	//   y: Number | String,
	// }
	t3 := MakeStructType("", []string{"x", "y"}, []*Type{
		MakeUnionType(MakeCycleType(0), NumberType),
		MakeUnionType(NumberType, StringType),
	})

	assert.True(tt, isSubtype(t3, t1, nil))
	assert.False(tt, isSubtype(t1, t3, nil))

	assert.True(tt, isSubtype(t3, t2, nil))
	assert.False(tt, isSubtype(t2, t3, nil))

	// struct {
	//   x: Cycle<0> | Number,
	//   y: Number,
	// }
	t4 := MakeStructType("", []string{"x", "y"}, []*Type{
		MakeUnionType(MakeCycleType(0), NumberType),
		NumberType,
	})

	assert.True(tt, IsSubtype(t4, t1))
	assert.False(tt, IsSubtype(t1, t4))

	assert.False(tt, IsSubtype(t4, t2))
	assert.False(tt, IsSubtype(t2, t4))

	assert.True(tt, IsSubtype(t3, t4))
	assert.False(tt, IsSubtype(t4, t3))

	// struct B {
	//   b: struct C {
	//     c: Cycle<1>,
	//   },
	// }

	// struct C {
	//   c: struct B {
	//     b: Cycle<1>,
	//   },
	// }

	tb := MakeStructType("", []string{"b"}, []*Type{
		MakeStructType("", []string{"c"}, []*Type{
			MakeCycleType(1),
		}),
	})
	tc := MakeStructType("", []string{"c"}, []*Type{
		MakeStructType("", []string{"b"}, []*Type{
			MakeCycleType(1),
		}),
	})

	assert.False(tt, IsSubtype(tb, tc))
	assert.False(tt, IsSubtype(tc, tb))
}

func TestIsSubtypeEmptySruct(tt *testing.T) {
	// struct {
	//   a: Number,
	//   b: struct {},
	// }
	t1 := MakeStructType("", []string{"a", "b"}, []*Type{
		NumberType,
		EmptyStructType,
	})

	// struct {
	//   a: Number,
	// }
	t2 := MakeStructType("", []string{"a"}, []*Type{
		NumberType,
	})

	assert.False(tt, IsSubtype(t1, t2))
	assert.True(tt, IsSubtype(t2, t1))
}

func TestIsSubtypeCompoundUnion(tt *testing.T) {
	rt := MakeListType(EmptyStructType)

	st1 := MakeStructType("One", []string{"a"}, []*Type{NumberType})
	st2 := MakeStructType("Two", []string{"b"}, []*Type{StringType})
	ct := MakeListType(MakeUnionType(st1, st2))

	assert.True(tt, IsSubtype(rt, ct))
	assert.False(tt, IsSubtype(ct, rt))

	ct2 := MakeListType(MakeUnionType(st1, st2, NumberType))
	assert.False(tt, IsSubtype(rt, ct2))
	assert.False(tt, IsSubtype(ct2, rt))
}
