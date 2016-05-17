package types

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func validateType(t *Type, v Value) {
	assertType(t, v)
}

func assertInvalid(tt *testing.T, t *Type, v Value) {
	assert := assert.New(tt)
	assert.Panics(func() {
		assertType(t, v)
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
			validateType(at, v)
		} else {
			assertInvalid(tt, at, v)
		}
	}
}

func TestAssertTypePrimitives(t *testing.T) {
	validateType(BoolType, Bool(true))
	validateType(BoolType, Bool(false))
	validateType(NumberType, Number(42))
	validateType(StringType, NewString("abc"))

	assertInvalid(t, BoolType, Number(1))
	assertInvalid(t, BoolType, NewString("abc"))
	assertInvalid(t, NumberType, Bool(true))
	assertInvalid(t, StringType, Number(42))
}

func TestAssertTypeValue(t *testing.T) {
	validateType(ValueType, Bool(true))
	validateType(ValueType, Number(1))
	validateType(ValueType, NewString("abc"))
	l := NewList(Number(0), Number(1), Number(2), Number(3))
	validateType(ValueType, l)
}

func TestAssertTypeBlob(t *testing.T) {
	blob := NewBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	assertAll(t, BlobType, blob)
}

func TestAssertTypeList(tt *testing.T) {
	listOfNumberType := MakeListType(NumberType)
	l := NewList(Number(0), Number(1), Number(2), Number(3))
	validateType(listOfNumberType, l)
	assertAll(tt, listOfNumberType, l)
	validateType(MakeListType(ValueType), l)
}

func TestAssertTypeMap(tt *testing.T) {
	mapOfNumberToStringType := MakeMapType(NumberType, StringType)
	m := NewMap(Number(0), NewString("a"), Number(2), NewString("b"))
	validateType(mapOfNumberToStringType, m)
	assertAll(tt, mapOfNumberToStringType, m)
	validateType(MakeMapType(ValueType, ValueType), m)
}

func TestAssertTypeSet(tt *testing.T) {
	setOfNumberType := MakeSetType(NumberType)
	s := NewSet(Number(0), Number(1), Number(2), Number(3))
	validateType(setOfNumberType, s)
	assertAll(tt, setOfNumberType, s)
	validateType(MakeSetType(ValueType), s)
}

func TestAssertTypeType(tt *testing.T) {
	t := MakeSetType(NumberType)
	validateType(TypeType, t)
	assertAll(tt, TypeType, t)
	validateType(ValueType, t)
}

func TestAssertTypeStruct(tt *testing.T) {
	t := MakeStructType("Struct", TypeMap{
		"x": BoolType,
	})

	v := NewStruct("Struct", structData{"x": Bool(true)})
	validateType(t, v)
	assertAll(tt, t, v)
	validateType(ValueType, v)
}

func TestAssertTypeUnion(tt *testing.T) {
	validateType(MakeUnionType(NumberType), Number(42))
	validateType(MakeUnionType(NumberType, StringType), Number(42))
	validateType(MakeUnionType(NumberType, StringType), NewString("hi"))
	validateType(MakeUnionType(NumberType, StringType, BoolType), Number(555))
	validateType(MakeUnionType(NumberType, StringType, BoolType), NewString("hi"))
	validateType(MakeUnionType(NumberType, StringType, BoolType), Bool(true))

	lt := MakeListType(MakeUnionType(NumberType, StringType))
	validateType(lt, NewList(Number(1), NewString("hi"), Number(2), NewString("bye")))

	st := MakeSetType(StringType)
	validateType(MakeUnionType(st, NumberType), Number(42))
	validateType(MakeUnionType(st, NumberType), NewSet(NewString("a"), NewString("b")))

	assertInvalid(tt, MakeUnionType(), Number(42))
	assertInvalid(tt, MakeUnionType(StringType), Number(42))
	assertInvalid(tt, MakeUnionType(StringType, BoolType), Number(42))
	assertInvalid(tt, MakeUnionType(st, StringType), Number(42))
	assertInvalid(tt, MakeUnionType(st, NumberType), NewSet(Number(1), Number(2)))
}

func TestAssertTypeEmptyListUnion(tt *testing.T) {
	lt := MakeListType(MakeUnionType())
	validateType(lt, NewList())
}

func TestAssertTypeEmptyList(tt *testing.T) {
	lt := MakeListType(NumberType)
	validateType(lt, NewList())
}

func TestAssertTypeEmptySet(tt *testing.T) {
	st := MakeSetType(NumberType)
	validateType(st, NewSet())
}

func TestAssertTypeEmptyMap(tt *testing.T) {
	mt := MakeMapType(NumberType, StringType)
	validateType(mt, NewMap())
}
