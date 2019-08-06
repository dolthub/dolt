// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
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
	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/store/d"
)

func assertSubtype(ctx context.Context, nbf *NomsBinFormat, t *Type, v Value) {
	is, err := IsValueSubtypeOf(nbf, v, t)
	d.PanicIfError(err)

	if !is {
		d.Panic("Invalid type. %s is not a subtype of %s", mustString(mustType(TypeOf(v)).Describe(ctx)), mustString(t.Describe(ctx)))
	}
}

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
	l, err := NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))
	assert.NoError(t, err)
	assertSubtype(context.Background(), Format_7_18, ValueType, l)
}

func TestAssertTypeBlob(t *testing.T) {
	vs := newTestValueStore()

	blob, err := NewBlob(context.Background(), vs, bytes.NewBuffer([]byte{0x00, 0x01}))
	assert.NoError(t, err)
	assertAll(t, BlobType, blob)
}

func TestAssertTypeList(tt *testing.T) {
	vs := newTestValueStore()

	listOfNumberType, err := MakeListType(FloaTType)
	l, err := NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, listOfNumberType, l)
	assertAll(tt, listOfNumberType, l)
	assertSubtype(context.Background(), Format_7_18, mustType(MakeListType(ValueType)), l)
}

func TestAssertTypeMap(tt *testing.T) {
	vs := newTestValueStore()

	mapOfNumberToStringType, err := MakeMapType(FloaTType, StringType)
	assert.NoError(tt, err)
	m, err := NewMap(context.Background(), vs, Float(0), String("a"), Float(2), String("b"))
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, mapOfNumberToStringType, m)
	assertAll(tt, mapOfNumberToStringType, m)
	assertSubtype(context.Background(), Format_7_18, mustType(MakeMapType(ValueType, ValueType)), m)
}

func TestAssertTypeSet(tt *testing.T) {
	vs := newTestValueStore()

	setOfNumberType, err := MakeSetType(FloaTType)
	assert.NoError(tt, err)
	s, err := NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, setOfNumberType, s)
	assertAll(tt, setOfNumberType, s)
	assertSubtype(context.Background(), Format_7_18, mustType(MakeSetType(ValueType)), s)
}

func TestAssertTypeType(tt *testing.T) {
	t, err := MakeSetType(FloaTType)
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, TypeType, t)
	assertAll(tt, TypeType, t)
	assertSubtype(context.Background(), Format_7_18, ValueType, t)
}

func TestAssertTypeStruct(tt *testing.T) {
	t, err := MakeStructType("Struct", StructField{"x", BoolType, false})
	assert.NoError(tt, err)

	v, err := NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)})
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, t, v)
	assertAll(tt, t, v)
	assertSubtype(context.Background(), Format_7_18, ValueType, v)
}

func TestAssertTypeUnion(tt *testing.T) {
	vs := newTestValueStore()

	assertSubtype(context.Background(), Format_7_18, mustType(MakeUnionType(FloaTType)), Float(42))
	assertSubtype(context.Background(), Format_7_18, mustType(MakeUnionType(FloaTType, StringType)), Float(42))
	assertSubtype(context.Background(), Format_7_18, mustType(MakeUnionType(FloaTType, StringType)), String("hi"))
	assertSubtype(context.Background(), Format_7_18, mustType(MakeUnionType(FloaTType, StringType, BoolType)), Float(555))
	assertSubtype(context.Background(), Format_7_18, mustType(MakeUnionType(FloaTType, StringType, BoolType)), String("hi"))
	assertSubtype(context.Background(), Format_7_18, mustType(MakeUnionType(FloaTType, StringType, BoolType)), Bool(true))

	lt, err := MakeListType(mustType(MakeUnionType(FloaTType, StringType)))
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, lt, mustList(NewList(context.Background(), vs, Float(1), String("hi"), Float(2), String("bye"))))

	st, err := MakeSetType(StringType)
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, mustType(MakeUnionType(st, FloaTType)), Float(42))
	assertSubtype(context.Background(), Format_7_18, mustType(MakeUnionType(st, FloaTType)), mustValue(NewSet(context.Background(), vs, String("a"), String("b"))))

	assertInvalid(tt, mustType(MakeUnionType()), Float(42))
	assertInvalid(tt, mustType(MakeUnionType(StringType)), Float(42))
	assertInvalid(tt, mustType(MakeUnionType(StringType, BoolType)), Float(42))
	assertInvalid(tt, mustType(MakeUnionType(st, StringType)), Float(42))
	assertInvalid(tt, mustType(MakeUnionType(st, FloaTType)), mustValue(NewSet(context.Background(), vs, Float(1), Float(2))))
}

func TestAssertConcreteTypeIsUnion(tt *testing.T) {
	assert.True(tt, IsSubtype(
		Format_7_18,
		mustType(MakeStructTypeFromFields("", FieldMap{})),
		mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": StringType})),
			mustType(MakeStructTypeFromFields("", FieldMap{"bar": StringType}))))))

	assert.False(tt, IsSubtype(
		Format_7_18,
		mustType(MakeStructTypeFromFields("", FieldMap{})),
		mustType(MakeUnionType(mustType(MakeStructTypeFromFields("", FieldMap{"foo": StringType})), FloaTType))))

	assert.True(tt, IsSubtype(
		Format_7_18,
		mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": StringType})),
			mustType(MakeStructTypeFromFields("", FieldMap{"bar": StringType})))),
		mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": StringType, "bar": StringType})),
			mustType(MakeStructTypeFromFields("", FieldMap{"bar": StringType})))),
	))

	assert.False(tt, IsSubtype(
		Format_7_18,
		mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": StringType})),
			mustType(MakeStructTypeFromFields("", FieldMap{"bar": StringType})))),
		mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": StringType, "bar": StringType})),
			FloaTType))))
}

func TestAssertTypeEmptyListUnion(tt *testing.T) {
	vs := newTestValueStore()

	ut, err := MakeUnionType()
	assert.NoError(tt, err)
	lt, err := MakeListType(ut)
	assert.NoError(tt, err)
	l, err := NewList(context.Background(), vs)
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, lt, l)
}

func TestAssertTypeEmptyList(tt *testing.T) {
	vs := newTestValueStore()

	lt, err := MakeListType(FloaTType)
	assert.NoError(tt, err)
	l, err := NewList(context.Background(), vs)
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, lt, l)

	// List<> not a subtype of List<Float>
	assertInvalid(tt, mustType(MakeListType(mustType(MakeUnionType()))), mustList(NewList(context.Background(), vs, Float(1))))
}

func TestAssertTypeEmptySet(tt *testing.T) {
	vs := newTestValueStore()

	st, err := MakeSetType(FloaTType)
	assert.NoError(tt, err)
	s, err := NewSet(context.Background(), vs)
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, st, s)

	// Set<> not a subtype of Set<Float>
	assertInvalid(tt, mustType(MakeSetType(mustType(MakeUnionType()))), mustValue(NewSet(context.Background(), vs, Float(1))))
}

func TestAssertTypeEmptyMap(tt *testing.T) {
	vs := newTestValueStore()

	mt, err := MakeMapType(FloaTType, StringType)
	assert.NoError(tt, err)
	m, err := NewMap(context.Background(), vs)
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, mt, m)

	// Map<> not a subtype of Map<Float, Float>
	m2, err := NewMap(context.Background(), vs, Float(1), Float(2))
	assert.NoError(tt, err)
	assertInvalid(tt, mustType(MakeMapType(mustType(MakeUnionType()), mustType(MakeUnionType()))), m2)
}

func TestAssertTypeStructSubtypeByName(tt *testing.T) {
	namedT, err := MakeStructType("Name", StructField{"x", FloaTType, false})
	assert.NoError(tt, err)
	anonT, err := MakeStructType("", StructField{"x", FloaTType, false})
	assert.NoError(tt, err)
	namedV, err := NewStruct(Format_7_18, "Name", StructData{"x": Float(42)})
	assert.NoError(tt, err)
	name2V, err := NewStruct(Format_7_18, "foo", StructData{"x": Float(42)})
	assert.NoError(tt, err)
	anonV, err := NewStruct(Format_7_18, "", StructData{"x": Float(42)})
	assert.NoError(tt, err)

	assertSubtype(context.Background(), Format_7_18, namedT, namedV)
	assertInvalid(tt, namedT, name2V)
	assertInvalid(tt, namedT, anonV)

	assertSubtype(context.Background(), Format_7_18, anonT, namedV)
	assertSubtype(context.Background(), Format_7_18, anonT, name2V)
	assertSubtype(context.Background(), Format_7_18, anonT, anonV)
}

func TestAssertTypeStructSubtypeExtraFields(tt *testing.T) {
	at, err := MakeStructType("")
	assert.NoError(tt, err)
	bt, err := MakeStructType("", StructField{"x", FloaTType, false})
	assert.NoError(tt, err)
	ct, err := MakeStructType("", StructField{"s", StringType, false}, StructField{"x", FloaTType, false})
	assert.NoError(tt, err)
	av, err := NewStruct(Format_7_18, "", StructData{})
	assert.NoError(tt, err)
	bv, err := NewStruct(Format_7_18, "", StructData{"x": Float(1)})
	assert.NoError(tt, err)
	cv, err := NewStruct(Format_7_18, "", StructData{"x": Float(2), "s": String("hi")})
	assert.NoError(tt, err)

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

	c1, err := NewStruct(Format_7_18, "Commit", StructData{
		"value":   Float(1),
		"parents": mustValue(NewSet(context.Background(), vs)),
	})
	assert.NoError(tt, err)
	t1, err := MakeStructType("Commit",
		StructField{"parents", mustType(MakeSetType(mustType(MakeUnionType()))), false},
		StructField{"value", FloaTType, false},
	)
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, t1, c1)

	t11, err := MakeStructType("Commit",
		StructField{"parents", mustType(MakeSetType(mustType(MakeRefType(MakeCycleType("Commit"))))), false},
		StructField{"value", FloaTType, false},
	)
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, t11, c1)

	c2, err := NewStruct(Format_7_18, "Commit", StructData{
		"value":   Float(2),
		"parents": mustValue(NewSet(context.Background(), vs, mustRef(NewRef(c1, Format_7_18)))),
	})
	assert.NoError(tt, err)
	assertSubtype(context.Background(), Format_7_18, t11, c2)
}

func TestAssertTypeCycleUnion(tt *testing.T) {
	// struct S {
	//   x: Cycle<S>,
	//   y: Float,
	// }
	t1, err := MakeStructType("S",
		StructField{"x", MakeCycleType("S"), false},
		StructField{"y", FloaTType, false},
	)
	assert.NoError(tt, err)
	// struct S {
	//   x: Cycle<S>,
	//   y: Float | String,
	// }
	t2, err := MakeStructType("S",
		StructField{"x", MakeCycleType("S"), false},
		StructField{"y", mustType(MakeUnionType(FloaTType, StringType)), false},
	)

	assert.NoError(tt, err)
	assert.True(tt, IsSubtype(Format_7_18, t2, t1))
	assert.False(tt, IsSubtype(Format_7_18, t1, t2))

	// struct S {
	//   x: Cycle<S> | Float,
	//   y: Float | String,
	// }
	t3, err := MakeStructType("S",
		StructField{"x", mustType(MakeUnionType(MakeCycleType("S"), FloaTType)), false},
		StructField{"y", mustType(MakeUnionType(FloaTType, StringType)), false},
	)

	assert.NoError(tt, err)
	assert.True(tt, IsSubtype(Format_7_18, t3, t1))
	assert.False(tt, IsSubtype(Format_7_18, t1, t3))

	assert.True(tt, IsSubtype(Format_7_18, t3, t2))
	assert.False(tt, IsSubtype(Format_7_18, t2, t3))

	// struct S {
	//   x: Cycle<S> | Float,
	//   y: Float,
	// }
	t4, err := MakeStructType("S",
		StructField{"x", mustType(MakeUnionType(MakeCycleType("S"), FloaTType)), false},
		StructField{"y", FloaTType, false},
	)

	assert.NoError(tt, err)
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

	tb, err := MakeStructType("A",
		StructField{
			"b",
			mustType(MakeStructType("B", StructField{"c", MakeCycleType("A"), false})),
			false,
		},
	)
	assert.NoError(tt, err)

	tc, err := MakeStructType("A",
		StructField{
			"c",
			mustType(MakeStructType("B", StructField{"b", MakeCycleType("A"), false})),
			false,
		},
	)
	assert.NoError(tt, err)

	assert.False(tt, IsSubtype(Format_7_18, tb, tc))
	assert.False(tt, IsSubtype(Format_7_18, tc, tb))
}

func TestIsSubtypeEmptySruct(tt *testing.T) {
	// struct {
	//   a: Float,
	//   b: struct {},
	// }
	t1, err := MakeStructType("X",
		StructField{"a", FloaTType, false},
		StructField{"b", EmptyStructType, false},
	)
	assert.NoError(tt, err)

	// struct {
	//   a: Float,
	// }
	t2, err := MakeStructType("X", StructField{"a", FloaTType, false})
	assert.NoError(tt, err)

	assert.False(tt, IsSubtype(Format_7_18, t1, t2))
	assert.True(tt, IsSubtype(Format_7_18, t2, t1))
}

func TestIsSubtypeCompoundUnion(tt *testing.T) {
	rt, err := MakeListType(EmptyStructType)
	assert.NoError(tt, err)

	st1, err := MakeStructType("One", StructField{"a", FloaTType, false})
	assert.NoError(tt, err)
	st2, err := MakeStructType("Two", StructField{"b", StringType, false})
	assert.NoError(tt, err)
	ct, err := MakeListType(mustType(MakeUnionType(st1, st2)))
	assert.NoError(tt, err)

	assert.True(tt, IsSubtype(Format_7_18, rt, ct))
	assert.False(tt, IsSubtype(Format_7_18, ct, rt))

	ct2, err := MakeListType(mustType(MakeUnionType(st1, st2, FloaTType)))
	assert.NoError(tt, err)
	assert.False(tt, IsSubtype(Format_7_18, rt, ct2))
	assert.False(tt, IsSubtype(Format_7_18, ct2, rt))
}

func TestIsSubtypeOptionalFields(tt *testing.T) {
	assert := assert.New(tt)

	s1, err := MakeStructType("", StructField{"a", FloaTType, true})
	assert.NoError(err)
	s2, err := MakeStructType("", StructField{"a", FloaTType, false})
	assert.NoError(err)
	assert.True(IsSubtype(Format_7_18, s1, s2))
	assert.False(IsSubtype(Format_7_18, s2, s1))

	s3, err := MakeStructType("", StructField{"a", StringType, false})
	assert.False(IsSubtype(Format_7_18, s1, s3))
	assert.False(IsSubtype(Format_7_18, s3, s1))

	s4, err := MakeStructType("", StructField{"a", StringType, true})
	assert.False(IsSubtype(Format_7_18, s1, s4))
	assert.False(IsSubtype(Format_7_18, s4, s1))

	test := func(t1s, t2s string, exp1, exp2 bool) {
		t1, err := makeTestStructTypeFromFieldNames(t1s)
		assert.NoError(err)
		t2, err := makeTestStructTypeFromFieldNames(t2s)
		assert.NoError(err)
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

	t1, err := MakeStructType("", StructField{"a", BoolType, true})
	assert.NoError(err)
	t2, err := MakeStructType("", StructField{"a", FloaTType, true})
	assert.NoError(err)
	assert.False(IsSubtype(Format_7_18, t1, t2))
	assert.False(IsSubtype(Format_7_18, t2, t1))
}

func makeTestStructTypeFromFieldNames(s string) (*Type, error) {
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

func makeTestStructFromFieldNames(s string) (Struct, error) {
	t, err := makeTestStructTypeFromFieldNames(s)

	if err != nil {
		return EmptyStruct(Format_7_18), err
	}

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
		t1, err := makeTestStructTypeFromFieldNames(t1s)
		assert.NoError(err)
		t2, err := makeTestStructTypeFromFieldNames(t2s)
		assert.NoError(err)
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

	allTypes := []struct {
		v Value
		t *Type
	}{
		{Bool(true), BoolType},
		{Float(42), FloaTType},
		{String("s"), StringType},
		{mustBlob(NewEmptyBlob(vs)), BlobType},
		{BoolType, TypeType},
		{mustList(NewList(context.Background(), vs, Float(42))), mustType(MakeListType(FloaTType))},
		{mustValue(NewSet(context.Background(), vs, Float(42))), mustType(MakeSetType(FloaTType))},
		{mustRef(NewRef(Float(42), Format_7_18)), mustType(MakeRefType(FloaTType))},
		{mustValue(NewMap(context.Background(), vs, Float(42), String("a"))), mustType(MakeMapType(FloaTType, StringType))},
		{mustValue(NewStruct(Format_7_18, "A", StructData{})), mustType(MakeStructType("A"))},
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

	assertTrue(Bool(true), mustType((MakeUnionType(BoolType, FloaTType))))
	assertTrue(Float(123), mustType(MakeUnionType(BoolType, FloaTType)))
	assertFalse(String("abc"), mustType(MakeUnionType(BoolType, FloaTType)))
	assertFalse(String("abc"), mustType(MakeUnionType()))

	assertTrue(mustList(NewList(context.Background(), vs)), mustType(MakeListType(FloaTType)))
	assertTrue(mustList(NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))), mustType(MakeListType(FloaTType)))
	assertFalse(mustList(NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))), mustType(MakeListType(BoolType)))
	assertTrue(mustList(NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))), mustType(MakeListType(mustType(MakeUnionType(FloaTType, BoolType)))))
	assertTrue(mustList(NewList(context.Background(), vs, Float(0), Bool(true))), mustType(MakeListType(mustType(MakeUnionType(FloaTType, BoolType)))))
	assertFalse(mustList(NewList(context.Background(), vs, Float(0))), mustType(MakeListType(mustType(MakeUnionType()))))
	assertTrue(mustList(NewList(context.Background(), vs)), mustType(MakeListType(mustType(MakeUnionType()))))

	{
		newChunkedList := func(vals ...Value) List {
			newSequenceMetaTuple := func(v Value) metaTuple {
				seq, err := newListLeafSequence(vs, v)
				assert.NoError(err)
				list := newList(seq)
				ref, err := vs.WriteValue(context.Background(), list)
				assert.NoError(err)
				ordKey, err := newOrderedKey(v, Format_7_18)
				assert.NoError(err)
				mt, err := newMetaTuple(ref, ordKey, 1)
				assert.NoError(err)
				return mt
			}

			tuples := make([]metaTuple, len(vals))
			for i, v := range vals {
				tuples[i] = newSequenceMetaTuple(v)
			}
			mseq, err := newListMetaSequence(1, tuples, vs)
			assert.NoError(err)
			return newList(mseq)
		}

		assertTrue(newChunkedList(Float(0), Float(1), Float(2), Float(3)), mustType(MakeListType(FloaTType)))
		assertFalse(newChunkedList(Float(0), Float(1), Float(2), Float(3)), mustType(MakeListType(BoolType)))
		assertTrue(newChunkedList(Float(0), Float(1), Float(2), Float(3)), mustType(MakeListType(mustType(MakeUnionType(FloaTType, BoolType)))))
		assertTrue(newChunkedList(Float(0), Bool(true)), mustType(MakeListType(mustType(MakeUnionType(FloaTType, BoolType)))))
		assertFalse(newChunkedList(Float(0)), mustType(MakeListType(mustType(MakeUnionType()))))
	}

	assertTrue(mustValue(NewSet(context.Background(), vs)), mustType(MakeSetType(FloaTType)))
	assertTrue(mustValue(NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))), mustType(MakeSetType(FloaTType)))
	assertFalse(mustValue(NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))), mustType(MakeSetType(BoolType)))
	assertTrue(mustValue(NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))), mustType(MakeSetType(mustType(MakeUnionType(FloaTType, BoolType)))))
	assertTrue(mustValue(NewSet(context.Background(), vs, Float(0), Bool(true))), mustType(MakeSetType(mustType(MakeUnionType(FloaTType, BoolType)))))
	assertFalse(mustValue(NewSet(context.Background(), vs, Float(0))), mustType(MakeSetType(mustType(MakeUnionType()))))
	assertTrue(mustValue(NewSet(context.Background(), vs)), mustType(MakeSetType(mustType(MakeUnionType()))))

	{
		newChunkedSet := func(vals ...Value) Set {
			newSequenceMetaTuple := func(v Value) metaTuple {
				seq, err := newSetLeafSequence(vs, v)
				assert.NoError(err)
				set := newSet(seq)
				ref, err := vs.WriteValue(context.Background(), set)
				assert.NoError(err)
				ordKey, err := newOrderedKey(v, Format_7_18)
				assert.NoError(err)
				mt, err := newMetaTuple(ref, ordKey, 1)
				assert.NoError(err)
				return mt
			}

			tuples := make([]metaTuple, len(vals))
			for i, v := range vals {
				tuples[i] = newSequenceMetaTuple(v)
			}
			return newSet(mustOrdSeq(newSetMetaSequence(1, tuples, vs)))
		}
		assertTrue(newChunkedSet(Float(0), Float(1), Float(2), Float(3)), mustType(MakeSetType(FloaTType)))
		assertFalse(newChunkedSet(Float(0), Float(1), Float(2), Float(3)), mustType(MakeSetType(BoolType)))
		assertTrue(newChunkedSet(Float(0), Float(1), Float(2), Float(3)), mustType(MakeSetType(mustType(MakeUnionType(FloaTType, BoolType)))))
		assertTrue(newChunkedSet(Float(0), Bool(true)), mustType(MakeSetType(mustType(MakeUnionType(FloaTType, BoolType)))))
		assertFalse(newChunkedSet(Float(0)), mustType(MakeSetType(mustType(MakeUnionType()))))
	}

	assertTrue(mustMap(NewMap(context.Background(), vs)), mustType(MakeMapType(FloaTType, StringType)))
	assertTrue(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b"))), mustType(MakeMapType(FloaTType, StringType)))
	assertFalse(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b"))), mustType(MakeMapType(BoolType, StringType)))
	assertFalse(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b"))), mustType(MakeMapType(FloaTType, BoolType)))
	assertTrue(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b"))), mustType(MakeMapType(mustType(MakeUnionType(FloaTType, BoolType)), StringType)))
	assertTrue(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b"))), mustType(MakeMapType(FloaTType, mustType(MakeUnionType(BoolType, StringType)))))
	assertTrue(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Bool(true), String("b"))), mustType(MakeMapType(mustType(MakeUnionType(FloaTType, BoolType)), StringType)))
	assertTrue(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), Bool(true))), mustType(MakeMapType(FloaTType, mustType(MakeUnionType(BoolType, StringType)))))
	assertFalse(mustMap(NewMap(context.Background(), vs, Float(0), String("a"))), mustType(MakeMapType(mustType(MakeUnionType()), StringType)))
	assertFalse(mustMap(NewMap(context.Background(), vs, Float(0), String("a"))), mustType(MakeMapType(FloaTType, mustType(MakeUnionType()))))
	assertTrue(mustMap(NewMap(context.Background(), vs)), mustType(MakeMapType(mustType(MakeUnionType()), mustType(MakeUnionType()))))

	{
		newChunkedMap := func(vals ...Value) Map {
			newSequenceMetaTuple := func(e mapEntry) metaTuple {
				seq, err := newMapLeafSequence(vs, e)
				assert.NoError(err)
				m := newMap(seq)
				ref, err := vs.WriteValue(context.Background(), m)
				assert.NoError(err)
				ordKey, err := newOrderedKey(e.key, Format_7_18)
				assert.NoError(err)
				mt, err := newMetaTuple(ref, ordKey, 1)
				assert.NoError(err)
				return mt
			}

			tuples := make([]metaTuple, len(vals)/2)
			for i := 0; i < len(vals); i += 2 {
				tuples[i/2] = newSequenceMetaTuple(mapEntry{vals[i], vals[i+1]})
			}
			return newMap(mustOrdSeq(newMapMetaSequence(1, tuples, vs)))
		}

		assertTrue(newChunkedMap(Float(0), String("a"), Float(1), String("b")), mustType(MakeMapType(FloaTType, StringType)))
		assertFalse(newChunkedMap(Float(0), String("a"), Float(1), String("b")), mustType(MakeMapType(BoolType, StringType)))
		assertFalse(newChunkedMap(Float(0), String("a"), Float(1), String("b")), mustType(MakeMapType(FloaTType, BoolType)))
		assertTrue(newChunkedMap(Float(0), String("a"), Float(1), String("b")), mustType(MakeMapType(mustType(MakeUnionType(FloaTType, BoolType)), StringType)))
		assertTrue(newChunkedMap(Float(0), String("a"), Float(1), String("b")), mustType(MakeMapType(FloaTType, mustType(MakeUnionType(BoolType, StringType)))))
		assertTrue(newChunkedMap(Float(0), String("a"), Bool(true), String("b")), mustType(MakeMapType(mustType(MakeUnionType(FloaTType, BoolType)), StringType)))
		assertTrue(newChunkedMap(Float(0), String("a"), Float(1), Bool(true)), mustType(MakeMapType(FloaTType, mustType(MakeUnionType(BoolType, StringType)))))
		assertFalse(newChunkedMap(Float(0), String("a")), mustType(MakeMapType(mustType(MakeUnionType()), StringType)))
		assertFalse(newChunkedMap(Float(0), String("a")), mustType(MakeMapType(FloaTType, mustType(MakeUnionType()))))
	}

	assertTrue(mustRef(NewRef(Float(1), Format_7_18)), mustType(MakeRefType(FloaTType)))
	assertFalse(mustRef(NewRef(Float(1), Format_7_18)), mustType(MakeRefType(BoolType)))
	assertTrue(mustRef(NewRef(Float(1), Format_7_18)), mustType(MakeRefType(mustType(MakeUnionType(FloaTType, BoolType)))))
	assertFalse(mustRef(NewRef(Float(1), Format_7_18)), mustType(MakeRefType(mustType(MakeUnionType()))))

	assertTrue(
		mustValue(NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct", StructField{"x", BoolType, false})),
	)
	assertTrue(
		mustValue(NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct", StructField{"x", BoolType, true})),
	)
	assertTrue(
		mustValue(NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct")),
	)
	assertTrue(
		mustValue(NewStruct(Format_7_18, "Struct", StructData{})),
		mustType(MakeStructType("Struct")),
	)
	assertFalse(
		mustValue(NewStruct(Format_7_18, "", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct")),
	)
	assertFalse(
		mustValue(NewStruct(Format_7_18, "struct", StructData{"x": Bool(true)})), // lower case name
		mustType(MakeStructType("Struct")),
	)
	assertTrue(
		mustValue(NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct", StructField{"x", mustType(MakeUnionType(BoolType, FloaTType)), true})),
	)
	assertTrue(
		mustValue(NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct", StructField{"y", BoolType, true})),
	)
	assertFalse(
		mustValue(NewStruct(Format_7_18, "Struct", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct", StructField{"x", StringType, true})),
	)

	assertTrue(
		mustValue(NewStruct(Format_7_18, "Node", StructData{
			"value": Float(1),
			"children": mustList(NewList(context.Background(), vs,
				mustValue(NewStruct(Format_7_18, "Node", StructData{
					"value":    Float(2),
					"children": mustList(NewList(context.Background(), vs)),
				})),
			)),
		})),
		mustType(MakeStructType("Node",
			StructField{"value", FloaTType, false},
			StructField{"children", mustType(MakeListType(MakeCycleType("Node"))), false},
		)),
	)

	assertFalse( // inner Node has wrong type.
		mustValue(NewStruct(Format_7_18, "Node", StructData{
			"value": Float(1),
			"children": mustList(NewList(context.Background(), vs,
				mustValue(NewStruct(Format_7_18, "Node", StructData{
					"value":    Bool(true),
					"children": mustList(NewList(context.Background(), vs)),
				}))),
			),
		})),
		mustType(MakeStructType("Node",
			StructField{"value", FloaTType, false},
			StructField{"children", mustType(MakeListType(MakeCycleType("Node"))), false},
		)),
	)

	{
		node := func(value Value, children ...Value) Value {
			childrenAsRefs := make(ValueSlice, len(children))
			for i, c := range children {
				var err error
				childrenAsRefs[i], err = NewRef(c, Format_7_18)
				assert.NoError(err)
			}
			rv, err := NewStruct(Format_7_18, "Node", StructData{
				"value":    value,
				"children": mustList(NewList(context.Background(), vs, childrenAsRefs...)),
			})
			assert.NoError(err)
			return rv
		}

		requiredType, err := MakeStructType("Node",
			StructField{"value", FloaTType, false},
			StructField{"children", mustType(MakeListType(mustType(MakeRefType(MakeCycleType("Node"))))), false},
		)
		assert.NoError(err)

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
		t1, err := MakeStructType("A",
			StructField{"a", FloaTType, false},
			StructField{"b", MakeCycleType("A"), false},
		)
		assert.NoError(err)
		t2, err := MakeStructType("A",
			StructField{"a", FloaTType, false},
			StructField{"b", MakeCycleType("A"), true},
		)
		assert.NoError(err)
		v, err := NewStruct(Format_7_18, "A", StructData{
			"a": Float(1),
			"b": mustValue(NewStruct(Format_7_18, "A", StructData{
				"a": Float(2),
			})),
		})
		assert.NoError(err)

		assertFalse(v, t1)
		assertTrue(v, t2)
	}

	{
		t, err := MakeStructType("A",
			StructField{"aa", FloaTType, true},
			StructField{"bb", BoolType, false},
		)
		assert.NoError(err)
		v, err := NewStruct(Format_7_18, "A", StructData{
			"a": Float(1),
			"b": Bool(true),
		})
		assert.NoError(err)
		assertFalse(v, t)
	}
}

func TestIsValueSubtypeOfDetails(tt *testing.T) {
	a := assert.New(tt)

	test := func(vString, tString string, exp1, exp2 bool) {
		v, err := makeTestStructFromFieldNames(vString)
		assert.NoError(tt, err)
		t, err := makeTestStructTypeFromFieldNames(tString)
		assert.NoError(tt, err)
		isSub, hasExtra, err := IsValueSubtypeOfDetails(Format_7_18, v, t)
		assert.NoError(tt, err)
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
