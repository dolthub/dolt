// Copyright 2019 Dolthub, Inc.
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
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/d"
)

func assertSubtype(ctx context.Context, nbf *NomsBinFormat, t *Type, v Value) {
	is, err := IsValueSubtypeOf(nbf, v, t)
	d.PanicIfError(err)

	if !is {
		d.Panic("Invalid type. %s is not a subtype of %s", mustString(mustType(TypeOf(v)).Describe(ctx)), mustString(t.Describe(ctx)))
	}
}

func assertInvalid(tt *testing.T, nbf *NomsBinFormat, t *Type, v Value) {
	assert := assert.New(tt)
	assert.Panics(func() {
		assertSubtype(context.Background(), nbf, t, v)
	})
}

func assertAll(tt *testing.T, nbf *NomsBinFormat, t *Type, v Value) {
	allTypes := []*Type{
		PrimitiveTypeMap[BoolKind],
		PrimitiveTypeMap[FloatKind],
		PrimitiveTypeMap[StringKind],
		PrimitiveTypeMap[BlobKind],
		PrimitiveTypeMap[TypeKind],
		PrimitiveTypeMap[ValueKind],
		PrimitiveTypeMap[UUIDKind],
		PrimitiveTypeMap[IntKind],
		PrimitiveTypeMap[UintKind],
		PrimitiveTypeMap[InlineBlobKind],
	}

	for _, at := range allTypes {
		if at == PrimitiveTypeMap[ValueKind] || t.Equals(at) {
			assertSubtype(context.Background(), nbf, at, v)
		} else {
			assertInvalid(tt, nbf, at, v)
		}
	}
}

func TestAssertTypePrimitives(t *testing.T) {
	assertSubtype(context.Background(), Format_Default, PrimitiveTypeMap[BoolKind], Bool(true))
	assertSubtype(context.Background(), Format_Default, PrimitiveTypeMap[BoolKind], Bool(false))
	assertSubtype(context.Background(), Format_Default, PrimitiveTypeMap[FloatKind], Float(42))
	assertSubtype(context.Background(), Format_Default, PrimitiveTypeMap[StringKind], String("abc"))
	assertSubtype(context.Background(), Format_Default, PrimitiveTypeMap[UUIDKind], UUID(uuid.Must(uuid.NewUUID())))
	assertSubtype(context.Background(), Format_Default, PrimitiveTypeMap[IntKind], Int(-1))
	assertSubtype(context.Background(), Format_Default, PrimitiveTypeMap[UintKind], Uint(0xffffffffffffffff))
	assertSubtype(context.Background(), Format_Default, PrimitiveTypeMap[InlineBlobKind], InlineBlob{})

	assertInvalid(t, Format_Default, PrimitiveTypeMap[BoolKind], Float(1))
	assertInvalid(t, Format_Default, PrimitiveTypeMap[BoolKind], String("abc"))
	assertInvalid(t, Format_Default, PrimitiveTypeMap[FloatKind], Bool(true))
	assertInvalid(t, Format_Default, PrimitiveTypeMap[StringKind], UUID(uuid.Must(uuid.NewUUID())))
	assertInvalid(t, Format_Default, PrimitiveTypeMap[UUIDKind], String("abs"))
	assertInvalid(t, Format_Default, PrimitiveTypeMap[IntKind], Float(-1))
	assertInvalid(t, Format_Default, PrimitiveTypeMap[UintKind], Float(500))
	assertInvalid(t, Format_Default, PrimitiveTypeMap[InlineBlobKind], Int(742))
}

func TestAssertTypeValue(t *testing.T) {
	vs := newTestValueStore()

	assertSubtype(context.Background(), vs.Format(), PrimitiveTypeMap[ValueKind], Bool(true))
	assertSubtype(context.Background(), vs.Format(), PrimitiveTypeMap[ValueKind], Float(1))
	assertSubtype(context.Background(), vs.Format(), PrimitiveTypeMap[ValueKind], String("abc"))
	l, err := NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))
	require.NoError(t, err)
	assertSubtype(context.Background(), vs.Format(), PrimitiveTypeMap[ValueKind], l)
}

func TestAssertTypeBlob(t *testing.T) {
	vs := newTestValueStore()

	blob, err := NewBlob(context.Background(), vs, bytes.NewBuffer([]byte{0x00, 0x01}))
	require.NoError(t, err)
	assertAll(t, vs.Format(), PrimitiveTypeMap[BlobKind], blob)
}

func TestAssertTypeList(tt *testing.T) {
	vs := newTestValueStore()

	listOfNumberType, err := MakeListType(PrimitiveTypeMap[FloatKind])
	require.NoError(tt, err)
	l, err := NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), listOfNumberType, l)
	assertAll(tt, vs.Format(), listOfNumberType, l)
	assertSubtype(context.Background(), vs.Format(), mustType(MakeListType(PrimitiveTypeMap[ValueKind])), l)
}

func TestAssertTypeMap(tt *testing.T) {
	vs := newTestValueStore()

	mapOfNumberToStringType, err := MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])
	require.NoError(tt, err)
	m, err := NewMap(context.Background(), vs, Float(0), String("a"), Float(2), String("b"))
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), mapOfNumberToStringType, m)
	assertAll(tt, vs.Format(), mapOfNumberToStringType, m)
	assertSubtype(context.Background(), vs.Format(), mustType(MakeMapType(PrimitiveTypeMap[ValueKind], PrimitiveTypeMap[ValueKind])), m)
}

func TestAssertTypeSet(tt *testing.T) {
	vs := newTestValueStore()

	setOfNumberType, err := MakeSetType(PrimitiveTypeMap[FloatKind])
	require.NoError(tt, err)
	s, err := NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), setOfNumberType, s)
	assertAll(tt, vs.Format(), setOfNumberType, s)
	assertSubtype(context.Background(), vs.Format(), mustType(MakeSetType(PrimitiveTypeMap[ValueKind])), s)
}

func TestAssertTypeType(tt *testing.T) {
	vs := newTestValueStore()
	t, err := MakeSetType(PrimitiveTypeMap[FloatKind])
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), PrimitiveTypeMap[TypeKind], t)
	assertAll(tt, vs.Format(), PrimitiveTypeMap[TypeKind], t)
	assertSubtype(context.Background(), vs.Format(), PrimitiveTypeMap[ValueKind], t)
}

func TestAssertTypeStruct(tt *testing.T) {
	vs := newTestValueStore()
	t, err := MakeStructType("Struct", StructField{"x", PrimitiveTypeMap[BoolKind], false})
	require.NoError(tt, err)

	v, err := NewStruct(vs.Format(), "Struct", StructData{"x": Bool(true)})
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), t, v)
	assertAll(tt, vs.Format(), t, v)
	assertSubtype(context.Background(), vs.Format(), PrimitiveTypeMap[ValueKind], v)
}

func TestAssertTypeUnion(tt *testing.T) {
	vs := newTestValueStore()

	assertSubtype(context.Background(), vs.Format(), mustType(MakeUnionType(PrimitiveTypeMap[FloatKind])), Float(42))
	assertSubtype(context.Background(), vs.Format(), mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])), Float(42))
	assertSubtype(context.Background(), vs.Format(), mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])), String("hi"))
	assertSubtype(context.Background(), vs.Format(), mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BoolKind])), Float(555))
	assertSubtype(context.Background(), vs.Format(), mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BoolKind])), String("hi"))
	assertSubtype(context.Background(), vs.Format(), mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BoolKind])), Bool(true))

	lt, err := MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])))
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), lt, mustList(NewList(context.Background(), vs, Float(1), String("hi"), Float(2), String("bye"))))

	st, err := MakeSetType(PrimitiveTypeMap[StringKind])
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), mustType(MakeUnionType(st, PrimitiveTypeMap[FloatKind])), Float(42))
	assertSubtype(context.Background(), vs.Format(), mustType(MakeUnionType(st, PrimitiveTypeMap[FloatKind])), mustValue(NewSet(context.Background(), vs, String("a"), String("b"))))

	assertInvalid(tt, vs.Format(), mustType(MakeUnionType()), Float(42))
	assertInvalid(tt, vs.Format(), mustType(MakeUnionType(PrimitiveTypeMap[StringKind])), Float(42))
	assertInvalid(tt, vs.Format(), mustType(MakeUnionType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BoolKind])), Float(42))
	assertInvalid(tt, vs.Format(), mustType(MakeUnionType(st, PrimitiveTypeMap[StringKind])), Float(42))
	assertInvalid(tt, vs.Format(), mustType(MakeUnionType(st, PrimitiveTypeMap[FloatKind])), mustValue(NewSet(context.Background(), vs, Float(1), Float(2))))
}

func TestAssertConcreteTypeIsUnion(tt *testing.T) {
	vs := newTestValueStore()
	assert.True(tt, IsSubtype(
		vs.Format(),
		mustType(MakeStructTypeFromFields("", FieldMap{})),
		mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[StringKind]})),
			mustType(MakeStructTypeFromFields("", FieldMap{"bar": PrimitiveTypeMap[StringKind]}))))))

	assert.False(tt, IsSubtype(
		vs.Format(),
		mustType(MakeStructTypeFromFields("", FieldMap{})),
		mustType(MakeUnionType(mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[StringKind]})), PrimitiveTypeMap[FloatKind]))))

	assert.True(tt, IsSubtype(
		vs.Format(),
		mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[StringKind]})),
			mustType(MakeStructTypeFromFields("", FieldMap{"bar": PrimitiveTypeMap[StringKind]})))),
		mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[StringKind], "bar": PrimitiveTypeMap[StringKind]})),
			mustType(MakeStructTypeFromFields("", FieldMap{"bar": PrimitiveTypeMap[StringKind]})))),
	))

	assert.False(tt, IsSubtype(
		vs.Format(),
		mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[StringKind]})),
			mustType(MakeStructTypeFromFields("", FieldMap{"bar": PrimitiveTypeMap[StringKind]})))),
		mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[StringKind], "bar": PrimitiveTypeMap[StringKind]})),
			PrimitiveTypeMap[FloatKind]))))
}

func TestAssertTypeEmptyListUnion(tt *testing.T) {
	vs := newTestValueStore()

	ut, err := MakeUnionType()
	require.NoError(tt, err)
	lt, err := MakeListType(ut)
	require.NoError(tt, err)
	l, err := NewList(context.Background(), vs)
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), lt, l)
}

func TestAssertTypeEmptyList(tt *testing.T) {
	vs := newTestValueStore()

	lt, err := MakeListType(PrimitiveTypeMap[FloatKind])
	require.NoError(tt, err)
	l, err := NewList(context.Background(), vs)
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), lt, l)

	// List<> not a subtype of List<Float>
	assertInvalid(tt, vs.Format(), mustType(MakeListType(mustType(MakeUnionType()))), mustList(NewList(context.Background(), vs, Float(1))))
}

func TestAssertTypeEmptySet(tt *testing.T) {
	vs := newTestValueStore()

	st, err := MakeSetType(PrimitiveTypeMap[FloatKind])
	require.NoError(tt, err)
	s, err := NewSet(context.Background(), vs)
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), st, s)

	// Set<> not a subtype of Set<Float>
	assertInvalid(tt, vs.Format(), mustType(MakeSetType(mustType(MakeUnionType()))), mustValue(NewSet(context.Background(), vs, Float(1))))
}

func TestAssertTypeEmptyMap(tt *testing.T) {
	vs := newTestValueStore()

	mt, err := MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])
	require.NoError(tt, err)
	m, err := NewMap(context.Background(), vs)
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), mt, m)

	// Map<> not a subtype of Map<Float, Float>
	m2, err := NewMap(context.Background(), vs, Float(1), Float(2))
	require.NoError(tt, err)
	assertInvalid(tt, vs.Format(), mustType(MakeMapType(mustType(MakeUnionType()), mustType(MakeUnionType()))), m2)
}

func TestAssertTypeStructSubtypeByName(tt *testing.T) {
	vs := newTestValueStore()
	namedT, err := MakeStructType("Name", StructField{"x", PrimitiveTypeMap[FloatKind], false})
	require.NoError(tt, err)
	anonT, err := MakeStructType("", StructField{"x", PrimitiveTypeMap[FloatKind], false})
	require.NoError(tt, err)
	namedV, err := NewStruct(vs.Format(), "Name", StructData{"x": Float(42)})
	require.NoError(tt, err)
	name2V, err := NewStruct(vs.Format(), "foo", StructData{"x": Float(42)})
	require.NoError(tt, err)
	anonV, err := NewStruct(vs.Format(), "", StructData{"x": Float(42)})
	require.NoError(tt, err)

	assertSubtype(context.Background(), vs.Format(), namedT, namedV)
	assertInvalid(tt, vs.Format(), namedT, name2V)
	assertInvalid(tt, vs.Format(), namedT, anonV)

	assertSubtype(context.Background(), vs.Format(), anonT, namedV)
	assertSubtype(context.Background(), vs.Format(), anonT, name2V)
	assertSubtype(context.Background(), vs.Format(), anonT, anonV)
}

func TestAssertTypeStructSubtypeExtraFields(tt *testing.T) {
	vs := newTestValueStore()
	at, err := MakeStructType("")
	require.NoError(tt, err)
	bt, err := MakeStructType("", StructField{"x", PrimitiveTypeMap[FloatKind], false})
	require.NoError(tt, err)
	ct, err := MakeStructType("", StructField{"s", PrimitiveTypeMap[StringKind], false}, StructField{"x", PrimitiveTypeMap[FloatKind], false})
	require.NoError(tt, err)
	av, err := NewStruct(vs.Format(), "", StructData{})
	require.NoError(tt, err)
	bv, err := NewStruct(vs.Format(), "", StructData{"x": Float(1)})
	require.NoError(tt, err)
	cv, err := NewStruct(vs.Format(), "", StructData{"x": Float(2), "s": String("hi")})
	require.NoError(tt, err)

	assertSubtype(context.Background(), vs.Format(), at, av)
	assertInvalid(tt, vs.Format(), bt, av)
	assertInvalid(tt, vs.Format(), ct, av)

	assertSubtype(context.Background(), vs.Format(), at, bv)
	assertSubtype(context.Background(), vs.Format(), bt, bv)
	assertInvalid(tt, vs.Format(), ct, bv)

	assertSubtype(context.Background(), vs.Format(), at, cv)
	assertSubtype(context.Background(), vs.Format(), bt, cv)
	assertSubtype(context.Background(), vs.Format(), ct, cv)
}

func TestAssertTypeStructSubtype(tt *testing.T) {
	vs := newTestValueStore()

	c1, err := NewStruct(vs.Format(), "Commit", StructData{
		"value":   Float(1),
		"parents": mustValue(NewSet(context.Background(), vs)),
	})
	require.NoError(tt, err)
	t1, err := MakeStructType("Commit",
		StructField{"parents", mustType(MakeSetType(mustType(MakeUnionType()))), false},
		StructField{"value", PrimitiveTypeMap[FloatKind], false},
	)
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), t1, c1)

	t11, err := MakeStructType("Commit",
		StructField{"parents", mustType(MakeSetType(mustType(MakeRefType(MakeCycleType("Commit"))))), false},
		StructField{"value", PrimitiveTypeMap[FloatKind], false},
	)
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), t11, c1)

	c2, err := NewStruct(vs.Format(), "Commit", StructData{
		"value":   Float(2),
		"parents": mustValue(NewSet(context.Background(), vs, mustRef(NewRef(c1, vs.Format())))),
	})
	require.NoError(tt, err)
	assertSubtype(context.Background(), vs.Format(), t11, c2)
}

func TestAssertTypeCycleUnion(tt *testing.T) {
	vs := newTestValueStore()
	// struct S {
	//   x: Cycle<S>,
	//   y: Float,
	// }
	t1, err := MakeStructType("S",
		StructField{"x", MakeCycleType("S"), false},
		StructField{"y", PrimitiveTypeMap[FloatKind], false},
	)
	require.NoError(tt, err)
	// struct S {
	//   x: Cycle<S>,
	//   y: Float | String,
	// }
	t2, err := MakeStructType("S",
		StructField{"x", MakeCycleType("S"), false},
		StructField{"y", mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])), false},
	)

	require.NoError(tt, err)
	assert.True(tt, IsSubtype(vs.Format(), t2, t1))
	assert.False(tt, IsSubtype(vs.Format(), t1, t2))

	// struct S {
	//   x: Cycle<S> | Float,
	//   y: Float | String,
	// }
	t3, err := MakeStructType("S",
		StructField{"x", mustType(MakeUnionType(MakeCycleType("S"), PrimitiveTypeMap[FloatKind])), false},
		StructField{"y", mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])), false},
	)

	require.NoError(tt, err)
	assert.True(tt, IsSubtype(vs.Format(), t3, t1))
	assert.False(tt, IsSubtype(vs.Format(), t1, t3))

	assert.True(tt, IsSubtype(vs.Format(), t3, t2))
	assert.False(tt, IsSubtype(vs.Format(), t2, t3))

	// struct S {
	//   x: Cycle<S> | Float,
	//   y: Float,
	// }
	t4, err := MakeStructType("S",
		StructField{"x", mustType(MakeUnionType(MakeCycleType("S"), PrimitiveTypeMap[FloatKind])), false},
		StructField{"y", PrimitiveTypeMap[FloatKind], false},
	)

	require.NoError(tt, err)
	assert.True(tt, IsSubtype(vs.Format(), t4, t1))
	assert.False(tt, IsSubtype(vs.Format(), t1, t4))

	assert.False(tt, IsSubtype(vs.Format(), t4, t2))
	assert.False(tt, IsSubtype(vs.Format(), t2, t4))

	assert.True(tt, IsSubtype(vs.Format(), t3, t4))
	assert.False(tt, IsSubtype(vs.Format(), t4, t3))

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
	require.NoError(tt, err)

	tc, err := MakeStructType("A",
		StructField{
			"c",
			mustType(MakeStructType("B", StructField{"b", MakeCycleType("A"), false})),
			false,
		},
	)
	require.NoError(tt, err)

	assert.False(tt, IsSubtype(vs.Format(), tb, tc))
	assert.False(tt, IsSubtype(vs.Format(), tc, tb))
}

func TestIsSubtypeEmptySruct(tt *testing.T) {
	vs := newTestValueStore()
	// struct {
	//   a: Float,
	//   b: struct {},
	// }
	t1, err := MakeStructType("X",
		StructField{"a", PrimitiveTypeMap[FloatKind], false},
		StructField{"b", EmptyStructType, false},
	)
	require.NoError(tt, err)

	// struct {
	//   a: Float,
	// }
	t2, err := MakeStructType("X", StructField{"a", PrimitiveTypeMap[FloatKind], false})
	require.NoError(tt, err)

	assert.False(tt, IsSubtype(vs.Format(), t1, t2))
	assert.True(tt, IsSubtype(vs.Format(), t2, t1))
}

func TestIsSubtypeCompoundUnion(tt *testing.T) {
	vs := newTestValueStore()
	rt, err := MakeListType(EmptyStructType)
	require.NoError(tt, err)

	st1, err := MakeStructType("One", StructField{"a", PrimitiveTypeMap[FloatKind], false})
	require.NoError(tt, err)
	st2, err := MakeStructType("Two", StructField{"b", PrimitiveTypeMap[StringKind], false})
	require.NoError(tt, err)
	ct, err := MakeListType(mustType(MakeUnionType(st1, st2)))
	require.NoError(tt, err)

	assert.True(tt, IsSubtype(vs.Format(), rt, ct))
	assert.False(tt, IsSubtype(vs.Format(), ct, rt))

	ct2, err := MakeListType(mustType(MakeUnionType(st1, st2, PrimitiveTypeMap[FloatKind])))
	require.NoError(tt, err)
	assert.False(tt, IsSubtype(vs.Format(), rt, ct2))
	assert.False(tt, IsSubtype(vs.Format(), ct2, rt))
}

func TestIsSubtypeOptionalFields(tt *testing.T) {
	vs := newTestValueStore()
	assert := assert.New(tt)

	s1, err := MakeStructType("", StructField{"a", PrimitiveTypeMap[FloatKind], true})
	require.NoError(tt, err)
	s2, err := MakeStructType("", StructField{"a", PrimitiveTypeMap[FloatKind], false})
	require.NoError(tt, err)
	assert.True(IsSubtype(vs.Format(), s1, s2))
	assert.False(IsSubtype(vs.Format(), s2, s1))

	s3, err := MakeStructType("", StructField{"a", PrimitiveTypeMap[StringKind], false})
	require.NoError(tt, err)
	assert.False(IsSubtype(vs.Format(), s1, s3))
	assert.False(IsSubtype(vs.Format(), s3, s1))

	s4, err := MakeStructType("", StructField{"a", PrimitiveTypeMap[StringKind], true})
	require.NoError(tt, err)
	assert.False(IsSubtype(vs.Format(), s1, s4))
	assert.False(IsSubtype(vs.Format(), s4, s1))

	test := func(t1s, t2s string, exp1, exp2 bool) {
		t1, err := makeTestStructTypeFromFieldNames(t1s)
		require.NoError(tt, err)
		t2, err := makeTestStructTypeFromFieldNames(t2s)
		require.NoError(tt, err)
		assert.Equal(exp1, IsSubtype(vs.Format(), t1, t2))
		assert.Equal(exp2, IsSubtype(vs.Format(), t2, t1))
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

	t1, err := MakeStructType("", StructField{"a", PrimitiveTypeMap[BoolKind], true})
	require.NoError(tt, err)
	t2, err := MakeStructType("", StructField{"a", PrimitiveTypeMap[FloatKind], true})
	require.NoError(tt, err)
	assert.False(IsSubtype(vs.Format(), t1, t2))
	assert.False(IsSubtype(vs.Format(), t2, t1))
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
		fields[i] = StructField{f, PrimitiveTypeMap[BoolKind], optional}
	}
	return MakeStructType("", fields...)
}

func makeTestStructFromFieldNames(nbf *NomsBinFormat, s string) (Struct, error) {
	t, err := makeTestStructTypeFromFieldNames(s)

	if err != nil {
		return Struct{}, err
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

	return newStruct(nbf, "", fieldNames, vals)
}

func TestIsSubtypeDisallowExtraStructFields(tt *testing.T) {
	vs := newTestValueStore()
	assert := assert.New(tt)

	test := func(t1s, t2s string, exp1, exp2 bool) {
		t1, err := makeTestStructTypeFromFieldNames(t1s)
		require.NoError(tt, err)
		t2, err := makeTestStructTypeFromFieldNames(t2s)
		require.NoError(tt, err)
		assert.Equal(exp1, IsSubtypeDisallowExtraStructFields(vs.Format(), t1, t2))
		assert.Equal(exp2, IsSubtypeDisallowExtraStructFields(vs.Format(), t2, t1))
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
		assert.True(IsValueSubtypeOf(vs.Format(), v, t))
	}

	assertFalse := func(v Value, t *Type) {
		assert.False(IsValueSubtypeOf(vs.Format(), v, t))
	}

	allTypes := []struct {
		v Value
		t *Type
	}{
		{Bool(true), PrimitiveTypeMap[BoolKind]},
		{Float(42), PrimitiveTypeMap[FloatKind]},
		{String("s"), PrimitiveTypeMap[StringKind]},
		{mustBlob(NewEmptyBlob(vs)), PrimitiveTypeMap[BlobKind]},
		{PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[TypeKind]},
		{mustList(NewList(context.Background(), vs, Float(42))), mustType(MakeListType(PrimitiveTypeMap[FloatKind]))},
		{mustValue(NewSet(context.Background(), vs, Float(42))), mustType(MakeSetType(PrimitiveTypeMap[FloatKind]))},
		{mustRef(NewRef(Float(42), vs.Format())), mustType(MakeRefType(PrimitiveTypeMap[FloatKind]))},
		{mustValue(NewMap(context.Background(), vs, Float(42), String("a"))), mustType(MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind]))},
		{mustValue(NewStruct(vs.Format(), "A", StructData{})), mustType(MakeStructType("A"))},
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
		assertTrue(rec.v, PrimitiveTypeMap[ValueKind])
	}

	assertTrue(Bool(true), mustType((MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind]))))
	assertTrue(Float(123), mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])))
	assertFalse(String("abc"), mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])))
	assertFalse(String("abc"), mustType(MakeUnionType()))

	assertTrue(mustList(NewList(context.Background(), vs)), mustType(MakeListType(PrimitiveTypeMap[FloatKind])))
	assertTrue(mustList(NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))), mustType(MakeListType(PrimitiveTypeMap[FloatKind])))
	assertFalse(mustList(NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))), mustType(MakeListType(PrimitiveTypeMap[BoolKind])))
	assertTrue(mustList(NewList(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))), mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))))
	assertTrue(mustList(NewList(context.Background(), vs, Float(0), Bool(true))), mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))))
	assertFalse(mustList(NewList(context.Background(), vs, Float(0))), mustType(MakeListType(mustType(MakeUnionType()))))
	assertTrue(mustList(NewList(context.Background(), vs)), mustType(MakeListType(mustType(MakeUnionType()))))

	{
		newChunkedList := func(vals ...Value) List {
			newSequenceMetaTuple := func(v Value) metaTuple {
				seq, err := newListLeafSequence(vs, v)
				require.NoError(tt, err)
				list := newList(seq)
				ref, err := vs.WriteValue(context.Background(), list)
				require.NoError(tt, err)
				ordKey, err := newOrderedKey(v, vs.Format())
				require.NoError(tt, err)
				mt, err := newMetaTuple(ref, ordKey, 1)
				require.NoError(tt, err)
				return mt
			}

			tuples := make([]metaTuple, len(vals))
			for i, v := range vals {
				tuples[i] = newSequenceMetaTuple(v)
			}
			mseq, err := newListMetaSequence(1, tuples, vs)
			require.NoError(tt, err)
			return newList(mseq)
		}

		assertTrue(newChunkedList(Float(0), Float(1), Float(2), Float(3)), mustType(MakeListType(PrimitiveTypeMap[FloatKind])))
		assertFalse(newChunkedList(Float(0), Float(1), Float(2), Float(3)), mustType(MakeListType(PrimitiveTypeMap[BoolKind])))
		assertTrue(newChunkedList(Float(0), Float(1), Float(2), Float(3)), mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))))
		assertTrue(newChunkedList(Float(0), Bool(true)), mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))))
		assertFalse(newChunkedList(Float(0)), mustType(MakeListType(mustType(MakeUnionType()))))
	}

	assertTrue(mustValue(NewSet(context.Background(), vs)), mustType(MakeSetType(PrimitiveTypeMap[FloatKind])))
	assertTrue(mustValue(NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))), mustType(MakeSetType(PrimitiveTypeMap[FloatKind])))
	assertFalse(mustValue(NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))), mustType(MakeSetType(PrimitiveTypeMap[BoolKind])))
	assertTrue(mustValue(NewSet(context.Background(), vs, Float(0), Float(1), Float(2), Float(3))), mustType(MakeSetType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))))
	assertTrue(mustValue(NewSet(context.Background(), vs, Float(0), Bool(true))), mustType(MakeSetType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))))
	assertFalse(mustValue(NewSet(context.Background(), vs, Float(0))), mustType(MakeSetType(mustType(MakeUnionType()))))
	assertTrue(mustValue(NewSet(context.Background(), vs)), mustType(MakeSetType(mustType(MakeUnionType()))))

	{
		newChunkedSet := func(vals ...Value) Set {
			newSequenceMetaTuple := func(v Value) metaTuple {
				seq, err := newSetLeafSequence(vs, v)
				require.NoError(tt, err)
				set := newSet(seq)
				ref, err := vs.WriteValue(context.Background(), set)
				require.NoError(tt, err)
				ordKey, err := newOrderedKey(v, vs.Format())
				require.NoError(tt, err)
				mt, err := newMetaTuple(ref, ordKey, 1)
				require.NoError(tt, err)
				return mt
			}

			tuples := make([]metaTuple, len(vals))
			for i, v := range vals {
				tuples[i] = newSequenceMetaTuple(v)
			}
			return newSet(mustOrdSeq(newSetMetaSequence(1, tuples, vs)))
		}
		assertTrue(newChunkedSet(Float(0), Float(1), Float(2), Float(3)), mustType(MakeSetType(PrimitiveTypeMap[FloatKind])))
		assertFalse(newChunkedSet(Float(0), Float(1), Float(2), Float(3)), mustType(MakeSetType(PrimitiveTypeMap[BoolKind])))
		assertTrue(newChunkedSet(Float(0), Float(1), Float(2), Float(3)), mustType(MakeSetType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))))
		assertTrue(newChunkedSet(Float(0), Bool(true)), mustType(MakeSetType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))))
		assertFalse(newChunkedSet(Float(0)), mustType(MakeSetType(mustType(MakeUnionType()))))
	}

	assertTrue(mustMap(NewMap(context.Background(), vs)), mustType(MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])))
	assertTrue(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b"))), mustType(MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])))
	assertFalse(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b"))), mustType(MakeMapType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind])))
	assertFalse(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b"))), mustType(MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))
	assertTrue(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b"))), mustType(MakeMapType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])), PrimitiveTypeMap[StringKind])))
	assertTrue(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), String("b"))), mustType(MakeMapType(PrimitiveTypeMap[FloatKind], mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind])))))
	assertTrue(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Bool(true), String("b"))), mustType(MakeMapType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])), PrimitiveTypeMap[StringKind])))
	assertTrue(mustMap(NewMap(context.Background(), vs, Float(0), String("a"), Float(1), Bool(true))), mustType(MakeMapType(PrimitiveTypeMap[FloatKind], mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind])))))
	assertFalse(mustMap(NewMap(context.Background(), vs, Float(0), String("a"))), mustType(MakeMapType(mustType(MakeUnionType()), PrimitiveTypeMap[StringKind])))
	assertFalse(mustMap(NewMap(context.Background(), vs, Float(0), String("a"))), mustType(MakeMapType(PrimitiveTypeMap[FloatKind], mustType(MakeUnionType()))))
	assertTrue(mustMap(NewMap(context.Background(), vs)), mustType(MakeMapType(mustType(MakeUnionType()), mustType(MakeUnionType()))))

	{
		newChunkedMap := func(vals ...Value) Map {
			newSequenceMetaTuple := func(e mapEntry) metaTuple {
				seq, err := newMapLeafSequence(vs, e)
				require.NoError(tt, err)
				m := newMap(seq)
				ref, err := vs.WriteValue(context.Background(), m)
				require.NoError(tt, err)
				ordKey, err := newOrderedKey(e.key, vs.Format())
				require.NoError(tt, err)
				mt, err := newMetaTuple(ref, ordKey, 1)
				require.NoError(tt, err)
				return mt
			}

			tuples := make([]metaTuple, len(vals)/2)
			for i := 0; i < len(vals); i += 2 {
				tuples[i/2] = newSequenceMetaTuple(mapEntry{vals[i], vals[i+1]})
			}
			return newMap(mustOrdSeq(newMapMetaSequence(1, tuples, vs)))
		}

		assertTrue(newChunkedMap(Float(0), String("a"), Float(1), String("b")), mustType(MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])))
		assertFalse(newChunkedMap(Float(0), String("a"), Float(1), String("b")), mustType(MakeMapType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind])))
		assertFalse(newChunkedMap(Float(0), String("a"), Float(1), String("b")), mustType(MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))
		assertTrue(newChunkedMap(Float(0), String("a"), Float(1), String("b")), mustType(MakeMapType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])), PrimitiveTypeMap[StringKind])))
		assertTrue(newChunkedMap(Float(0), String("a"), Float(1), String("b")), mustType(MakeMapType(PrimitiveTypeMap[FloatKind], mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind])))))
		assertTrue(newChunkedMap(Float(0), String("a"), Bool(true), String("b")), mustType(MakeMapType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])), PrimitiveTypeMap[StringKind])))
		assertTrue(newChunkedMap(Float(0), String("a"), Float(1), Bool(true)), mustType(MakeMapType(PrimitiveTypeMap[FloatKind], mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind])))))
		assertFalse(newChunkedMap(Float(0), String("a")), mustType(MakeMapType(mustType(MakeUnionType()), PrimitiveTypeMap[StringKind])))
		assertFalse(newChunkedMap(Float(0), String("a")), mustType(MakeMapType(PrimitiveTypeMap[FloatKind], mustType(MakeUnionType()))))
	}

	assertTrue(mustRef(NewRef(Float(1), vs.Format())), mustType(MakeRefType(PrimitiveTypeMap[FloatKind])))
	assertFalse(mustRef(NewRef(Float(1), vs.Format())), mustType(MakeRefType(PrimitiveTypeMap[BoolKind])))
	assertTrue(mustRef(NewRef(Float(1), vs.Format())), mustType(MakeRefType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))))
	assertFalse(mustRef(NewRef(Float(1), vs.Format())), mustType(MakeRefType(mustType(MakeUnionType()))))

	assertTrue(
		mustValue(NewStruct(vs.Format(), "Struct", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct", StructField{"x", PrimitiveTypeMap[BoolKind], false})),
	)
	assertTrue(
		mustValue(NewStruct(vs.Format(), "Struct", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct", StructField{"x", PrimitiveTypeMap[BoolKind], true})),
	)
	assertTrue(
		mustValue(NewStruct(vs.Format(), "Struct", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct")),
	)
	assertTrue(
		mustValue(NewStruct(vs.Format(), "Struct", StructData{})),
		mustType(MakeStructType("Struct")),
	)
	assertFalse(
		mustValue(NewStruct(vs.Format(), "", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct")),
	)
	assertFalse(
		mustValue(NewStruct(vs.Format(), "struct", StructData{"x": Bool(true)})), // lower case name
		mustType(MakeStructType("Struct")),
	)
	assertTrue(
		mustValue(NewStruct(vs.Format(), "Struct", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct", StructField{"x", mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])), true})),
	)
	assertTrue(
		mustValue(NewStruct(vs.Format(), "Struct", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct", StructField{"y", PrimitiveTypeMap[BoolKind], true})),
	)
	assertFalse(
		mustValue(NewStruct(vs.Format(), "Struct", StructData{"x": Bool(true)})),
		mustType(MakeStructType("Struct", StructField{"x", PrimitiveTypeMap[StringKind], true})),
	)

	assertTrue(
		mustValue(NewStruct(vs.Format(), "Node", StructData{
			"value": Float(1),
			"children": mustList(NewList(context.Background(), vs,
				mustValue(NewStruct(vs.Format(), "Node", StructData{
					"value":    Float(2),
					"children": mustList(NewList(context.Background(), vs)),
				})),
			)),
		})),
		mustType(MakeStructType("Node",
			StructField{"value", PrimitiveTypeMap[FloatKind], false},
			StructField{"children", mustType(MakeListType(MakeCycleType("Node"))), false},
		)),
	)

	assertFalse( // inner Node has wrong type.
		mustValue(NewStruct(vs.Format(), "Node", StructData{
			"value": Float(1),
			"children": mustList(NewList(context.Background(), vs,
				mustValue(NewStruct(vs.Format(), "Node", StructData{
					"value":    Bool(true),
					"children": mustList(NewList(context.Background(), vs)),
				}))),
			),
		})),
		mustType(MakeStructType("Node",
			StructField{"value", PrimitiveTypeMap[FloatKind], false},
			StructField{"children", mustType(MakeListType(MakeCycleType("Node"))), false},
		)),
	)

	{
		node := func(value Value, children ...Value) Value {
			childrenAsRefs := make(ValueSlice, len(children))
			for i, c := range children {
				var err error
				childrenAsRefs[i], err = NewRef(c, vs.Format())
				require.NoError(tt, err)
			}
			rv, err := NewStruct(vs.Format(), "Node", StructData{
				"value":    value,
				"children": mustList(NewList(context.Background(), vs, childrenAsRefs...)),
			})
			require.NoError(tt, err)
			return rv
		}

		requiredType, err := MakeStructType("Node",
			StructField{"value", PrimitiveTypeMap[FloatKind], false},
			StructField{"children", mustType(MakeListType(mustType(MakeRefType(MakeCycleType("Node"))))), false},
		)
		require.NoError(tt, err)

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
			StructField{"a", PrimitiveTypeMap[FloatKind], false},
			StructField{"b", MakeCycleType("A"), false},
		)
		require.NoError(tt, err)
		t2, err := MakeStructType("A",
			StructField{"a", PrimitiveTypeMap[FloatKind], false},
			StructField{"b", MakeCycleType("A"), true},
		)
		require.NoError(tt, err)
		v, err := NewStruct(vs.Format(), "A", StructData{
			"a": Float(1),
			"b": mustValue(NewStruct(vs.Format(), "A", StructData{
				"a": Float(2),
			})),
		})
		require.NoError(tt, err)

		assertFalse(v, t1)
		assertTrue(v, t2)
	}

	{
		t, err := MakeStructType("A",
			StructField{"aa", PrimitiveTypeMap[FloatKind], true},
			StructField{"bb", PrimitiveTypeMap[BoolKind], false},
		)
		require.NoError(tt, err)
		v, err := NewStruct(vs.Format(), "A", StructData{
			"a": Float(1),
			"b": Bool(true),
		})
		require.NoError(tt, err)
		assertFalse(v, t)
	}
}

func TestIsValueSubtypeOfDetails(tt *testing.T) {
	vs := newTestValueStore()
	a := assert.New(tt)

	test := func(vString, tString string, exp1, exp2 bool) {
		v, err := makeTestStructFromFieldNames(vs.Format(), vString)
		require.NoError(tt, err)
		t, err := makeTestStructTypeFromFieldNames(tString)
		require.NoError(tt, err)
		isSub, hasExtra, err := IsValueSubtypeOfDetails(vs.Format(), v, t)
		require.NoError(tt, err)
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
