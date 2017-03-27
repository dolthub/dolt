// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestTypeCacheList(t *testing.T) {
	assert := assert.New(t)

	lbt := MakeListType(BoolType)
	lbt2 := MakeListType(BoolType)
	assert.True(lbt == lbt2)

	lst := MakeListType(StringType)
	lnt := MakeListType(NumberType)
	assert.False(lst == lnt)

	lst2 := MakeListType(StringType)
	assert.True(lst == lst2)

	lnt2 := MakeListType(NumberType)
	assert.True(lnt == lnt2)

	lbt3 := MakeListType(BoolType)
	assert.True(lbt == lbt3)
}

func TestTypeCacheSet(t *testing.T) {
	assert := assert.New(t)

	lbt := MakeSetType(BoolType)
	lbt2 := MakeSetType(BoolType)
	assert.True(lbt == lbt2)

	lst := MakeSetType(StringType)
	lnt := MakeSetType(NumberType)
	assert.False(lst == lnt)

	lst2 := MakeSetType(StringType)
	assert.True(lst == lst2)

	lnt2 := MakeSetType(NumberType)
	assert.True(lnt == lnt2)

	lbt3 := MakeSetType(BoolType)
	assert.True(lbt == lbt3)
}

func TestTypeCacheRef(t *testing.T) {
	assert := assert.New(t)

	lbt := MakeRefType(BoolType)
	lbt2 := MakeRefType(BoolType)
	assert.True(lbt == lbt2)

	lst := MakeRefType(StringType)
	lnt := MakeRefType(NumberType)
	assert.False(lst == lnt)

	lst2 := MakeRefType(StringType)
	assert.True(lst == lst2)

	lnt2 := MakeRefType(NumberType)
	assert.True(lnt == lnt2)

	lbt3 := MakeRefType(BoolType)
	assert.True(lbt == lbt3)
}

func TestTypeCacheStruct(t *testing.T) {
	assert := assert.New(t)

	st := MakeStructType2("Foo",
		StructField{"bar", StringType, false},
		StructField{"foo", NumberType, false},
	)
	st2 := MakeStructType2("Foo",
		StructField{"bar", StringType, false},
		StructField{"foo", NumberType, false},
	)

	assert.True(st == st2)
}

func TestTypeCacheUnion(t *testing.T) {
	assert := assert.New(t)
	ut := MakeUnionType(NumberType)
	ut2 := MakeUnionType(NumberType)
	assert.True(ut == ut2)

	ut = MakeUnionType(NumberType, StringType)
	ut2 = MakeUnionType(StringType, NumberType)
	assert.True(ut == ut2)

	ut = MakeUnionType(StringType, BoolType, NumberType)
	ut2 = MakeUnionType(NumberType, StringType, BoolType)
	assert.True(ut == ut2)
}

func TestTypeCacheCyclicStruct(t *testing.T) {
	assert := assert.New(t)

	st := MakeStructType2("Foo", StructField{"foo", MakeRefType(MakeCycleType(0)), false})
	assert.True(st == st.Desc.(StructDesc).fields[0].Type.Desc.(CompoundDesc).ElemTypes[0])
	assert.False(st.HasUnresolvedCycle())

	st2 := MakeStructType2("Foo", StructField{"foo", MakeRefType(MakeCycleType(0)), false})
	assert.True(st2 == st2.Desc.(StructDesc).fields[0].Type.Desc.(CompoundDesc).ElemTypes[0])
	assert.True(st == st2)
}

func TestTypeCacheCyclicStruct2(t *testing.T) {
	assert := assert.New(t)

	// Foo {
	//   bar: Cycle<1>
	//   foo: Cycle<0>
	// }
	st := MakeStructType2("Foo",
		StructField{"bar", MakeCycleType(1), false},
		StructField{"foo", MakeCycleType(0), false},
	)
	assert.True(st.HasUnresolvedCycle())
	// foo ref is cyclic
	assert.True(st == st.Desc.(StructDesc).fields[1].Type)

	// Bar {
	//   baz: Cycle<1>
	//   foo: Foo {
	//     bar: Cycle<1>
	//     foo: Cycle<0>
	//   }
	// }
	st2 := MakeStructType2("Bar",
		StructField{"baz", MakeCycleType(1), false},
		StructField{"foo", st, false},
	)
	assert.True(st2.HasUnresolvedCycle())
	// foo ref is cyclic
	assert.True(st2.Desc.(StructDesc).fields[1].Type == st2.Desc.(StructDesc).fields[1].Type.Desc.(StructDesc).fields[1].Type)
	// bar ref is cyclic
	assert.True(st2 == st2.Desc.(StructDesc).fields[1].Type.Desc.(StructDesc).fields[0].Type)

	// Baz {
	//   bar: Bar {
	//     baz: Cycle<1>
	//     foo: Foo {
	//       bar: Cycle<1>
	//       foo: Cycle<0>
	//     }
	//   }
	//   baz: Cycle<0>
	// }
	st3 := MakeStructType2("Baz",
		StructField{"bar", st2, false},
		StructField{"baz", MakeCycleType(0), false},
	)
	assert.False(st3.HasUnresolvedCycle())

	// foo ref is cyclic
	assert.True(st3.Desc.(StructDesc).fields[0].Type.Desc.(StructDesc).fields[1].Type == st3.Desc.(StructDesc).fields[0].Type.Desc.(StructDesc).fields[1].Type.Desc.(StructDesc).fields[1].Type)
	// bar ref is cyclic
	assert.True(st3.Desc.(StructDesc).fields[0].Type == st3.Desc.(StructDesc).fields[0].Type.Desc.(StructDesc).fields[0].Type.Desc.(StructDesc).fields[0].Type)
	// baz second-level ref is cyclic
	assert.True(st3 == st3.Desc.(StructDesc).fields[0].Type.Desc.(StructDesc).fields[0].Type)
	// baz top-level ref is cyclic
	assert.True(st3 == st3.Desc.(StructDesc).fields[1].Type)
}

func TestTypeCacheCyclicUnions(t *testing.T) {
	assert := assert.New(t)

	ut := MakeUnionType(MakeCycleType(0), NumberType, StringType, BoolType, BlobType, ValueType, TypeType)
	st := MakeStructType2("Foo", StructField{"foo", ut, false})

	assert.True(ut.Desc.(CompoundDesc).ElemTypes[6].Kind() == CycleKind)
	// That the Struct / Cycle landed in index 5 was found empirically.
	assert.Equal(st, st.Desc.(StructDesc).fields[0].Type.Desc.(CompoundDesc).ElemTypes[5])
	// ut contains an explicit Cycle type; noms must not surrepticiously change existing types so we can be sure that the Union within st is different in that the cycle has been resolved.
	assert.False(ut == st.Desc.(StructDesc).fields[0].Type)

	// Note that the union in this second case is created with a different ordering of its type arguments.
	ut2 := MakeUnionType(NumberType, StringType, BoolType, BlobType, ValueType, TypeType, MakeCycleType(0))
	st2 := MakeStructType2("Foo", StructField{"foo", ut2, false})
	assert.True(ut2.Desc.(CompoundDesc).ElemTypes[6].Kind() == CycleKind)
	assert.True(st2 == st2.Desc.(StructDesc).fields[0].Type.Desc.(CompoundDesc).ElemTypes[5])
	assert.False(ut2 == st2.Desc.(StructDesc).fields[0].Type)

	assert.True(ut == ut2)
	assert.True(st == st2)
}

func TestNonNormalizedCycles(t *testing.T) {
	assert := assert.New(t)

	t1 := MakeStructType2("A",
		StructField{
			"a",
			MakeStructType2("A", StructField{"a", MakeCycleType(1), false}),
			false,
		},
	)
	t2 := t1.Desc.(StructDesc).fields[0].Type
	assert.True(t1.Equals(t2))
}

func TestMakeStructTypeFromFields(t *testing.T) {
	assert := assert.New(t)
	fields := map[string]*Type{
		"str":    StringType,
		"number": NumberType,
		"bool":   BoolType,
	}
	desc := MakeStructTypeFromFields("Thing", fields).Desc.(StructDesc)
	assert.Equal("Thing", desc.Name)
	assert.Equal(3, desc.Len())
	for k, v := range fields {
		f, optional := desc.Field(k)
		assert.False(optional)
		assert.True(v == f)
	}
}
