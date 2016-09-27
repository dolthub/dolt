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
	assert.NotNil(lbt.serialization)

	lst := MakeListType(StringType)
	lnt := MakeListType(NumberType)
	assert.False(lst == lnt)
	assert.NotNil(lnt.serialization)
	assert.NotNil(lst.serialization)

	lst2 := MakeListType(StringType)
	assert.True(lst == lst2)
	assert.NotNil(lst.serialization)

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
	assert.NotNil(lbt.serialization)

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
	assert.NotNil(lbt.serialization)

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

	st := MakeStructType("Foo",
		[]string{"bar", "foo"},
		[]*Type{StringType, NumberType},
	)
	st2 := MakeStructType("Foo",
		[]string{"bar", "foo"},
		[]*Type{StringType, NumberType},
	)

	assert.True(st == st2)
	assert.NotNil(st.serialization)
}

func TestTypeCacheUnion(t *testing.T) {
	assert := assert.New(t)
	ut := MakeUnionType(NumberType)
	ut2 := MakeUnionType(NumberType)
	assert.True(ut == ut2)
	assert.NotNil(ut.serialization)

	ut = MakeUnionType(NumberType, StringType)
	ut2 = MakeUnionType(StringType, NumberType)
	assert.True(ut == ut2)

	ut = MakeUnionType(StringType, BoolType, NumberType)
	ut2 = MakeUnionType(NumberType, StringType, BoolType)
	assert.True(ut == ut2)
}

func TestTypeCacheCyclicStruct(t *testing.T) {
	assert := assert.New(t)

	st := MakeStructType("Foo",
		[]string{"foo"},
		[]*Type{MakeRefType(MakeCycleType(0))},
	)
	assert.True(st == st.Desc.(StructDesc).fields[0].t.Desc.(CompoundDesc).ElemTypes[0])
	assert.False(st.HasUnresolvedCycle())
	assert.NotNil(st.serialization)

	st2 := MakeStructType("Foo",
		[]string{"foo"},
		[]*Type{MakeRefType(MakeCycleType(0))},
	)
	assert.True(st2 == st2.Desc.(StructDesc).fields[0].t.Desc.(CompoundDesc).ElemTypes[0])
	assert.True(st == st2)
}

func TestTypeCacheCyclicStruct2(t *testing.T) {
	assert := assert.New(t)

	// Foo {
	//   bar: Cycle<1>
	//   foo: Cycle<0>
	// }
	st := MakeStructType("Foo",
		[]string{"bar", "foo"},
		[]*Type{
			MakeCycleType(1),
			MakeCycleType(0),
		},
	)
	assert.True(st.HasUnresolvedCycle())
	assert.Nil(st.serialization)
	// foo ref is cyclic
	assert.True(st == st.Desc.(StructDesc).fields[1].t)

	// Bar {
	//   baz: Cycle<1>
	//   foo: Foo {
	//     bar: Cycle<1>
	//     foo: Cycle<0>
	//   }
	// }
	st2 := MakeStructType("Bar",
		[]string{"baz", "foo"},
		[]*Type{
			MakeCycleType(1),
			st,
		},
	)
	assert.True(st2.HasUnresolvedCycle())
	assert.Nil(st2.serialization)
	// foo ref is cyclic
	assert.True(st2.Desc.(StructDesc).fields[1].t == st2.Desc.(StructDesc).fields[1].t.Desc.(StructDesc).fields[1].t)
	// bar ref is cyclic
	assert.True(st2 == st2.Desc.(StructDesc).fields[1].t.Desc.(StructDesc).fields[0].t)

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
	st3 := MakeStructType("Baz",
		[]string{"bar", "baz"},
		[]*Type{
			st2,
			MakeCycleType(0),
		},
	)
	assert.False(st3.HasUnresolvedCycle())
	assert.NotNil(st3.serialization)

	// foo ref is cyclic
	assert.True(st3.Desc.(StructDesc).fields[0].t.Desc.(StructDesc).fields[1].t == st3.Desc.(StructDesc).fields[0].t.Desc.(StructDesc).fields[1].t.Desc.(StructDesc).fields[1].t)
	// bar ref is cyclic
	assert.True(st3.Desc.(StructDesc).fields[0].t == st3.Desc.(StructDesc).fields[0].t.Desc.(StructDesc).fields[0].t.Desc.(StructDesc).fields[0].t)
	// baz second-level ref is cyclic
	assert.True(st3 == st3.Desc.(StructDesc).fields[0].t.Desc.(StructDesc).fields[0].t)
	// baz top-level ref is cyclic
	assert.True(st3 == st3.Desc.(StructDesc).fields[1].t)
}

func TestTypeCacheCyclicUnions(t *testing.T) {
	assert := assert.New(t)

	ut := MakeUnionType(MakeCycleType(0), NumberType, StringType, BoolType, BlobType, ValueType, TypeType)
	st := MakeStructType("Foo",
		[]string{"foo"},
		[]*Type{ut},
	)

	assert.True(ut.Desc.(CompoundDesc).ElemTypes[0].Kind() == CycleKind)
	// That the Struct / Cycle landed in index 1 was found empirically.
	assert.True(st == st.Desc.(StructDesc).fields[0].t.Desc.(CompoundDesc).ElemTypes[1])
	// ut contains an explicit Cycle type; noms must not surrepticiously change existing types so we can be sure that the Union within st is different in that the cycle has been resolved.
	assert.False(ut == st.Desc.(StructDesc).fields[0].t)

	// Note that the union in this second case is created with a different ordering of its type arguments.
	ut2 := MakeUnionType(NumberType, StringType, BoolType, BlobType, ValueType, TypeType, MakeCycleType(0))
	st2 := MakeStructType("Foo",
		[]string{"foo"},
		[]*Type{ut2},
	)
	assert.True(ut2.Desc.(CompoundDesc).ElemTypes[0].Kind() == CycleKind)
	assert.True(st2 == st2.Desc.(StructDesc).fields[0].t.Desc.(CompoundDesc).ElemTypes[1])
	assert.False(ut2 == st2.Desc.(StructDesc).fields[0].t)

	assert.True(ut == ut2)
	assert.True(st == st2)
}

func TestInvalidCyclesAndUnions(t *testing.T) {
	assert := assert.New(t)

	assert.Panics(func() {
		MakeStructType("A",
			[]string{"a"},
			[]*Type{MakeStructType("A", []string{"a"}, []*Type{MakeCycleType(1)})})
	})
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
		f := desc.Field(k)
		assert.True(v == f)
	}
}
