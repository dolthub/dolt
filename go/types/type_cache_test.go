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

	assert.True(ut.Desc.(CompoundDesc).ElemTypes[5].Kind() == CycleKind)
	assert.True(st == st.Desc.(StructDesc).fields[0].t.Desc.(CompoundDesc).ElemTypes[5])
	assert.False(ut.Equals(st.Desc.(StructDesc).fields[0].t))

	// Note that the union in this second case has a different provided ordering of it's element types.
	ut2 := MakeUnionType(NumberType, StringType, BoolType, BlobType, ValueType, TypeType, MakeCycleType(0))
	st2 := MakeStructType("Foo",
		[]string{"foo"},
		[]*Type{ut2},
	)
	assert.True(ut2.Desc.(CompoundDesc).ElemTypes[5].Kind() == CycleKind)
	assert.True(st2 == st2.Desc.(StructDesc).fields[0].t.Desc.(CompoundDesc).ElemTypes[5])
	assert.False(ut2.Equals(st2.Desc.(StructDesc).fields[0].t))

	assert.True(ut == ut2)
	assert.True(st == st2)
}
