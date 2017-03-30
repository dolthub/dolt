// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestWalkStructTypes(t *testing.T) {
	assert := assert.New(t)

	test := func(t *Type, exp []string) {
		act := []string{}
		t2, changed := walkStructTypes(staticTypeCache, t, nil, func(st *Type, cycle bool) (*Type, bool) {
			name := st.Desc.(StructDesc).Name
			if cycle {
				act = append(act, "Cycle<"+name+">")
			} else {
				act = append(act, name)
			}
			return st, false
		})
		assert.False(changed)
		assert.True(t.Equals(t2))
		assert.Equal(exp, act)
	}

	test(BoolType, []string{})
	test(BoolType, []string{})
	test(NumberType, []string{})
	test(StringType, []string{})
	test(BlobType, []string{})
	test(ValueType, []string{})
	test(TypeType, []string{})
	test(MakeCycleType(1), []string{})

	test(MakeStructType("A"), []string{"A"})
	test(MakeListType(MakeStructType("A")), []string{"A"})
	test(MakeRefType(MakeStructType("A")), []string{"A"})
	test(MakeSetType(MakeStructType("A")), []string{"A"})
	test(MakeMapType(MakeStructType("A"), MakeStructType("B")), []string{"A", "B"})

	test(
		MakeStructType("A",
			StructField{"l", MakeListType(NumberType), false},
			StructField{"m", MakeMapType(BoolType, StringType), false},
			StructField{"r", MakeRefType(BlobType), false},
			StructField{"s", MakeSetType(TypeType), false},
			StructField{"u", MakeUnionType(ValueType, NumberType), false},
		),
		[]string{"A"},
	)

	test(MakeStructType("A", StructField{"c", MakeCycleType(0), false}), []string{"Cycle<A>", "A"})
}

func TestWalkStructTypesMutate(t *testing.T) {
	assert := assert.New(t)

	test := func(t *Type, exp *Type) {
		act, changed := walkStructTypes(staticTypeCache, t, nil, func(st *Type, cycle bool) (*Type, bool) {
			name := st.Desc.(StructDesc).Name
			newName := "Changed" + name
			if cycle {
				newName = "ChangedCyclic" + name
				return MakeStructType(newName), true
			}
			return MakeStructType(newName, st.Desc.(StructDesc).fields...), true
		})
		assert.True(changed)
		assert.True(exp.Equals(act), "Expected: %s, Actual: %s", exp.Describe(), act.Describe())
	}

	test(MakeStructType("A"), MakeStructType("ChangedA"))
	test(MakeListType(MakeStructType("A")), MakeListType(MakeStructType("ChangedA")))
	test(MakeRefType(MakeStructType("A")), MakeRefType(MakeStructType("ChangedA")))
	test(MakeSetType(MakeStructType("A")), MakeSetType(MakeStructType("ChangedA")))
	test(MakeMapType(MakeStructType("A"), MakeStructType("B")), MakeMapType(MakeStructType("ChangedA"), MakeStructType("ChangedB")))

	test(
		MakeStructType("A",
			StructField{"b", MakeStructType("B"), false},
			StructField{"c", MakeStructType("C"), false},
			StructField{"d", MakeStructType("D"), false},
		),
		MakeStructType("ChangedA",
			StructField{"b", MakeStructType("ChangedB"), false},
			StructField{"c", MakeStructType("ChangedC"), false},
			StructField{"d", MakeStructType("ChangedD"), false},
		),
	)

	test(
		MakeStructType("A",
			StructField{"c", MakeCycleType(0), false},
		),
		MakeStructType("ChangedA",
			StructField{"c", MakeStructType("ChangedCyclicA"), false},
		),
	)
}
