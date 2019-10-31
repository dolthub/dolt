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

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimplifyStructFields(t *testing.T) {
	assert := assert.New(t)

	test := func(in []structTypeFields, exp structTypeFields) {
		// simplifier := newSimplifier(false)
		act, err := simplifyStructFields(in, typeset{}, false)
		assert.NoError(err)
		assert.Equal(act, exp)
	}

	test([]structTypeFields{
		{
			StructField{"a", PrimitiveTypeMap[BoolKind], false},
		},
		{
			StructField{"a", PrimitiveTypeMap[BoolKind], false},
		},
	},
		structTypeFields{
			StructField{"a", PrimitiveTypeMap[BoolKind], false},
		},
	)

	test([]structTypeFields{
		{
			StructField{"a", PrimitiveTypeMap[BoolKind], false},
		},
		{
			StructField{"b", PrimitiveTypeMap[BoolKind], false},
		},
	},
		structTypeFields{
			StructField{"a", PrimitiveTypeMap[BoolKind], true},
			StructField{"b", PrimitiveTypeMap[BoolKind], true},
		},
	)

	test([]structTypeFields{
		{
			StructField{"a", PrimitiveTypeMap[BoolKind], false},
		},
		{
			StructField{"a", PrimitiveTypeMap[BoolKind], true},
		},
	},
		structTypeFields{
			StructField{"a", PrimitiveTypeMap[BoolKind], true},
		},
	)
}

func TestSimplifyType(t *testing.T) {
	assert := assert.New(t)

	run := func(intersectStructs bool) {
		test := func(in, exp *Type) {
			act, err := simplifyType(in, intersectStructs)
			assert.NoError(err)
			assert.True(exp.Equals(act), "Expected: %s\nActual: %s", mustString(exp.Describe(context.Background())), mustString(act.Describe(context.Background())))
		}
		testSame := func(t *Type) {
			test(t, t)
		}

		testSame(PrimitiveTypeMap[BlobKind])
		testSame(PrimitiveTypeMap[BoolKind])
		testSame(PrimitiveTypeMap[FloatKind])
		testSame(PrimitiveTypeMap[StringKind])
		testSame(PrimitiveTypeMap[TypeKind])
		testSame(PrimitiveTypeMap[ValueKind])
		testSame(mustType(makeCompoundType(ListKind, PrimitiveTypeMap[BoolKind])))
		testSame(mustType(makeCompoundType(SetKind, PrimitiveTypeMap[BoolKind])))
		testSame(mustType(makeCompoundType(RefKind, PrimitiveTypeMap[BoolKind])))
		testSame(mustType(makeCompoundType(MapKind, PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])))

		{
			// Cannot do equals on cycle types
			in := MakeCycleType("ABC")
			act := mustType(simplifyType(in, intersectStructs))
			assert.Equal(in, act)
		}

		test(mustType(makeUnionType(PrimitiveTypeMap[BoolKind])), PrimitiveTypeMap[BoolKind])
		test(mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[BoolKind])), PrimitiveTypeMap[BoolKind])
		testSame(mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])))
		test(mustType(makeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])), mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])))
		test(mustType(makeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])), mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])))

		testSame(mustType(makeCompoundType(ListKind, mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])))))
		test(mustType(makeCompoundType(ListKind, mustType(makeUnionType(PrimitiveTypeMap[BoolKind])))), mustType(makeCompoundType(ListKind, PrimitiveTypeMap[BoolKind])))
		test(mustType(makeCompoundType(ListKind, mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[BoolKind])))), mustType(makeCompoundType(ListKind, PrimitiveTypeMap[BoolKind])))

		testSame(mustType(makeStructType("", nil)))
		testSame(mustType(makeStructType("", structTypeFields{})))
		testSame(mustType(makeStructType("", structTypeFields{
			StructField{"b", PrimitiveTypeMap[BoolKind], false},
			StructField{"s", PrimitiveTypeMap[StringKind], !intersectStructs},
		})))
		test(
			mustType(makeStructType("", structTypeFields{
				StructField{"a", PrimitiveTypeMap[BoolKind], false},
				StructField{"b", mustType(makeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[FloatKind])), false},
			})),
			mustType(makeStructType("", structTypeFields{
				StructField{"a", PrimitiveTypeMap[BoolKind], false},
				StructField{"b", PrimitiveTypeMap[FloatKind], false},
			})),
		)
		// non named structs do not create cycles.
		testSame(mustType(makeStructType("", structTypeFields{
			StructField{"b", PrimitiveTypeMap[BoolKind], false},
			StructField{
				"s",
				mustType(makeStructType("", structTypeFields{
					StructField{"c", PrimitiveTypeMap[StringKind], false},
				})),
				!intersectStructs,
			},
		})))

		// merge non named structs in unions
		test(
			mustType(makeCompoundType(
				UnionKind,
				mustType(makeStructType("", structTypeFields{
					StructField{"a", PrimitiveTypeMap[BoolKind], false},
				})),
				mustType(makeStructType("", structTypeFields{
					StructField{"b", PrimitiveTypeMap[BoolKind], false},
				})),
			)),
			mustType(makeStructType("", structTypeFields{
				StructField{"a", PrimitiveTypeMap[BoolKind], !intersectStructs},
				StructField{"b", PrimitiveTypeMap[BoolKind], !intersectStructs},
			})),
		)

		// List<Float> | List<Bool> -> List<Bool | Float>
		for _, k := range []NomsKind{ListKind, SetKind, RefKind} {
			test(
				mustType(makeCompoundType(
					UnionKind,
					mustType(makeCompoundType(k, PrimitiveTypeMap[FloatKind])),
					mustType(makeCompoundType(k, PrimitiveTypeMap[BoolKind])),
				)),
				mustType(makeCompoundType(k,
					mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])),
				)),
			)
		}

		// Map<Float, Float> | List<Bool, Float> -> List<Bool | Float, Float>
		test(
			mustType(makeCompoundType(
				UnionKind,
				mustType(makeCompoundType(MapKind, PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[FloatKind])),
				mustType(makeCompoundType(MapKind, PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])),
			)),
			mustType(makeCompoundType(MapKind,
				mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])),
				PrimitiveTypeMap[FloatKind],
			)),
		)

		// Map<Float, Float> | List<Float, Bool> -> List<Float, Bool | Float>
		test(
			mustType(makeCompoundType(
				UnionKind,
				mustType(makeCompoundType(MapKind, PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[FloatKind])),
				mustType(makeCompoundType(MapKind, PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])),
			)),
			mustType(makeCompoundType(MapKind,
				PrimitiveTypeMap[FloatKind],
				mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])),
			)),
		)

		// union flattening
		test(
			mustType(makeUnionType(PrimitiveTypeMap[FloatKind], mustType(makeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))),
			mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])),
		)

		{
			// Cannot do equals on cycle types
			in := mustType(makeUnionType(MakeCycleType("A"), MakeCycleType("A")))
			exp := MakeCycleType("A")
			act := mustType(simplifyType(in, intersectStructs))
			assert.Equal(exp, act)
		}

		{
			// Cannot do equals on cycle types
			in := mustType(makeCompoundType(UnionKind,
				mustType(makeCompoundType(ListKind, MakeCycleType("A"))),
				mustType(makeCompoundType(ListKind, MakeCycleType("A")))))
			exp := mustType(makeCompoundType(ListKind, MakeCycleType("A")))
			act := mustType(simplifyType(in, intersectStructs))
			assert.Equal(exp, act, "Expected: %s\nActual: %s", mustString(exp.Describe(context.Background())), mustString(act.Describe(context.Background())))
		}

		testSame(mustType(makeStructType("A", nil)))
		testSame(mustType(makeStructType("A", structTypeFields{})))
		testSame(mustType(makeStructType("A", structTypeFields{
			StructField{"a", PrimitiveTypeMap[BoolKind], !intersectStructs},
		})))
		test(
			mustType(makeStructType("A", structTypeFields{
				StructField{"a", mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])), false},
			})),
			mustType(makeStructType("A", structTypeFields{
				StructField{"a", mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])), false},
			})),
		)

		testSame(
			mustType(makeStructType("A", structTypeFields{
				StructField{
					"a",
					mustType(makeStructType("B", structTypeFields{
						StructField{"b", PrimitiveTypeMap[BoolKind], !intersectStructs},
					})),
					false,
				},
			}),
			))

		{
			// Create pointer cycle manually.
			exp := mustType(makeStructType("A", structTypeFields{
				StructField{
					"a",
					PrimitiveTypeMap[BoolKind], // placeholder
					!intersectStructs,
				},
			}))
			exp.Desc.(StructDesc).fields[0].Type = exp
			test(
				mustType(makeStructType("A", structTypeFields{
					StructField{
						"a",
						mustType(makeStructType("A", structTypeFields{})),
						false,
					},
				})),
				exp,
			)
		}

		{
			a := mustType(makeStructType("S", structTypeFields{}))
			exp := mustType(makeCompoundType(MapKind, a, a))
			test(
				mustType(makeCompoundType(MapKind,
					mustType(makeStructType("S", structTypeFields{})),
					mustType(makeStructType("S", structTypeFields{})),
				)),
				exp,
			)
		}

		{
			a := mustType(makeStructType("S", structTypeFields{
				StructField{"a", PrimitiveTypeMap[BoolKind], !intersectStructs},
				StructField{"b", mustType(makeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind])), false},
			}))
			exp := mustType(makeCompoundType(MapKind, a, a))
			test(
				mustType(makeCompoundType(MapKind,
					mustType(makeStructType("S", structTypeFields{
						StructField{"a", PrimitiveTypeMap[BoolKind], false},
						StructField{"b", PrimitiveTypeMap[StringKind], false},
					})),
					mustType(makeStructType("S", structTypeFields{
						StructField{"b", PrimitiveTypeMap[BoolKind], false},
					})),
				)),
				exp,
			)
		}

		// Non named do not get merged outside unions
		testSame(
			mustType(makeCompoundType(MapKind,
				mustType(makeStructType("", structTypeFields{
					StructField{"a", PrimitiveTypeMap[BoolKind], false},
					StructField{"b", PrimitiveTypeMap[StringKind], false},
				})),
				mustType(makeStructType("", structTypeFields{
					StructField{"b", PrimitiveTypeMap[BoolKind], false},
				})),
			)),
		)

		// Cycle in union
		{
			a := mustType(makeStructType("A", structTypeFields{
				StructField{
					"a",
					PrimitiveTypeMap[BoolKind], // placeholder
					!intersectStructs,
				},
			}))
			a.Desc.(StructDesc).fields[0].Type = a
			exp := mustType(makeUnionType(PrimitiveTypeMap[FloatKind], a, PrimitiveTypeMap[TypeKind]))
			test(
				mustType(makeCompoundType(UnionKind,
					mustType(makeStructType("A", structTypeFields{
						StructField{
							"a",
							mustType(makeStructType("A", structTypeFields{})),
							false,
						},
					})),
					PrimitiveTypeMap[FloatKind],
					PrimitiveTypeMap[TypeKind],
				)),
				exp,
			)
		}

		test(
			mustType(makeCompoundType(RefKind,
				mustType(makeCompoundType(UnionKind,
					mustType(makeCompoundType(ListKind,
						PrimitiveTypeMap[BoolKind],
					)),
					mustType(makeCompoundType(SetKind,
						mustType(makeUnionType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[FloatKind])),
					)),
				)),
			)),
			mustType(makeCompoundType(RefKind,
				mustType(makeCompoundType(UnionKind,
					mustType(makeCompoundType(ListKind,
						PrimitiveTypeMap[BoolKind],
					)),
					mustType(makeCompoundType(SetKind,
						mustType(makeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])),
					)),
				)),
			)),
		)
	}

	t.Run("Union", func(*testing.T) {
		run(false)
	})
	t.Run("IntersectStructs", func(*testing.T) {
		run(true)
	})
}
