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
			StructField{"a", BoolType, false},
		},
		{
			StructField{"a", BoolType, false},
		},
	},
		structTypeFields{
			StructField{"a", BoolType, false},
		},
	)

	test([]structTypeFields{
		{
			StructField{"a", BoolType, false},
		},
		{
			StructField{"b", BoolType, false},
		},
	},
		structTypeFields{
			StructField{"a", BoolType, true},
			StructField{"b", BoolType, true},
		},
	)

	test([]structTypeFields{
		{
			StructField{"a", BoolType, false},
		},
		{
			StructField{"a", BoolType, true},
		},
	},
		structTypeFields{
			StructField{"a", BoolType, true},
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

		testSame(BlobType)
		testSame(BoolType)
		testSame(FloaTType)
		testSame(StringType)
		testSame(TypeType)
		testSame(ValueType)
		testSame(mustType(makeCompoundType(ListKind, BoolType)))
		testSame(mustType(makeCompoundType(SetKind, BoolType)))
		testSame(mustType(makeCompoundType(RefKind, BoolType)))
		testSame(mustType(makeCompoundType(MapKind, BoolType, FloaTType)))

		{
			// Cannot do equals on cycle types
			in := MakeCycleType("ABC")
			act := mustType(simplifyType(in, intersectStructs))
			assert.Equal(in, act)
		}

		test(mustType(makeUnionType(BoolType)), BoolType)
		test(mustType(makeUnionType(BoolType, BoolType)), BoolType)
		testSame(mustType(makeUnionType(BoolType, FloaTType)))
		test(mustType(makeUnionType(FloaTType, BoolType)), mustType(makeUnionType(BoolType, FloaTType)))
		test(mustType(makeUnionType(FloaTType, BoolType)), mustType(makeUnionType(BoolType, FloaTType)))

		testSame(mustType(makeCompoundType(ListKind, mustType(makeUnionType(BoolType, FloaTType)))))
		test(mustType(makeCompoundType(ListKind, mustType(makeUnionType(BoolType)))), mustType(makeCompoundType(ListKind, BoolType)))
		test(mustType(makeCompoundType(ListKind, mustType(makeUnionType(BoolType, BoolType)))), mustType(makeCompoundType(ListKind, BoolType)))

		testSame(mustType(makeStructType("", nil)))
		testSame(mustType(makeStructType("", structTypeFields{})))
		testSame(mustType(makeStructType("", structTypeFields{
			StructField{"b", BoolType, false},
			StructField{"s", StringType, !intersectStructs},
		})))
		test(
			mustType(makeStructType("", structTypeFields{
				StructField{"a", BoolType, false},
				StructField{"b", mustType(makeUnionType(FloaTType, FloaTType)), false},
			})),
			mustType(makeStructType("", structTypeFields{
				StructField{"a", BoolType, false},
				StructField{"b", FloaTType, false},
			})),
		)
		// non named structs do not create cycles.
		testSame(mustType(makeStructType("", structTypeFields{
			StructField{"b", BoolType, false},
			StructField{
				"s",
				mustType(makeStructType("", structTypeFields{
					StructField{"c", StringType, false},
				})),
				!intersectStructs,
			},
		})))

		// merge non named structs in unions
		test(
			mustType(makeCompoundType(
				UnionKind,
				mustType(makeStructType("", structTypeFields{
					StructField{"a", BoolType, false},
				})),
				mustType(makeStructType("", structTypeFields{
					StructField{"b", BoolType, false},
				})),
			)),
			mustType(makeStructType("", structTypeFields{
				StructField{"a", BoolType, !intersectStructs},
				StructField{"b", BoolType, !intersectStructs},
			})),
		)

		// List<Float> | List<Bool> -> List<Bool | Float>
		for _, k := range []NomsKind{ListKind, SetKind, RefKind} {
			test(
				mustType(makeCompoundType(
					UnionKind,
					mustType(makeCompoundType(k, FloaTType)),
					mustType(makeCompoundType(k, BoolType)),
				)),
				mustType(makeCompoundType(k,
					mustType(makeUnionType(BoolType, FloaTType)),
				)),
			)
		}

		// Map<Float, Float> | List<Bool, Float> -> List<Bool | Float, Float>
		test(
			mustType(makeCompoundType(
				UnionKind,
				mustType(makeCompoundType(MapKind, FloaTType, FloaTType)),
				mustType(makeCompoundType(MapKind, BoolType, FloaTType)),
			)),
			mustType(makeCompoundType(MapKind,
				mustType(makeUnionType(BoolType, FloaTType)),
				FloaTType,
			)),
		)

		// Map<Float, Float> | List<Float, Bool> -> List<Float, Bool | Float>
		test(
			mustType(makeCompoundType(
				UnionKind,
				mustType(makeCompoundType(MapKind, FloaTType, FloaTType)),
				mustType(makeCompoundType(MapKind, FloaTType, BoolType)),
			)),
			mustType(makeCompoundType(MapKind,
				FloaTType,
				mustType(makeUnionType(BoolType, FloaTType)),
			)),
		)

		// union flattening
		test(
			mustType(makeUnionType(FloaTType, mustType(makeUnionType(FloaTType, BoolType)))),
			mustType(makeUnionType(BoolType, FloaTType)),
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
			StructField{"a", BoolType, !intersectStructs},
		})))
		test(
			mustType(makeStructType("A", structTypeFields{
				StructField{"a", mustType(makeUnionType(BoolType, BoolType, FloaTType)), false},
			})),
			mustType(makeStructType("A", structTypeFields{
				StructField{"a", mustType(makeUnionType(BoolType, FloaTType)), false},
			})),
		)

		testSame(
			mustType(makeStructType("A", structTypeFields{
				StructField{
					"a",
					mustType(makeStructType("B", structTypeFields{
						StructField{"b", BoolType, !intersectStructs},
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
					BoolType, // placeholder
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
				StructField{"a", BoolType, !intersectStructs},
				StructField{"b", mustType(makeUnionType(BoolType, StringType)), false},
			}))
			exp := mustType(makeCompoundType(MapKind, a, a))
			test(
				mustType(makeCompoundType(MapKind,
					mustType(makeStructType("S", structTypeFields{
						StructField{"a", BoolType, false},
						StructField{"b", StringType, false},
					})),
					mustType(makeStructType("S", structTypeFields{
						StructField{"b", BoolType, false},
					})),
				)),
				exp,
			)
		}

		// Non named do not get merged outside unions
		testSame(
			mustType(makeCompoundType(MapKind,
				mustType(makeStructType("", structTypeFields{
					StructField{"a", BoolType, false},
					StructField{"b", StringType, false},
				})),
				mustType(makeStructType("", structTypeFields{
					StructField{"b", BoolType, false},
				})),
			)),
		)

		// Cycle in union
		{
			a := mustType(makeStructType("A", structTypeFields{
				StructField{
					"a",
					BoolType, // placeholder
					!intersectStructs,
				},
			}))
			a.Desc.(StructDesc).fields[0].Type = a
			exp := mustType(makeUnionType(FloaTType, a, TypeType))
			test(
				mustType(makeCompoundType(UnionKind,
					mustType(makeStructType("A", structTypeFields{
						StructField{
							"a",
							mustType(makeStructType("A", structTypeFields{})),
							false,
						},
					})),
					FloaTType,
					TypeType,
				)),
				exp,
			)
		}

		test(
			mustType(makeCompoundType(RefKind,
				mustType(makeCompoundType(UnionKind,
					mustType(makeCompoundType(ListKind,
						BoolType,
					)),
					mustType(makeCompoundType(SetKind,
						mustType(makeUnionType(StringType, FloaTType)),
					)),
				)),
			)),
			mustType(makeCompoundType(RefKind,
				mustType(makeCompoundType(UnionKind,
					mustType(makeCompoundType(ListKind,
						BoolType,
					)),
					mustType(makeCompoundType(SetKind,
						mustType(makeUnionType(FloaTType, StringType)),
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
