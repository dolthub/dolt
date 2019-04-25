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
		act := simplifyStructFields(in, typeset{}, false)
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
			act := simplifyType(in, intersectStructs)
			assert.True(exp.Equals(act), "Expected: %s\nActual: %s", exp.Describe(context.Background()), act.Describe(context.Background()))
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
		testSame(makeCompoundType(ListKind, BoolType))
		testSame(makeCompoundType(SetKind, BoolType))
		testSame(makeCompoundType(RefKind, BoolType))
		testSame(makeCompoundType(MapKind, BoolType, FloaTType))

		{
			// Cannot do equals on cycle types
			in := MakeCycleType("ABC")
			act := simplifyType(in, intersectStructs)
			assert.Equal(in, act)
		}

		test(makeUnionType(BoolType), BoolType)
		test(makeUnionType(BoolType, BoolType), BoolType)
		testSame(makeUnionType(BoolType, FloaTType))
		test(makeUnionType(FloaTType, BoolType), makeUnionType(BoolType, FloaTType))
		test(makeUnionType(FloaTType, BoolType), makeUnionType(BoolType, FloaTType))

		testSame(makeCompoundType(ListKind, makeUnionType(BoolType, FloaTType)))
		test(makeCompoundType(ListKind, makeUnionType(BoolType)), makeCompoundType(ListKind, BoolType))
		test(makeCompoundType(ListKind, makeUnionType(BoolType, BoolType)), makeCompoundType(ListKind, BoolType))

		testSame(makeStructType("", nil))
		testSame(makeStructType("", structTypeFields{}))
		testSame(makeStructType("", structTypeFields{
			StructField{"b", BoolType, false},
			StructField{"s", StringType, !intersectStructs},
		}))
		test(
			makeStructType("", structTypeFields{
				StructField{"a", BoolType, false},
				StructField{"b", makeUnionType(FloaTType, FloaTType), false},
			}),
			makeStructType("", structTypeFields{
				StructField{"a", BoolType, false},
				StructField{"b", FloaTType, false},
			}),
		)
		// non named structs do not create cycles.
		testSame(makeStructType("", structTypeFields{
			StructField{"b", BoolType, false},
			StructField{
				"s",
				makeStructType("", structTypeFields{
					StructField{"c", StringType, false},
				}),
				!intersectStructs,
			},
		}))

		// merge non named structs in unions
		test(
			makeCompoundType(
				UnionKind,
				makeStructType("", structTypeFields{
					StructField{"a", BoolType, false},
				}),
				makeStructType("", structTypeFields{
					StructField{"b", BoolType, false},
				}),
			),
			makeStructType("", structTypeFields{
				StructField{"a", BoolType, !intersectStructs},
				StructField{"b", BoolType, !intersectStructs},
			}),
		)

		// List<Float> | List<Bool> -> List<Bool | Float>
		for _, k := range []NomsKind{ListKind, SetKind, RefKind} {
			test(
				makeCompoundType(
					UnionKind,
					makeCompoundType(k, FloaTType),
					makeCompoundType(k, BoolType),
				),
				makeCompoundType(k,
					makeUnionType(BoolType, FloaTType),
				),
			)
		}

		// Map<Float, Float> | List<Bool, Float> -> List<Bool | Float, Float>
		test(
			makeCompoundType(
				UnionKind,
				makeCompoundType(MapKind, FloaTType, FloaTType),
				makeCompoundType(MapKind, BoolType, FloaTType),
			),
			makeCompoundType(MapKind,
				makeUnionType(BoolType, FloaTType),
				FloaTType,
			),
		)

		// Map<Float, Float> | List<Float, Bool> -> List<Float, Bool | Float>
		test(
			makeCompoundType(
				UnionKind,
				makeCompoundType(MapKind, FloaTType, FloaTType),
				makeCompoundType(MapKind, FloaTType, BoolType),
			),
			makeCompoundType(MapKind,
				FloaTType,
				makeUnionType(BoolType, FloaTType),
			),
		)

		// union flattening
		test(
			makeUnionType(FloaTType, makeUnionType(FloaTType, BoolType)),
			makeUnionType(BoolType, FloaTType),
		)

		{
			// Cannot do equals on cycle types
			in := makeUnionType(MakeCycleType("A"), MakeCycleType("A"))
			exp := MakeCycleType("A")
			act := simplifyType(in, intersectStructs)
			assert.Equal(exp, act)
		}

		{
			// Cannot do equals on cycle types
			in := makeCompoundType(UnionKind,
				makeCompoundType(ListKind, MakeCycleType("A")),
				makeCompoundType(ListKind, MakeCycleType("A")))
			exp := makeCompoundType(ListKind, MakeCycleType("A"))
			act := simplifyType(in, intersectStructs)
			assert.Equal(exp, act, "Expected: %s\nActual: %s", exp.Describe(context.Background()), act.Describe(context.Background()))
		}

		testSame(makeStructType("A", nil))
		testSame(makeStructType("A", structTypeFields{}))
		testSame(makeStructType("A", structTypeFields{
			StructField{"a", BoolType, !intersectStructs},
		}))
		test(
			makeStructType("A", structTypeFields{
				StructField{"a", makeUnionType(BoolType, BoolType, FloaTType), false},
			}),
			makeStructType("A", structTypeFields{
				StructField{"a", makeUnionType(BoolType, FloaTType), false},
			}),
		)

		testSame(
			makeStructType("A", structTypeFields{
				StructField{
					"a",
					makeStructType("B", structTypeFields{
						StructField{"b", BoolType, !intersectStructs},
					}),
					false,
				},
			}),
		)

		{
			// Create pointer cycle manually.
			exp := makeStructType("A", structTypeFields{
				StructField{
					"a",
					BoolType, // placeholder
					!intersectStructs,
				},
			})
			exp.Desc.(StructDesc).fields[0].Type = exp
			test(
				makeStructType("A", structTypeFields{
					StructField{
						"a",
						makeStructType("A", structTypeFields{}),
						false,
					},
				}),
				exp,
			)
		}

		{
			a := makeStructType("S", structTypeFields{})
			exp := makeCompoundType(MapKind, a, a)
			test(
				makeCompoundType(MapKind,
					makeStructType("S", structTypeFields{}),
					makeStructType("S", structTypeFields{}),
				),
				exp,
			)
		}

		{
			a := makeStructType("S", structTypeFields{
				StructField{"a", BoolType, !intersectStructs},
				StructField{"b", makeUnionType(BoolType, StringType), false},
			})
			exp := makeCompoundType(MapKind, a, a)
			test(
				makeCompoundType(MapKind,
					makeStructType("S", structTypeFields{
						StructField{"a", BoolType, false},
						StructField{"b", StringType, false},
					}),
					makeStructType("S", structTypeFields{
						StructField{"b", BoolType, false},
					}),
				),
				exp,
			)
		}

		// Non named do not get merged outside unions
		testSame(
			makeCompoundType(MapKind,
				makeStructType("", structTypeFields{
					StructField{"a", BoolType, false},
					StructField{"b", StringType, false},
				}),
				makeStructType("", structTypeFields{
					StructField{"b", BoolType, false},
				}),
			),
		)

		// Cycle in union
		{
			a := makeStructType("A", structTypeFields{
				StructField{
					"a",
					BoolType, // placeholder
					!intersectStructs,
				},
			})
			a.Desc.(StructDesc).fields[0].Type = a
			exp := makeUnionType(FloaTType, a, TypeType)
			test(
				makeCompoundType(UnionKind,
					makeStructType("A", structTypeFields{
						StructField{
							"a",
							makeStructType("A", structTypeFields{}),
							false,
						},
					}),
					FloaTType,
					TypeType,
				),
				exp,
			)
		}

		test(
			makeCompoundType(RefKind,
				makeCompoundType(UnionKind,
					makeCompoundType(ListKind,
						BoolType,
					),
					makeCompoundType(SetKind,
						makeUnionType(StringType, FloaTType),
					),
				),
			),
			makeCompoundType(RefKind,
				makeCompoundType(UnionKind,
					makeCompoundType(ListKind,
						BoolType,
					),
					makeCompoundType(SetKind,
						makeUnionType(FloaTType, StringType),
					),
				),
			),
		)
	}

	t.Run("Union", func(*testing.T) {
		run(false)
	})
	t.Run("IntersectStructs", func(*testing.T) {
		run(true)
	})
}
