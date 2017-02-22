package graphql_test

import (
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/testutil"
)

func TestValidate_OverlappingFieldsCanBeMerged_UniqueFields(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment uniqueFields on Dog {
        name
        nickname
      }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_IdenticalFields(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment mergeIdenticalFields on Dog {
        name
        name
      }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_IdenticalFieldsWithIdenticalArgs(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment mergeIdenticalFieldsWithIdenticalArgs on Dog {
        doesKnowCommand(dogCommand: SIT)
        doesKnowCommand(dogCommand: SIT)
      }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_IdenticalFieldsWithIdenticalDirectives(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment mergeSameFieldsWithSameDirectives on Dog {
        name @include(if: true)
        name @include(if: true)
      }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_DifferentArgsWithDifferentAliases(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment differentArgsWithDifferentAliases on Dog {
        knowsSit: doesKnowCommand(dogCommand: SIT)
        knowsDown: doesKnowCommand(dogCommand: DOWN)
      }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_DifferentDirectivesWithDifferentAliases(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment differentDirectivesWithDifferentAliases on Dog {
        nameIfTrue: name @include(if: true)
        nameIfFalse: name @include(if: false)
      }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_DifferentSkipIncludeDirectivesAccepted(t *testing.T) {
	// Note: Differing skip/include directives don't create an ambiguous return
	// value and are acceptable in conditions where differing runtime values
	// may have the same desired effect of including or skipping a field.
	testutil.ExpectPassesRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment differentDirectivesWithDifferentAliases on Dog {
        name @include(if: true)
        name @include(if: false)
      }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_SameAliasesWithDifferentFieldTargets(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment sameAliasesWithDifferentFieldTargets on Dog {
        fido: name
        fido: nickname
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fields "fido" conflict because name and nickname are different fields.`, 3, 9, 4, 9),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_SameAliasesAllowedOnNonOverlappingFields(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment sameAliasesWithDifferentFieldTargets on Pet {
        ... on Dog {
          name
        }
        ... on Cat {
          name: nickname
        }
      }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_AliasMaskingDirectFieldAccess(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment aliasMaskingDirectFieldAccess on Dog {
        name: nickname
        name
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fields "name" conflict because nickname and name are different fields.`, 3, 9, 4, 9),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_DifferentArgs_SecondAddsAnArgument(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment conflictingArgs on Dog {
        doesKnowCommand
        doesKnowCommand(dogCommand: HEEL)
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fields "doesKnowCommand" conflict because they have differing arguments.`, 3, 9, 4, 9),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_DifferentArgs_SecondMissingAnArgument(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment conflictingArgs on Dog {
        doesKnowCommand(dogCommand: SIT)
        doesKnowCommand
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fields "doesKnowCommand" conflict because they have differing arguments.`, 3, 9, 4, 9),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_ConflictingArgs(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment conflictingArgs on Dog {
        doesKnowCommand(dogCommand: SIT)
        doesKnowCommand(dogCommand: HEEL)
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fields "doesKnowCommand" conflict because they have differing arguments.`, 3, 9, 4, 9),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_AllowDifferentArgsWhereNoConflictIsPossible(t *testing.T) {
	// This is valid since no object can be both a "Dog" and a "Cat", thus
	// these fields can never overlap.
	testutil.ExpectPassesRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      fragment conflictingArgs on Pet {
        ... on Dog {
          name(surname: true)
        }
        ... on Cat {
          name
        }
      }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_EncountersConflictInFragments(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      {
        ...A
        ...B
      }
      fragment A on Type {
        x: a
      }
      fragment B on Type {
        x: b
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fields "x" conflict because a and b are different fields.`, 7, 9, 10, 9),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_ReportsEachConflictOnce(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      {
        f1 {
          ...A
          ...B
        }
        f2 {
          ...B
          ...A
        }
        f3 {
          ...A
          ...B
          x: c
        }
      }
      fragment A on Type {
        x: a
      }
      fragment B on Type {
        x: b
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fields "x" conflict because a and b are different fields.`, 18, 9, 21, 9),
		testutil.RuleError(`Fields "x" conflict because a and c are different fields.`, 18, 9, 14, 11),
		testutil.RuleError(`Fields "x" conflict because b and c are different fields.`, 21, 9, 14, 11),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_DeepConflict(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      {
        field {
          x: a
        },
        field {
          x: b
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fields "field" conflict because subfields "x" conflict because a and b are different fields.`,
			3, 9,
			4, 11,
			6, 9,
			7, 11),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_DeepConflictWithMultipleIssues(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      {
        field {
          x: a
          y: c
        },
        field {
          x: b
          y: d
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(
			`Fields "field" conflict because subfields "x" conflict because a and b are different fields and `+
				`subfields "y" conflict because c and d are different fields.`,
			3, 9,
			4, 11,
			5, 11,
			7, 9,
			8, 11,
			9, 11),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_VeryDeepConflict(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      {
        field {
          deepField {
            x: a
          }
        },
        field {
          deepField {
            x: b
          }
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(
			`Fields "field" conflict because subfields "deepField" conflict because subfields "x" conflict because `+
				`a and b are different fields.`,
			3, 9,
			4, 11,
			5, 13,
			8, 9,
			9, 11,
			10, 13),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_ReportsDeepConflictToNearestCommonAncestor(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.OverlappingFieldsCanBeMergedRule, `
      {
        field {
          deepField {
            x: a
          }
          deepField {
            x: b
          }
        },
        field {
          deepField {
            y
          }
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(
			`Fields "deepField" conflict because subfields "x" conflict because `+
				`a and b are different fields.`,
			4, 11,
			5, 13,
			7, 11,
			8, 13),
	})
}

var someBoxInterface *graphql.Interface
var stringBoxObject *graphql.Object
var intBoxObject *graphql.Object
var schema graphql.Schema

func init() {
	someBoxInterface = graphql.NewInterface(graphql.InterfaceConfig{
		Name: "SomeBox",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return stringBoxObject
		},
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"deepBox": &graphql.Field{
					Type: someBoxInterface,
				},
				"unrelatedField": &graphql.Field{
					Type: graphql.String,
				},
			}
		}),
	})
	stringBoxObject = graphql.NewObject(graphql.ObjectConfig{
		Name: "StringBox",
		Interfaces: (graphql.InterfacesThunk)(func() []*graphql.Interface {
			return []*graphql.Interface{someBoxInterface}
		}),
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"scalar": &graphql.Field{
					Type: graphql.String,
				},
				"deepBox": &graphql.Field{
					Type: stringBoxObject,
				},
				"unrelatedField": &graphql.Field{
					Type: graphql.String,
				},
				"listStringBox": &graphql.Field{
					Type: graphql.NewList(stringBoxObject),
				},
				"stringBox": &graphql.Field{
					Type: stringBoxObject,
				},
				"intBox": &graphql.Field{
					Type: intBoxObject,
				},
			}
		}),
	})
	intBoxObject = graphql.NewObject(graphql.ObjectConfig{
		Name: "IntBox",
		Interfaces: (graphql.InterfacesThunk)(func() []*graphql.Interface {
			return []*graphql.Interface{someBoxInterface}
		}),
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"scalar": &graphql.Field{
					Type: graphql.Int,
				},
				"deepBox": &graphql.Field{
					Type: someBoxInterface,
				},
				"unrelatedField": &graphql.Field{
					Type: graphql.String,
				},
				"listStringBox": &graphql.Field{
					Type: graphql.NewList(stringBoxObject),
				},
				"stringBox": &graphql.Field{
					Type: stringBoxObject,
				},
				"intBox": &graphql.Field{
					Type: intBoxObject,
				},
			}
		}),
	})
	var nonNullStringBox1Interface = graphql.NewInterface(graphql.InterfaceConfig{
		Name: "NonNullStringBox1",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return stringBoxObject
		},
		Fields: graphql.Fields{
			"scalar": &graphql.Field{
				Type: graphql.NewNonNull(graphql.String),
			},
		},
	})
	NonNullStringBox1Impl := graphql.NewObject(graphql.ObjectConfig{
		Name: "NonNullStringBox1Impl",
		Interfaces: (graphql.InterfacesThunk)(func() []*graphql.Interface {
			return []*graphql.Interface{someBoxInterface, nonNullStringBox1Interface}
		}),
		Fields: graphql.Fields{
			"scalar": &graphql.Field{
				Type: graphql.NewNonNull(graphql.String),
			},
			"unrelatedField": &graphql.Field{
				Type: graphql.String,
			},
			"deepBox": &graphql.Field{
				Type: someBoxInterface,
			},
		},
	})
	var nonNullStringBox2Interface = graphql.NewInterface(graphql.InterfaceConfig{
		Name: "NonNullStringBox2",
		ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
			return stringBoxObject
		},
		Fields: graphql.Fields{
			"scalar": &graphql.Field{
				Type: graphql.NewNonNull(graphql.String),
			},
		},
	})
	NonNullStringBox2Impl := graphql.NewObject(graphql.ObjectConfig{
		Name: "NonNullStringBox2Impl",
		Interfaces: (graphql.InterfacesThunk)(func() []*graphql.Interface {
			return []*graphql.Interface{someBoxInterface, nonNullStringBox2Interface}
		}),
		Fields: graphql.Fields{
			"scalar": &graphql.Field{
				Type: graphql.NewNonNull(graphql.String),
			},
			"unrelatedField": &graphql.Field{
				Type: graphql.String,
			},
			"deepBox": &graphql.Field{
				Type: someBoxInterface,
			},
		},
	})

	var connectionObject = graphql.NewObject(graphql.ObjectConfig{
		Name: "Connection",
		Fields: graphql.Fields{
			"edges": &graphql.Field{
				Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
					Name: "Edge",
					Fields: graphql.Fields{
						"node": &graphql.Field{
							Type: graphql.NewObject(graphql.ObjectConfig{
								Name: "Node",
								Fields: graphql.Fields{
									"id": &graphql.Field{
										Type: graphql.ID,
									},
									"name": &graphql.Field{
										Type: graphql.String,
									},
								},
							}),
						},
					},
				})),
			},
		},
	})
	var err error
	schema, err = graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "QueryRoot",
			Fields: graphql.Fields{
				"someBox": &graphql.Field{
					Type: someBoxInterface,
				},
				"connection": &graphql.Field{
					Type: connectionObject,
				},
			},
		}),
		Types: []graphql.Type{
			intBoxObject,
			stringBoxObject,
			NonNullStringBox1Impl,
			NonNullStringBox2Impl,
		},
	})
	if err != nil {
		panic(err)
	}
}

func TestValidate_OverlappingFieldsCanBeMerged_ReturnTypesMustBeUnambiguous_ConflictingReturnTypesWhichPotentiallyOverlap(t *testing.T) {
	// This is invalid since an object could potentially be both the Object
	// type IntBox and the interface type NonNullStringBox1. While that
	// condition does not exist in the current schema, the schema could
	// expand in the future to allow this. Thus it is invalid.
	testutil.ExpectFailsRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
        {
          someBox {
            ...on IntBox {
              scalar
            }
            ...on NonNullStringBox1 {
              scalar
            }
          }
        }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(
			`Fields "scalar" conflict because they return conflicting types Int and String!.`,
			5, 15,
			8, 15),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_ReturnTypesMustBeUnambiguous_CompatibleReturnShapesOnDifferentReturnTypes(t *testing.T) {
	// In this case `deepBox` returns `SomeBox` in the first usage, and
	// `StringBox` in the second usage. These return types are not the same!
	// however this is valid because the return *shapes* are compatible.
	testutil.ExpectPassesRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
      {
        someBox {
          ... on SomeBox {
            deepBox {
              unrelatedField
            }
          }
          ... on StringBox {
            deepBox {
              unrelatedField
            }
          }
        }
      }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_ReturnTypesMustBeUnambiguous_DisallowsDifferingReturnTypesDespiteNoOverlap(t *testing.T) {
	testutil.ExpectFailsRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
        {
          someBox {
            ... on IntBox {
              scalar
            }
            ... on StringBox {
              scalar
            }
          }
        }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(
			`Fields "scalar" conflict because they return conflicting types Int and String.`,
			5, 15,
			8, 15),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_ReturnTypesMustBeUnambiguous_DisallowsDifferingReturnTypeNullabilityDespiteNoOverlap(t *testing.T) {
	testutil.ExpectFailsRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
        {
          someBox {
            ... on NonNullStringBox1 {
              scalar
            }
            ... on StringBox {
              scalar
            }
          }
        }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(
			`Fields "scalar" conflict because they return conflicting types String! and String.`,
			5, 15,
			8, 15),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_ReturnTypesMustBeUnambiguous_DisallowsDifferingReturnTypeListDespiteNoOverlap(t *testing.T) {
	testutil.ExpectFailsRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
        {
          someBox {
            ... on IntBox {
              box: listStringBox {
                scalar
              }
            }
            ... on StringBox {
              box: stringBox {
                scalar
              }
            }
          }
        }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(
			`Fields "box" conflict because they return conflicting types [StringBox] and StringBox.`,
			5, 15,
			10, 15),
	})

	testutil.ExpectFailsRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
        {
          someBox {
            ... on IntBox {
              box: stringBox {
                scalar
              }
            }
            ... on StringBox {
              box: listStringBox {
                scalar
              }
            }
          }
        }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(
			`Fields "box" conflict because they return conflicting types StringBox and [StringBox].`,
			5, 15,
			10, 15),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_ReturnTypesMustBeUnambiguous_DisallowsDifferingSubfields(t *testing.T) {
	testutil.ExpectFailsRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
        {
          someBox {
            ... on IntBox {
              box: stringBox {
                val: scalar
                val: unrelatedField
              }
            }
            ... on StringBox {
              box: stringBox {
                val: scalar
              }
            }
          }
        }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(
			`Fields "val" conflict because scalar and unrelatedField are different fields.`,
			6, 17,
			7, 17),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_ReturnTypesMustBeUnambiguous_DisallowsDifferingDeepReturnTypesDespiteNoOverlap(t *testing.T) {
	testutil.ExpectFailsRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
        {
          someBox {
            ... on IntBox {
              box: stringBox {
                scalar
              }
            }
            ... on StringBox {
              box: intBox {
                scalar
              }
            }
          }
        }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(
			`Fields "box" conflict because subfields "scalar" conflict because they return conflicting types String and Int.`,
			5, 15,
			6, 17,
			10, 15,
			11, 17),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_ReturnTypesMustBeUnambiguous_AllowsNonConflictingOverlappingTypes(t *testing.T) {
	testutil.ExpectPassesRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
        {
          someBox {
            ... on IntBox {
              scalar: unrelatedField
            }
            ... on StringBox {
              scalar
            }
          }
        }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_ReturnTypesMustBeUnambiguous_SameWrappedScalarReturnTypes(t *testing.T) {
	testutil.ExpectPassesRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
        {
          someBox {
            ...on NonNullStringBox1 {
              scalar
            }
            ...on NonNullStringBox2 {
              scalar
            }
          }
        }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_ReturnTypesMustBeUnambiguous_AllowsInlineTypelessFragments(t *testing.T) {
	testutil.ExpectPassesRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
        {
          a
          ... {
            a
          }
        }
    `)
}
func TestValidate_OverlappingFieldsCanBeMerged_ReturnTypesMustBeUnambiguous_ComparesDeepTypesIncludingList(t *testing.T) {
	testutil.ExpectFailsRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
        {
          connection {
            ...edgeID
            edges {
              node {
                id: name
              }
            }
          }
        }

        fragment edgeID on Connection {
          edges {
            node {
              id
            }
          }
        }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(
			`Fields "edges" conflict because subfields "node" conflict because subfields "id" conflict because `+
				`id and name are different fields.`,
			14, 11,
			15, 13,
			16, 15,
			5, 13,
			6, 15,
			7, 17),
	})
}
func TestValidate_OverlappingFieldsCanBeMerged_ReturnTypesMustBeUnambiguous_IgnoresUnknownTypes(t *testing.T) {
	testutil.ExpectPassesRuleWithSchema(t, &schema, graphql.OverlappingFieldsCanBeMergedRule, `
        {
          someBox {
            ...on UnknownType {
              scalar
            }
            ...on NonNullStringBox2 {
              scalar
            }
          }
        }
    `)
}
