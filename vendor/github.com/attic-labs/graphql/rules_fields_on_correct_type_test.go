package graphql_test

import (
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/testutil"
)

func TestValidate_FieldsOnCorrectType_ObjectFieldSelection(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment objectFieldSelection on Dog {
        __typename
        name
      }
    `)
}
func TestValidate_FieldsOnCorrectType_AliasedObjectFieldSelection(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment aliasedObjectFieldSelection on Dog {
        tn : __typename
        otherName : name
      }
    `)
}
func TestValidate_FieldsOnCorrectType_InterfaceFieldSelection(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment interfaceFieldSelection on Pet {
        __typename
        name
      }
    `)
}
func TestValidate_FieldsOnCorrectType_AliasedInterfaceFieldSelection(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment interfaceFieldSelection on Pet {
        otherName : name
      }
    `)
}
func TestValidate_FieldsOnCorrectType_LyingAliasSelection(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment lyingAliasSelection on Dog {
        name : nickname
      }
    `)
}
func TestValidate_FieldsOnCorrectType_IgnoresFieldsOnUnknownType(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment unknownSelection on UnknownType {
        unknownField
      }
    `)
}
func TestValidate_FieldsOnCorrectType_ReportErrorsWhenTheTypeIsKnownAgain(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment typeKnownAgain on Pet {
        unknown_pet_field {
          ... on Cat {
            unknown_cat_field
          }
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot query field "unknown_pet_field" on type "Pet".`, 3, 9),
		testutil.RuleError(`Cannot query field "unknown_cat_field" on type "Cat".`, 5, 13),
	})
}
func TestValidate_FieldsOnCorrectType_FieldNotDefinedOnFragment(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment fieldNotDefined on Dog {
        meowVolume
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot query field "meowVolume" on type "Dog".`, 3, 9),
	})
}
func TestValidate_FieldsOnCorrectType_IgnoreDeeplyUnknownField(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment deepFieldNotDefined on Dog {
        unknown_field {
          deeper_unknown_field
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot query field "unknown_field" on type "Dog".`, 3, 9),
	})
}
func TestValidate_FieldsOnCorrectType_SubFieldNotDefined(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment subFieldNotDefined on Human {
        pets {
          unknown_field
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot query field "unknown_field" on type "Pet".`, 4, 11),
	})
}
func TestValidate_FieldsOnCorrectType_FieldNotDefinedOnInlineFragment(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment fieldNotDefined on Pet {
        ... on Dog {
          meowVolume
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot query field "meowVolume" on type "Dog".`, 4, 11),
	})
}
func TestValidate_FieldsOnCorrectType_AliasedFieldTargetNotDefined(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment aliasedFieldTargetNotDefined on Dog {
        volume : mooVolume
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot query field "mooVolume" on type "Dog".`, 3, 9),
	})
}
func TestValidate_FieldsOnCorrectType_AliasedLyingFieldTargetNotDefined(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment aliasedLyingFieldTargetNotDefined on Dog {
        barkVolume : kawVolume
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot query field "kawVolume" on type "Dog".`, 3, 9),
	})
}
func TestValidate_FieldsOnCorrectType_NotDefinedOnInterface(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment notDefinedOnInterface on Pet {
        tailLength
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot query field "tailLength" on type "Pet".`, 3, 9),
	})
}
func TestValidate_FieldsOnCorrectType_DefinedOnImplementorsButNotOnInterface(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment definedOnImplementorsButNotInterface on Pet {
        nickname
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot query field "nickname" on type "Pet". However, this field exists on "Cat", "Dog". Perhaps you meant to use an inline fragment?`, 3, 9),
	})
}
func TestValidate_FieldsOnCorrectType_MetaFieldSelectionOnUnion(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment directFieldSelectionOnUnion on CatOrDog {
        __typename
      }
    `)
}
func TestValidate_FieldsOnCorrectType_DirectFieldSelectionOnUnion(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment directFieldSelectionOnUnion on CatOrDog {
        directField
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot query field "directField" on type "CatOrDog".`, 3, 9),
	})
}
func TestValidate_FieldsOnCorrectType_DefinedImplementorsQueriedOnUnion(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment definedOnImplementorsQueriedOnUnion on CatOrDog {
        name
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot query field "name" on type "CatOrDog". However, this field exists on "Being", "Pet", "Canine", "Cat", "Dog". Perhaps you meant to use an inline fragment?`, 3, 9),
	})
}
func TestValidate_FieldsOnCorrectType_ValidFieldInInlineFragment(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FieldsOnCorrectTypeRule, `
      fragment objectFieldSelection on Pet {
        ... on Dog {
          name
        }
        ... {
          name
        }
      }
    `)
}

func TestValidate_FieldsOnCorrectTypeErrorMessage_WorksWithNoSuggestions(t *testing.T) {
	message := graphql.UndefinedFieldMessage("T", "f", []string{})
	expected := `Cannot query field "T" on type "f".`
	if message != expected {
		t.Fatalf("Unexpected message, expected: %v, got %v", expected, message)
	}
}

func TestValidate_FieldsOnCorrectTypeErrorMessage_WorksWithNoSmallNumbersOfSuggestions(t *testing.T) {
	message := graphql.UndefinedFieldMessage("T", "f", []string{"A", "B"})
	expected := `Cannot query field "T" on type "f". ` +
		`However, this field exists on "A", "B". ` +
		`Perhaps you meant to use an inline fragment?`
	if message != expected {
		t.Fatalf("Unexpected message, expected: %v, got %v", expected, message)
	}
}
func TestValidate_FieldsOnCorrectTypeErrorMessage_WorksWithLotsOfSuggestions(t *testing.T) {
	message := graphql.UndefinedFieldMessage("T", "f", []string{"A", "B", "C", "D", "E", "F"})
	expected := `Cannot query field "T" on type "f". ` +
		`However, this field exists on "A", "B", "C", "D", "E", and 1 other types. ` +
		`Perhaps you meant to use an inline fragment?`
	if message != expected {
		t.Fatalf("Unexpected message, expected: %v, got %v", expected, message)
	}
}
