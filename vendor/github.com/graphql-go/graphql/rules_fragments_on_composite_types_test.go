package graphql_test

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/testutil"
)

func TestValidate_FragmentsOnCompositeTypes_ObjectIsValidFragmentType(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FragmentsOnCompositeTypesRule, `
      fragment validFragment on Dog {
        barks
      }
    `)
}
func TestValidate_FragmentsOnCompositeTypes_InterfaceIsValidFragmentType(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FragmentsOnCompositeTypesRule, `
      fragment validFragment on Pet {
        name
      }
    `)
}
func TestValidate_FragmentsOnCompositeTypes_ObjectIsValidInlineFragmentType(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FragmentsOnCompositeTypesRule, `
      fragment validFragment on Pet {
        ... on Dog {
          barks
        }
      }
    `)
}
func TestValidate_FragmentsOnCompositeTypes_InlineFragmentWithoutTypeIsValid(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FragmentsOnCompositeTypesRule, `
      fragment validFragment on Pet {
        ... {
          name
        }
      }
    `)
}
func TestValidate_FragmentsOnCompositeTypes_UnionIsValidFragmentType(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.FragmentsOnCompositeTypesRule, `
      fragment validFragment on CatOrDog {
        __typename
      }
    `)
}
func TestValidate_FragmentsOnCompositeTypes_ScalarIsInvalidFragmentType(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FragmentsOnCompositeTypesRule, `
      fragment scalarFragment on Boolean {
        bad
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fragment "scalarFragment" cannot condition on non composite type "Boolean".`, 2, 34),
	})
}
func TestValidate_FragmentsOnCompositeTypes_EnumIsInvalidFragmentType(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FragmentsOnCompositeTypesRule, `
      fragment scalarFragment on FurColor {
        bad
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fragment "scalarFragment" cannot condition on non composite type "FurColor".`, 2, 34),
	})
}
func TestValidate_FragmentsOnCompositeTypes_InputObjectIsInvalidFragmentType(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FragmentsOnCompositeTypesRule, `
      fragment inputFragment on ComplexInput {
        stringField
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fragment "inputFragment" cannot condition on non composite type "ComplexInput".`, 2, 33),
	})
}
func TestValidate_FragmentsOnCompositeTypes_ScalarIsInvalidInlineFragmentType(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.FragmentsOnCompositeTypesRule, `
      fragment invalidFragment on Pet {
        ... on String {
          barks
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fragment cannot condition on non composite type "String".`, 3, 16),
	})
}
