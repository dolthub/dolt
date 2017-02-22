package graphql_test

import (
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/testutil"
)

func TestValidate_VariableDefaultValuesOfCorrectType_VariablesWithNoDefaultValues(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.DefaultValuesOfCorrectTypeRule, `
      query NullableValues($a: Int, $b: String, $c: ComplexInput) {
        dog { name }
      }
    `)
}
func TestValidate_VariableDefaultValuesOfCorrectType_RequiredVariablesWithoutDefaultValues(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.DefaultValuesOfCorrectTypeRule, `
      query RequiredValues($a: Int!, $b: String!) {
        dog { name }
      }
    `)
}
func TestValidate_VariableDefaultValuesOfCorrectType_VariablesWithValidDefaultValues(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.DefaultValuesOfCorrectTypeRule, `
      query WithDefaultValues(
        $a: Int = 1,
        $b: String = "ok",
        $c: ComplexInput = { requiredField: true, intField: 3 }
      ) {
        dog { name }
      }
    `)
}
func TestValidate_VariableDefaultValuesOfCorrectType_NoRequiredVariablesWithDefaultValues(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.DefaultValuesOfCorrectTypeRule, `
      query UnreachableDefaultValues($a: Int! = 3, $b: String! = "default") {
        dog { name }
      }
    `,
		[]gqlerrors.FormattedError{
			testutil.RuleError(
				`Variable "$a" of type "Int!" is required and will not `+
					`use the default value. Perhaps you meant to use type "Int".`,
				2, 49,
			),
			testutil.RuleError(
				`Variable "$b" of type "String!" is required and will not `+
					`use the default value. Perhaps you meant to use type "String".`,
				2, 66,
			),
		})
}
func TestValidate_VariableDefaultValuesOfCorrectType_VariablesWithInvalidDefaultValues(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.DefaultValuesOfCorrectTypeRule, `
      query InvalidDefaultValues(
        $a: Int = "one",
        $b: String = 4,
        $c: ComplexInput = "notverycomplex"
      ) {
        dog { name }
      }
    `,
		[]gqlerrors.FormattedError{
			testutil.RuleError(`Variable "$a" has invalid default value: "one".`+
				"\nExpected type \"Int\", found \"one\".",
				3, 19),
			testutil.RuleError(`Variable "$b" has invalid default value: 4.`+
				"\nExpected type \"String\", found 4.",
				4, 22),
			testutil.RuleError(
				`Variable "$c" has invalid default value: "notverycomplex".`+
					"\nExpected \"ComplexInput\", found not an object.",
				5, 28),
		})
}
func TestValidate_VariableDefaultValuesOfCorrectType_ComplexVariablesMissingRequiredField(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.DefaultValuesOfCorrectTypeRule, `
      query MissingRequiredField($a: ComplexInput = {intField: 3}) {
        dog { name }
      }
    `,
		[]gqlerrors.FormattedError{
			testutil.RuleError(
				`Variable "$a" has invalid default value: {intField: 3}.`+
					"\nIn field \"requiredField\": Expected \"Boolean!\", found null.",
				2, 53),
		})
}
func TestValidate_VariableDefaultValuesOfCorrectType_ListVariablesWithInvalidItem(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.DefaultValuesOfCorrectTypeRule, `
      query InvalidItem($a: [String] = ["one", 2]) {
        dog { name }
      }
    `,
		[]gqlerrors.FormattedError{
			testutil.RuleError(
				`Variable "$a" has invalid default value: ["one", 2].`+
					"\nIn element #1: Expected type \"String\", found 2.",
				2, 40),
		})
}
