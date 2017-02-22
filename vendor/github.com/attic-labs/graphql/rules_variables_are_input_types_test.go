package graphql_test

import (
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/testutil"
)

func TestValidate_VariablesAreInputTypes_(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.VariablesAreInputTypesRule, `
      query Foo($a: String, $b: [Boolean!]!, $c: ComplexInput) {
        field(a: $a, b: $b, c: $c)
      }
    `)
}
func TestValidate_VariablesAreInputTypes_1(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.VariablesAreInputTypesRule, `
      query Foo($a: Dog, $b: [[CatOrDog!]]!, $c: Pet) {
        field(a: $a, b: $b, c: $c)
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Variable "$a" cannot be non-input type "Dog".`, 2, 21),
		testutil.RuleError(`Variable "$b" cannot be non-input type "[[CatOrDog!]]!".`, 2, 30),
		testutil.RuleError(`Variable "$c" cannot be non-input type "Pet".`, 2, 50),
	})
}
