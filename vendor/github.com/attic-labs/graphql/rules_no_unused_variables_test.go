package graphql_test

import (
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/testutil"
)

func TestValidate_NoUnusedVariables_UsesAllVariables(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoUnusedVariablesRule, `
      query ($a: String, $b: String, $c: String) {
        field(a: $a, b: $b, c: $c)
      }
    `)
}
func TestValidate_NoUnusedVariables_UsesAllVariablesDeeply(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoUnusedVariablesRule, `
      query Foo($a: String, $b: String, $c: String) {
        field(a: $a) {
          field(b: $b) {
            field(c: $c)
          }
        }
      }
    `)
}
func TestValidate_NoUnusedVariables_UsesAllVariablesDeeplyInInlineFragments(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoUnusedVariablesRule, `
      query Foo($a: String, $b: String, $c: String) {
        ... on Type {
          field(a: $a) {
            field(b: $b) {
              ... on Type {
                field(c: $c)
              }
            }
          }
        }
      }
    `)
}
func TestValidate_NoUnusedVariables_UsesAllVariablesInFragments(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoUnusedVariablesRule, `
      query Foo($a: String, $b: String, $c: String) {
        ...FragA
      }
      fragment FragA on Type {
        field(a: $a) {
          ...FragB
        }
      }
      fragment FragB on Type {
        field(b: $b) {
          ...FragC
        }
      }
      fragment FragC on Type {
        field(c: $c)
      }
    `)
}
func TestValidate_NoUnusedVariables_VariableUsedByFragmentInMultipleOperations(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoUnusedVariablesRule, `
      query Foo($a: String) {
        ...FragA
      }
      query Bar($b: String) {
        ...FragB
      }
      fragment FragA on Type {
        field(a: $a)
      }
      fragment FragB on Type {
        field(b: $b)
      }
    `)
}
func TestValidate_NoUnusedVariables_VariableUsedByRecursiveFragment(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoUnusedVariablesRule, `
      query Foo($a: String) {
        ...FragA
      }
      fragment FragA on Type {
        field(a: $a) {
          ...FragA
        }
      }
    `)
}
func TestValidate_NoUnusedVariables_VariableNotUsed(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoUnusedVariablesRule, `
      query ($a: String, $b: String, $c: String) {
        field(a: $a, b: $b)
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Variable "$c" is never used.`, 2, 38),
	})
}
func TestValidate_NoUnusedVariables_MultipleVariablesNotUsed(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoUnusedVariablesRule, `
      query Foo($a: String, $b: String, $c: String) {
        field(b: $b)
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Variable "$a" is never used in operation "Foo".`, 2, 17),
		testutil.RuleError(`Variable "$c" is never used in operation "Foo".`, 2, 41),
	})
}
func TestValidate_NoUnusedVariables_VariableNotUsedInFragments(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoUnusedVariablesRule, `
      query Foo($a: String, $b: String, $c: String) {
        ...FragA
      }
      fragment FragA on Type {
        field(a: $a) {
          ...FragB
        }
      }
      fragment FragB on Type {
        field(b: $b) {
          ...FragC
        }
      }
      fragment FragC on Type {
        field
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Variable "$c" is never used in operation "Foo".`, 2, 41),
	})
}
func TestValidate_NoUnusedVariables_MultipleVariablesNotUsed2(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoUnusedVariablesRule, `
      query Foo($a: String, $b: String, $c: String) {
        ...FragA
      }
      fragment FragA on Type {
        field {
          ...FragB
        }
      }
      fragment FragB on Type {
        field(b: $b) {
          ...FragC
        }
      }
      fragment FragC on Type {
        field
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Variable "$a" is never used in operation "Foo".`, 2, 17),
		testutil.RuleError(`Variable "$c" is never used in operation "Foo".`, 2, 41),
	})
}
func TestValidate_NoUnusedVariables_VariableNotUsedByUnreferencedFragment(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoUnusedVariablesRule, `
      query Foo($b: String) {
        ...FragA
      }
      fragment FragA on Type {
        field(a: $a)
      }
      fragment FragB on Type {
        field(b: $b)
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Variable "$b" is never used in operation "Foo".`, 2, 17),
	})
}
func TestValidate_NoUnusedVariables_VariableNotUsedByFragmentUsedByOtherOperation(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoUnusedVariablesRule, `
      query Foo($b: String) {
        ...FragA
      }
      query Bar($a: String) {
        ...FragB
      }
      fragment FragA on Type {
        field(a: $a)
      }
      fragment FragB on Type {
        field(b: $b)
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Variable "$b" is never used in operation "Foo".`, 2, 17),
		testutil.RuleError(`Variable "$a" is never used in operation "Bar".`, 5, 17),
	})
}
