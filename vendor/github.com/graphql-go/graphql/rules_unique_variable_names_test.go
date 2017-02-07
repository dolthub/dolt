package graphql_test

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/testutil"
)

func TestValidate_UniqueVariableNames_UniqueVariableNames(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.UniqueVariableNamesRule, `
      query A($x: Int, $y: String) { __typename }
      query B($x: String, $y: Int) { __typename }
    `)
}
func TestValidate_UniqueVariableNames_DuplicateVariableNames(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.UniqueVariableNamesRule, `
      query A($x: Int, $x: Int, $x: String) { __typename }
      query B($x: String, $x: Int) { __typename }
      query C($x: Int, $x: Int) { __typename }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`There can only be one variable named "x".`, 2, 16, 2, 25),
		testutil.RuleError(`There can only be one variable named "x".`, 2, 16, 2, 34),
		testutil.RuleError(`There can only be one variable named "x".`, 3, 16, 3, 28),
		testutil.RuleError(`There can only be one variable named "x".`, 4, 16, 4, 25),
	})
}
