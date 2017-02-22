package graphql_test

import (
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/testutil"
)

func TestValidate_UniqueInputFieldNames_InputObjectWithFields(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.UniqueInputFieldNamesRule, `
      {
        field(arg: { f: true })
      }
    `)
}
func TestValidate_UniqueInputFieldNames_SameInputObjectWithinTwoArgs(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.UniqueInputFieldNamesRule, `
      {
        field(arg1: { f: true }, arg2: { f: true })
      }
    `)
}
func TestValidate_UniqueInputFieldNames_MultipleInputObjectFields(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.UniqueInputFieldNamesRule, `
      {
        field(arg: { f1: "value", f2: "value", f3: "value" })
      }
    `)
}
func TestValidate_UniqueInputFieldNames_AllowsForNestedInputObjectsWithSimilarFields(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.UniqueInputFieldNamesRule, `
      {
        field(arg: {
          deep: {
            deep: {
              id: 1
            }
            id: 1
          }
          id: 1
        })
      }
    `)
}
func TestValidate_UniqueInputFieldNames_DuplicateInputObjectFields(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.UniqueInputFieldNamesRule, `
      {
        field(arg: { f1: "value", f1: "value" })
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`There can be only one input field named "f1".`, 3, 22, 3, 35),
	})
}
func TestValidate_UniqueInputFieldNames_ManyDuplicateInputObjectFields(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.UniqueInputFieldNamesRule, `
      {
        field(arg: { f1: "value", f1: "value", f1: "value" })
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`There can be only one input field named "f1".`, 3, 22, 3, 35),
		testutil.RuleError(`There can be only one input field named "f1".`, 3, 22, 3, 48),
	})
}
