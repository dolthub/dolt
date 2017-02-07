package graphql_test

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/testutil"
)

func TestValidate_KnownFragmentNames_KnownFragmentNamesAreValid(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.KnownFragmentNamesRule, `
      {
        human(id: 4) {
          ...HumanFields1
          ... on Human {
            ...HumanFields2
          }
          ... {
            name
          }
        }
      }
      fragment HumanFields1 on Human {
        name
        ...HumanFields3
      }
      fragment HumanFields2 on Human {
        name
      }
      fragment HumanFields3 on Human {
        name
      }
    `)
}
func TestValidate_KnownFragmentNames_UnknownFragmentNamesAreInvalid(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.KnownFragmentNamesRule, `
      {
        human(id: 4) {
          ...UnknownFragment1
          ... on Human {
            ...UnknownFragment2
          }
        }
      }
      fragment HumanFields on Human {
        name
        ...UnknownFragment3
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Unknown fragment "UnknownFragment1".`, 4, 14),
		testutil.RuleError(`Unknown fragment "UnknownFragment2".`, 6, 16),
		testutil.RuleError(`Unknown fragment "UnknownFragment3".`, 12, 12),
	})
}
