package graphql_test

import (
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/testutil"
)

func TestValidate_NoUnusedFragments_AllFragmentNamesAreUsed(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoUnusedFragmentsRule, `
      {
        human(id: 4) {
          ...HumanFields1
          ... on Human {
            ...HumanFields2
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
func TestValidate_NoUnusedFragments_AllFragmentNamesAreUsedByMultipleOperations(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoUnusedFragmentsRule, `
      query Foo {
        human(id: 4) {
          ...HumanFields1
        }
      }
      query Bar {
        human(id: 4) {
          ...HumanFields2
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
func TestValidate_NoUnusedFragments_ContainsUnknownFragments(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoUnusedFragmentsRule, `
      query Foo {
        human(id: 4) {
          ...HumanFields1
        }
      }
      query Bar {
        human(id: 4) {
          ...HumanFields2
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
      fragment Unused1 on Human {
        name
      }
      fragment Unused2 on Human {
        name
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fragment "Unused1" is never used.`, 22, 7),
		testutil.RuleError(`Fragment "Unused2" is never used.`, 25, 7),
	})
}
func TestValidate_NoUnusedFragments_ContainsUnknownFragmentsWithRefCycle(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoUnusedFragmentsRule, `
      query Foo {
        human(id: 4) {
          ...HumanFields1
        }
      }
      query Bar {
        human(id: 4) {
          ...HumanFields2
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
      fragment Unused1 on Human {
        name
        ...Unused2
      }
      fragment Unused2 on Human {
        name
        ...Unused1
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fragment "Unused1" is never used.`, 22, 7),
		testutil.RuleError(`Fragment "Unused2" is never used.`, 26, 7),
	})
}
func TestValidate_NoUnusedFragments_ContainsUnknownAndUndefFragments(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoUnusedFragmentsRule, `
      query Foo {
        human(id: 4) {
          ...bar
        }
      }
      fragment foo on Human {
        name
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Fragment "foo" is never used.`, 7, 7),
	})
}
