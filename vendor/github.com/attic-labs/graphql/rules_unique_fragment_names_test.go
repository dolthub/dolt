package graphql_test

import (
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/testutil"
)

func TestValidate_UniqueFragmentNames_NoFragments(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.UniqueFragmentNamesRule, `
      {
        field
      }
    `)
}
func TestValidate_UniqueFragmentNames_OneFragment(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.UniqueFragmentNamesRule, `
      {
        ...fragA
      }

      fragment fragA on Type {
        field
      }
    `)
}
func TestValidate_UniqueFragmentNames_ManyFragments(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.UniqueFragmentNamesRule, `
      {
        ...fragA
        ...fragB
        ...fragC
      }
      fragment fragA on Type {
        fieldA
      }
      fragment fragB on Type {
        fieldB
      }
      fragment fragC on Type {
        fieldC
      }
    `)
}
func TestValidate_UniqueFragmentNames_InlineFragmentsAreAlwaysUnique(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.UniqueFragmentNamesRule, `
      {
        ...on Type {
          fieldA
        }
        ...on Type {
          fieldB
        }
      }
    `)
}
func TestValidate_UniqueFragmentNames_FragmentAndOperationNamedTheSame(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.UniqueFragmentNamesRule, `
      query Foo {
        ...Foo
      }
      fragment Foo on Type {
        field
      }
    `)
}
func TestValidate_UniqueFragmentNames_FragmentsNamedTheSame(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.UniqueFragmentNamesRule, `
      {
        ...fragA
      }
      fragment fragA on Type {
        fieldA
      }
      fragment fragA on Type {
        fieldB
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`There can only be one fragment named "fragA".`, 5, 16, 8, 16),
	})
}
func TestValidate_UniqueFragmentNames_FragmentsNamedTheSameWithoutBeingReferenced(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.UniqueFragmentNamesRule, `
      fragment fragA on Type {
        fieldA
      }
      fragment fragA on Type {
        fieldB
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`There can only be one fragment named "fragA".`, 2, 16, 5, 16),
	})
}
