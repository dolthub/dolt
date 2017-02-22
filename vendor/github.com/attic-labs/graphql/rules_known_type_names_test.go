package graphql_test

import (
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/testutil"
)

func TestValidate_KnownTypeNames_KnownTypeNamesAreValid(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.KnownTypeNamesRule, `
      query Foo($var: String, $required: [String!]!) {
        user(id: 4) {
          pets { ... on Pet { name }, ...PetFields, ... { name } }
        }
      }
      fragment PetFields on Pet {
        name
      }
    `)
}
func TestValidate_KnownTypeNames_UnknownTypeNamesAreInValid(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.KnownTypeNamesRule, `
      query Foo($var: JumbledUpLetters) {
        user(id: 4) {
          name
          pets { ... on Badger { name }, ...PetFields }
        }
      }
      fragment PetFields on Peettt {
        name
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Unknown type "JumbledUpLetters".`, 2, 23),
		testutil.RuleError(`Unknown type "Badger".`, 5, 25),
		testutil.RuleError(`Unknown type "Peettt".`, 8, 29),
	})
}

func TestValidate_KnownTypeNames_IgnoresTypeDefinitions(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.KnownTypeNamesRule, `
      type NotInTheSchema {
        field: FooBar
      }
      interface FooBar {
        field: NotInTheSchema
      }
      union U = A | B
      input Blob {
        field: UnknownType
      }
      query Foo($var: NotInTheSchema) {
        user(id: $var) {
          id
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Unknown type "NotInTheSchema".`, 12, 23),
	})
}
