package graphql_test

import (
	"testing"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/testutil"
)

func TestValidate_KnownArgumentNames_SingleArgIsKnown(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.KnownArgumentNamesRule, `
      fragment argOnRequiredArg on Dog {
        doesKnowCommand(dogCommand: SIT)
      }
    `)
}
func TestValidate_KnownArgumentNames_MultipleArgsAreKnown(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.KnownArgumentNamesRule, `
      fragment multipleArgs on ComplicatedArgs {
        multipleReqs(req1: 1, req2: 2)
      }
    `)
}
func TestValidate_KnownArgumentNames_IgnoresArgsOfUnknownFields(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.KnownArgumentNamesRule, `
      fragment argOnUnknownField on Dog {
        unknownField(unknownArg: SIT)
      }
    `)
}
func TestValidate_KnownArgumentNames_MultipleArgsInReverseOrderAreKnown(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.KnownArgumentNamesRule, `
      fragment multipleArgsReverseOrder on ComplicatedArgs {
        multipleReqs(req2: 2, req1: 1)
      }
    `)
}
func TestValidate_KnownArgumentNames_NoArgsOnOptionalArg(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.KnownArgumentNamesRule, `
      fragment noArgOnOptionalArg on Dog {
        isHousetrained
      }
    `)
}
func TestValidate_KnownArgumentNames_ArgsAreKnownDeeply(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.KnownArgumentNamesRule, `
      {
        dog {
          doesKnowCommand(dogCommand: SIT)
        }
        human {
          pet {
            ... on Dog {
              doesKnowCommand(dogCommand: SIT)
            }
          }
        }
      }
    `)
}
func TestValidate_KnownArgumentNames_DirectiveArgsAreKnown(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.KnownArgumentNamesRule, `
      {
        dog @skip(if: true)
      }
    `)
}
func TestValidate_KnownArgumentNames_UndirectiveArgsAreInvalid(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.KnownArgumentNamesRule, `
      {
        dog @skip(unless: true)
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Unknown argument "unless" on directive "@skip".`, 3, 19),
	})
}
func TestValidate_KnownArgumentNames_InvalidArgName(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.KnownArgumentNamesRule, `
      fragment invalidArgName on Dog {
        doesKnowCommand(unknown: true)
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Unknown argument "unknown" on field "doesKnowCommand" of type "Dog".`, 3, 25),
	})
}
func TestValidate_KnownArgumentNames_UnknownArgsAmongstKnownArgs(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.KnownArgumentNamesRule, `
      fragment oneGoodArgOneInvalidArg on Dog {
        doesKnowCommand(whoknows: 1, dogCommand: SIT, unknown: true)
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Unknown argument "whoknows" on field "doesKnowCommand" of type "Dog".`, 3, 25),
		testutil.RuleError(`Unknown argument "unknown" on field "doesKnowCommand" of type "Dog".`, 3, 55),
	})
}
func TestValidate_KnownArgumentNames_UnknownArgsDeeply(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.KnownArgumentNamesRule, `
      {
        dog {
          doesKnowCommand(unknown: true)
        }
        human {
          pet {
            ... on Dog {
              doesKnowCommand(unknown: true)
            }
          }
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Unknown argument "unknown" on field "doesKnowCommand" of type "Dog".`, 4, 27),
		testutil.RuleError(`Unknown argument "unknown" on field "doesKnowCommand" of type "Dog".`, 9, 31),
	})
}
