package graphql_test

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/testutil"
)

func TestValidate_ProvidedNonNullArguments_IgnoresUnknownArguments(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
      {
        dog {
          isHousetrained(unknownArgument: true)
        }
      }
    `)
}

func TestValidate_ProvidedNonNullArguments_ValidNonNullableValue_ArgOnOptionalArg(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          dog {
            isHousetrained(atOtherHomes: true)
          }
        }
    `)
}
func TestValidate_ProvidedNonNullArguments_ValidNonNullableValue_NoArgOnOptionalArg(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          dog {
            isHousetrained
          }
        }
    `)
}
func TestValidate_ProvidedNonNullArguments_ValidNonNullableValue_MultipleArgs(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          complicatedArgs {
            multipleReqs(req1: 1, req2: 2)
          }
        }
    `)
}
func TestValidate_ProvidedNonNullArguments_ValidNonNullableValue_MultipleArgsReverseOrder(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          complicatedArgs {
            multipleReqs(req2: 2, req1: 1)
          }
        }
    `)
}
func TestValidate_ProvidedNonNullArguments_ValidNonNullableValue_NoArgsOnMultipleOptional(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          complicatedArgs {
            multipleOpts
          }
        }
    `)
}
func TestValidate_ProvidedNonNullArguments_ValidNonNullableValue_OneArgOnMultipleOptional(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          complicatedArgs {
            multipleOpts(opt1: 1)
          }
        }
    `)
}
func TestValidate_ProvidedNonNullArguments_ValidNonNullableValue_SecondArgOnMultipleOptional(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          complicatedArgs {
            multipleOpts(opt2: 1)
          }
        }
    `)
}
func TestValidate_ProvidedNonNullArguments_ValidNonNullableValue_MultipleReqsOnMixedList(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          complicatedArgs {
            multipleOptAndReq(req1: 3, req2: 4)
          }
        }
    `)
}
func TestValidate_ProvidedNonNullArguments_ValidNonNullableValue_MultipleReqsAndOneOptOnMixedList(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          complicatedArgs {
            multipleOptAndReq(req1: 3, req2: 4, opt1: 5)
          }
        }
    `)
}
func TestValidate_ProvidedNonNullArguments_ValidNonNullableValue_AllReqsAndOptsOnMixedList(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          complicatedArgs {
            multipleOptAndReq(req1: 3, req2: 4, opt1: 5, opt2: 6)
          }
        }
    `)
}

func TestValidate_ProvidedNonNullArguments_InvalidNonNullableValue_MissingOneNonNullableArgument(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          complicatedArgs {
            multipleReqs(req2: 2)
          }
        }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Field "multipleReqs" argument "req1" of type "Int!" is required but not provided.`, 4, 13),
	})
}
func TestValidate_ProvidedNonNullArguments_InvalidNonNullableValue_MissingMultipleNonNullableArguments(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          complicatedArgs {
            multipleReqs
          }
        }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Field "multipleReqs" argument "req1" of type "Int!" is required but not provided.`, 4, 13),
		testutil.RuleError(`Field "multipleReqs" argument "req2" of type "Int!" is required but not provided.`, 4, 13),
	})
}
func TestValidate_ProvidedNonNullArguments_InvalidNonNullableValue_IncorrectValueAndMissingArgument(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          complicatedArgs {
            multipleReqs(req1: "one")
          }
        }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Field "multipleReqs" argument "req2" of type "Int!" is required but not provided.`, 4, 13),
	})
}

func TestValidate_ProvidedNonNullArguments_DirectiveArguments_IgnoresUnknownDirectives(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          dog @unknown
        }
    `)
}
func TestValidate_ProvidedNonNullArguments_DirectiveArguments_WithDirectivesOfValidTypes(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          dog @include(if: true) {
            name
          }
          human @skip(if: false) {
            name
          }
        }
    `)
}
func TestValidate_ProvidedNonNullArguments_DirectiveArguments_WithDirectiveWithMissingTypes(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.ProvidedNonNullArgumentsRule, `
        {
          dog @include {
            name @skip
          }
        }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Directive "@include" argument "if" of type "Boolean!" is required but not provided.`, 3, 15),
		testutil.RuleError(`Directive "@skip" argument "if" of type "Boolean!" is required but not provided.`, 4, 18),
	})
}
