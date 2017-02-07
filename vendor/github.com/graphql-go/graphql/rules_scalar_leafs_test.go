package graphql_test

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/testutil"
)

func TestValidate_ScalarLeafs_ValidScalarSelection(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ScalarLeafsRule, `
      fragment scalarSelection on Dog {
        barks
      }
    `)
}
func TestValidate_ScalarLeafs_ObjectTypeMissingSelection(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.ScalarLeafsRule, `
      query directQueryOnObjectWithoutSubFields {
        human
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Field "human" of type "Human" must have a sub selection.`, 3, 9),
	})
}
func TestValidate_ScalarLeafs_InterfaceTypeMissingSelection(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.ScalarLeafsRule, `
      {
        human { pets }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Field "pets" of type "[Pet]" must have a sub selection.`, 3, 17),
	})
}
func TestValidate_ScalarLeafs_ValidScalarSelectionWithArgs(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.ScalarLeafsRule, `
      fragment scalarSelectionWithArgs on Dog {
        doesKnowCommand(dogCommand: SIT)
      }
    `)
}

func TestValidate_ScalarLeafs_ScalarSelectionNotAllowedOnBoolean(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.ScalarLeafsRule, `
      fragment scalarSelectionsNotAllowedOnBoolean on Dog {
        barks { sinceWhen }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Field "barks" of type "Boolean" must not have a sub selection.`, 3, 15),
	})
}
func TestValidate_ScalarLeafs_ScalarSelectionNotAllowedOnEnum(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.ScalarLeafsRule, `
      fragment scalarSelectionsNotAllowedOnEnum on Cat {
        furColor { inHexdec }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Field "furColor" of type "FurColor" must not have a sub selection.`, 3, 18),
	})
}
func TestValidate_ScalarLeafs_ScalarSelectionNotAllowedWithArgs(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.ScalarLeafsRule, `
      fragment scalarSelectionsNotAllowedWithArgs on Dog {
        doesKnowCommand(dogCommand: SIT) { sinceWhen }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Field "doesKnowCommand" of type "Boolean" must not have a sub selection.`, 3, 42),
	})
}
func TestValidate_ScalarLeafs_ScalarSelectionNotAllowedWithDirectives(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.ScalarLeafsRule, `
      fragment scalarSelectionsNotAllowedWithDirectives on Dog {
        name @include(if: true) { isAlsoHumanName }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Field "name" of type "String" must not have a sub selection.`, 3, 33),
	})
}
func TestValidate_ScalarLeafs_ScalarSelectionNotAllowedWithDirectivesAndArgs(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.ScalarLeafsRule, `
      fragment scalarSelectionsNotAllowedWithDirectivesAndArgs on Dog {
        doesKnowCommand(dogCommand: SIT) @include(if: true) { sinceWhen }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Field "doesKnowCommand" of type "Boolean" must not have a sub selection.`, 3, 61),
	})
}
