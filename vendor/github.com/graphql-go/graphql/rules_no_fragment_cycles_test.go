package graphql_test

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/testutil"
)

func TestValidate_NoCircularFragmentSpreads_SingleReferenceIsValid(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragA on Dog { ...fragB }
      fragment fragB on Dog { name }
    `)
}
func TestValidate_NoCircularFragmentSpreads_SpreadingTwiceIsNotCircular(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragA on Dog { ...fragB, ...fragB }
      fragment fragB on Dog { name }
    `)
}
func TestValidate_NoCircularFragmentSpreads_SpreadingTwiceIndirectlyIsNotCircular(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragA on Dog { ...fragB, ...fragC }
      fragment fragB on Dog { ...fragC }
      fragment fragC on Dog { name }
    `)
}
func TestValidate_NoCircularFragmentSpreads_DoubleSpreadWithinAbstractTypes(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoFragmentCyclesRule, `
      fragment nameFragment on Pet {
        ... on Dog { name }
        ... on Cat { name }
      }

      fragment spreadsInAnon on Pet {
        ... on Dog { ...nameFragment }
        ... on Cat { ...nameFragment }
      }
    `)
}
func TestValidate_NoCircularFragmentSpreads_DoesNotFalsePositiveOnUnknownFragment(t *testing.T) {
	testutil.ExpectPassesRule(t, graphql.NoFragmentCyclesRule, `
      fragment nameFragment on Pet {
        ...UnknownFragment
      }
    `)
}
func TestValidate_NoCircularFragmentSpreads_SpreadingRecursivelyWithinFieldFails(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragA on Human { relatives { ...fragA } },
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot spread fragment "fragA" within itself.`, 2, 45),
	})
}

func TestValidate_NoCircularFragmentSpreads_NoSpreadingItselfDirectly(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragA on Dog { ...fragA }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot spread fragment "fragA" within itself.`, 2, 31),
	})
}
func TestValidate_NoCircularFragmentSpreads_NoSpreadingItselfDirectlyWithinInlineFragment(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragA on Pet {
        ... on Dog {
          ...fragA
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot spread fragment "fragA" within itself.`, 4, 11),
	})
}

func TestValidate_NoCircularFragmentSpreads_NoSpreadingItselfIndirectly(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragA on Dog { ...fragB }
      fragment fragB on Dog { ...fragA }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot spread fragment "fragA" within itself via fragB.`, 2, 31, 3, 31),
	})
}
func TestValidate_NoCircularFragmentSpreads_NoSpreadingItselfIndirectlyReportsOppositeOrder(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragB on Dog { ...fragA }
      fragment fragA on Dog { ...fragB }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot spread fragment "fragB" within itself via fragA.`, 2, 31, 3, 31),
	})
}
func TestValidate_NoCircularFragmentSpreads_NoSpreadingItselfIndirectlyWithinInlineFragment(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragA on Pet {
        ... on Dog {
          ...fragB
        }
      }
      fragment fragB on Pet {
        ... on Dog {
          ...fragA
        }
      }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot spread fragment "fragA" within itself via fragB.`, 4, 11, 9, 11),
	})
}

func TestValidate_NoCircularFragmentSpreads_NoSpreadingItselfDeeply(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragA on Dog { ...fragB }
      fragment fragB on Dog { ...fragC }
      fragment fragC on Dog { ...fragO }
      fragment fragX on Dog { ...fragY }
      fragment fragY on Dog { ...fragZ }
      fragment fragZ on Dog { ...fragO }
      fragment fragO on Dog { ...fragP }
      fragment fragP on Dog { ...fragA, ...fragX }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot spread fragment "fragA" within itself via fragB, fragC, fragO, fragP.`,
			2, 31,
			3, 31,
			4, 31,
			8, 31,
			9, 31),
		testutil.RuleError(`Cannot spread fragment "fragO" within itself via fragP, fragX, fragY, fragZ.`,
			8, 31,
			9, 41,
			5, 31,
			6, 31,
			7, 31),
	})
}
func TestValidate_NoCircularFragmentSpreads_NoSpreadingItselfDeeplyTwoPaths(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragA on Dog { ...fragB, ...fragC }
      fragment fragB on Dog { ...fragA }
      fragment fragC on Dog { ...fragA }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot spread fragment "fragA" within itself via fragB.`,
			2, 31,
			3, 31),
		testutil.RuleError(`Cannot spread fragment "fragA" within itself via fragC.`,
			2, 41,
			4, 31),
	})
}
func TestValidate_NoCircularFragmentSpreads_NoSpreadingItselfDeeplyTwoPaths_AltTraverseOrder(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragA on Dog { ...fragC }
      fragment fragB on Dog { ...fragC }
      fragment fragC on Dog { ...fragA, ...fragB }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot spread fragment "fragA" within itself via fragC.`,
			2, 31,
			4, 31),
		testutil.RuleError(`Cannot spread fragment "fragC" within itself via fragB.`,
			4, 41,
			3, 31),
	})
}
func TestValidate_NoCircularFragmentSpreads_NoSpreadingItselfDeeplyAndImmediately(t *testing.T) {
	testutil.ExpectFailsRule(t, graphql.NoFragmentCyclesRule, `
      fragment fragA on Dog { ...fragB }
      fragment fragB on Dog { ...fragB, ...fragC }
      fragment fragC on Dog { ...fragA, ...fragB }
    `, []gqlerrors.FormattedError{
		testutil.RuleError(`Cannot spread fragment "fragB" within itself.`, 3, 31),
		testutil.RuleError(`Cannot spread fragment "fragA" within itself via fragB, fragC.`,
			2, 31,
			3, 41,
			4, 31),
		testutil.RuleError(`Cannot spread fragment "fragB" within itself via fragC.`,
			3, 41,
			4, 41),
	})
}
