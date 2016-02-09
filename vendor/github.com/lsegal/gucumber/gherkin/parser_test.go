package gherkin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSingleScenario(t *testing.T) {
	s := `
# a line comment
@tag1 @tag2
Feature: Refund item

  # a comment embedded in the description
  A multiline description of the Feature
  that can contain any text like
  Rules:
  - a
  - b

  @tag1 @tag2
  Scenario: Jeff returns a faulty microwave
    Given Jeff has bought a microwave for $100
    And he has a receipt
    When he returns the microwave
    Then Jeff should be refunded $100
`
	features, err := Parse(s)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(features))
	assert.Equal(t, "Refund item", features[0].Title)
	assert.Equal(t, "@tag1", features[0].Tags[0])
	assert.Equal(t, "@tag2", features[0].Tags[1])
	assert.Equal(t, 1, len(features[0].Scenarios))
	assert.Equal(t, "Jeff returns a faulty microwave", features[0].Scenarios[0].Title)
	assert.Equal(t, "A multiline description of the Feature\nthat can contain any text like\nRules:\n- a\n- b", features[0].Description)
	assert.Equal(t, 4, len(features[0].Scenarios[0].Steps))
	assert.Equal(t, "@tag1", features[0].Scenarios[0].Tags[0])
	assert.Equal(t, "@tag2", features[0].Scenarios[0].Tags[1])
	assert.Equal(t, StepType("Given"), features[0].Scenarios[0].Steps[0].Type)
	assert.Equal(t, "Jeff has bought a microwave for $100", features[0].Scenarios[0].Steps[0].Text)
	assert.Equal(t, StepType("And"), features[0].Scenarios[0].Steps[1].Type)
	assert.Equal(t, "he has a receipt", features[0].Scenarios[0].Steps[1].Text)
	assert.Equal(t, StepType("When"), features[0].Scenarios[0].Steps[2].Type)
	assert.Equal(t, "he returns the microwave", features[0].Scenarios[0].Steps[2].Text)
	assert.Equal(t, StepType("Then"), features[0].Scenarios[0].Steps[3].Type)
	assert.Equal(t, "Jeff should be refunded $100", features[0].Scenarios[0].Steps[3].Text)
}

func TestMultipleScenarios(t *testing.T) {
	s := `
Feature: Parsing multiple scenarios
  Scenario: Scenario name here
    Given some precondition
    Then something happens

  Scenario: Another scenario
    Given another precondition
    Then something happens
`
	features, err := Parse(s)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(features[0].Scenarios))
	assert.Equal(t, 2, len(features[0].Scenarios[0].Steps))
	assert.Equal(t, 2, len(features[0].Scenarios[1].Steps))
}

func TestBackgroundAndScenarios(t *testing.T) {
	s := `
Feature: Parsing multiple scenarios
  @tag_on_background
  Background:
    Given there is some background

  Scenario: Scenario name here
    Given some precondition
    Then something happens

  Scenario: Another scenario
    Given another precondition
    Then something happens
`
	features, err := Parse(s)
	assert.NoError(t, err)
	assert.Equal(t, "@tag_on_background", features[0].Background.Tags[0])
	assert.Equal(t, "there is some background", features[0].Background.Steps[0].Text)
	assert.Equal(t, 2, len(features[0].Scenarios))
}

func TestMultipleFeatures(t *testing.T) {
	s := `
Feature: Feature 1
  Scenario: Scenario name here
    Given some precondition
    Then something happens

Feature: Feature 2
  Scenario: Another scenario
    Given another precondition
    Then something happens
`
	f, err := Parse(s)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(f))
	assert.Equal(t, "Feature 1", f[0].Title)
	assert.Equal(t, "Feature 2", f[1].Title)
	assert.Equal(t, 1, len(f[0].Scenarios))
	assert.Equal(t, 1, len(f[1].Scenarios))

}

func TestTagParsing(t *testing.T) {
	f, err := Parse("@tag1   @tag2@tag3\nFeature: Tag parsing")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(f[0].Tags))
	assert.Equal(t, "@tag1", f[0].Tags[0])
	assert.Equal(t, "@tag2@tag3", f[0].Tags[1])
}

func TestBacktrackingCommentsAtEnd(t *testing.T) {
	s := `
Feature: Comments at end
  Scenario: Scenario name here
    Given some precondition
    Then something happens
  # comments here
`
	_, err := Parse(s)
	assert.NoError(t, err)
}

func TestBacktrackingCommentsDontAffectIndent(t *testing.T) {
	s := `
Feature: Comments at end
  Scenario: Scenario name here
    Given some precondition
    Then something happens
# comments here
  Scenario: Another scenario
    Given a step
`
	f, err := Parse(s)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(f[0].Scenarios))
}

func TestScenarioOutlines(t *testing.T) {
	s := `
Feature: Scenario outlines
  Scenario Outline: Scenario 1
    Given some value <foo>
    Then some result <bar>

    Examples:
    | foo | bar |
    | 1   | 2   |
    | 3   | 4   |

  Scenario: Scenario 2
    Given some other scenario
`
	f, err := Parse(s)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(f[0].Scenarios))
	assert.Equal(t, StringData("| foo | bar |\n| 1   | 2   |\n| 3   | 4   |"), f[0].Scenarios[0].Examples)
}

func TestStepArguments(t *testing.T) {
	s := `
Feature: Step arguments
  Scenario: Scenario 1
    Given some data
                          | 1   | 2   |
                          | 3   | 4   |
    And some docstring
    """
     hello
     world
    """
    And some table
    | 1 | 2 |
    Then other text

  Scenario: Scenario 2
    Given some other scenario
`
	f, err := Parse(s)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(f[0].Scenarios))
	assert.Equal(t, StringData("| 1   | 2   |\n| 3   | 4   |"), f[0].Scenarios[0].Steps[0].Argument)
	assert.Equal(t, StringData(" hello\n world"), f[0].Scenarios[0].Steps[1].Argument)
	assert.Equal(t, StringData("| 1 | 2 |"), f[0].Scenarios[0].Steps[2].Argument)
	assert.Equal(t, "other text", f[0].Scenarios[0].Steps[3].Text)
}

func TestFailureNoFeature(t *testing.T) {
	_, err := Parse("")
	assert.EqualError(t, err, `parse error (<unknown>.feature:1): no features parsed.`)
}

func TestTagWithoutFeature(t *testing.T) {
	_, err := Parse("@tag")
	assert.EqualError(t, err, `parse error (<unknown>.feature:1): tags not applied to feature.`)
}

func TestFailureExpectingFeature(t *testing.T) {
	_, err := Parse("@tag\n@tag")
	assert.EqualError(t, err, `parse error (<unknown>.feature:2): expected "Feature:", found "@tag".`)
}

func TestFailureInvalidTag(t *testing.T) {
	_, err := Parse("@tag tag")
	assert.EqualError(t, err, `parse error (<unknown>.feature:1): invalid tag "tag".`)
}

func TestFailureDescriptionAfterTags(t *testing.T) {
	s := `
Feature: Descriptions after tags
  @tag1
  Descriptions should not be allowed after tags

  Scenario: Scenario name here
    Given some precondition
    Then something happens
`
	_, err := Parse(s)
	assert.EqualError(t, err, `parse error (<unknown>.feature:4): illegal description text after tags.`)
}

func TestFailureDescriptionAfterScenario(t *testing.T) {
	s := `
Feature: Descriptions after scenario
  Scenario: Scenario name here
    Given some precondition
    Then something happens

  Descriptions should not be allowed after scenario

  Scenario: Another scenario
    Given some precondition
    Then something happens
`
	_, err := Parse(s)
	assert.EqualError(t, err, `parse error (<unknown>.feature:7): illegal description text after scenario.`)
}

func TestFailureMultipleBackgrounds(t *testing.T) {
	s := `
Feature: Multiple backgrounds
  Background:
    Given one

  Background:
    Given two
`
	_, err := Parse(s)
	assert.EqualError(t, err, `parse error (<unknown>.feature:6): multiple backgrounds not allowed.`)
}

func TestFailureBackgroundAfterScenario(t *testing.T) {
	s := `
Feature: Background after scenario
  Scenario: Scenario name here
    Given some precondition
    Then something happens

  Background:
    Given it's after a scenario
`
	_, err := Parse(s)
	assert.EqualError(t, err, `parse error (<unknown>.feature:7): illegal background after scenario.`)
}

func TestFailureInvalidStep(t *testing.T) {
	s := `
Feature: Invalid steps
  Scenario: Scenario name here
    Invalid step
`
	_, err := Parse(s)
	assert.EqualError(t, err, `parse error (<unknown>.feature:4): illegal step prefix "Invalid".`)
}

func TestFailureNoStepText(t *testing.T) {
	s := `
Feature: No step text
  Scenario: Scenario name here
    Given
`
	_, err := Parse(s)
	assert.EqualError(t, err, `parse error (<unknown>.feature:4): expected step text after "Given".`)
}

func TestFailureInvalidTagOnScenario(t *testing.T) {
	s := `
Feature: Invalid tag on scenario
  @invalid tags
  Scenario:
    Given a scenario
`
	_, err := Parse(s)
	assert.EqualError(t, err, `parse error (<unknown>.feature:3): invalid tag "tags".`)

}

func TestFailureInvalidBackground(t *testing.T) {
	s := `
Feature: Invalid background
  Background:
    Invalid step
  Scenario: A scenario
    Given a scenario
`
	_, err := Parse(s)
	assert.EqualError(t, err, `parse error (<unknown>.feature:4): illegal step prefix "Invalid".`)
}
