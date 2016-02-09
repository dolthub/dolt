package gherkin

type Language string

// Translation represents the Gherkin syntax keywords in a single language.
type Translation struct {
	// Language specific term representing a feature.
	Feature string

	// Language specific term representing the feature background.
	Background string

	// Language specific term representing a scenario.
	Scenario string

	// Language specific term representing a scenario outline.
	Outline string

	// Language specific term representing the "And" step.
	And string

	// Language specific term representing the "Given" step.
	Given string

	// Language specific term representing the "When" step.
	When string

	// Language specific term representing the "Then" step.
	Then string

	// Language specific term representing a scenario outline prefix.
	Examples string
}

const (
	// The language code for English translations.
	LANG_EN = Language("en")
)

var (
	// Translations contains internationalized translations of the Gherkin
	// syntax keywords in a variety of supported languages.
	Translations = map[Language]Translation{
		LANG_EN: Translation{
			Feature:    "Feature",
			Background: "Background",
			Scenario:   "Scenario",
			Outline:    "Scenario Outline",
			And:        "And",
			Given:      "Given",
			When:       "When",
			Then:       "Then",
			Examples:   "Examples",
		},
	}
)
