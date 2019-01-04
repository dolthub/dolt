package argparser

import (
	"testing"
)

var forceOpt = &Option{"force", "f", "", OptionalFlag, "force desc", nil}
var messageOpt = &Option{"message", "m", "msg", OptionalValue, "msg desc", nil}

func TestParsing(t *testing.T) {
	tests := []struct {
		name         string
		options      []*Option
		args         []string
		expectedOpts map[string]string
		expectedArgs []string
	}{
		{
			"empty",
			[]*Option{},
			[]string{},
			map[string]string{},
			[]string{},
		},
		{
			"no options",
			[]*Option{},
			[]string{"a", "b", "c"},
			map[string]string{},
			[]string{"a", "b", "c"},
		},
		{
			"force",
			[]*Option{forceOpt},
			[]string{"--force", "b", "c"},
			map[string]string{"force": ""},
			[]string{"b", "c"},
		},
		{
			"force abbrev",
			[]*Option{forceOpt},
			[]string{"b", "-f", "c"},
			map[string]string{"force": ""},
			[]string{"b", "c"},
		},
		{
			"message",
			[]*Option{forceOpt, messageOpt},
			[]string{"-m", "b", "c"},
			map[string]string{"message": "b"},
			[]string{"c"},
		},
		{
			"message equals value",
			[]*Option{forceOpt, messageOpt},
			[]string{"b", "--message=value", "c"},
			map[string]string{"message": "value"},
			[]string{"b", "c"},
		},
	}

	for _, test := range tests {
		parser := NewArgParser()

		for _, opt := range test.options {
			parser.SupportOption(opt)
		}

		res, err := parser.Parse(test.args)

		if err != nil {
			t.Error("In test", test.name, err)
		} else {
			if !res.Equals(&ArgParseResults{test.expectedOpts, test.expectedArgs, parser}) {
				t.Error("In test", test.name, "result did not match expected")
			}
		}
	}
}
