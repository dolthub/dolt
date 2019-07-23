package argparser

import (
	"reflect"
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
		{
			"empty string",
			[]*Option{forceOpt, messageOpt},
			[]string{"b", "--message=value", ""},
			map[string]string{"message": "value"},
			[]string{"b", ""},
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

func TestValidation(t *testing.T) {
	ap := NewArgParser()
	ap.SupportsString("string", "s", "string_value", "A string")
	ap.SupportsString("string2", "", "string_value", "Another string")
	ap.SupportsFlag("flag", "f", "A flag")
	ap.SupportsFlag("flag2", "", "Another flag")
	ap.SupportsInt("integer", "n", "num", "A number")
	ap.SupportsInt("integer2", "", "num", "Another number")
	apr, err := ap.Parse([]string{"-s", "string", "--flag", "--n", "1234", "a", "b", "c"})

	if err != nil {
		t.Fatal(err.Error())
	}

	if !apr.ContainsAll("string", "flag", "integer") {
		t.Error("Missing expected parameter(s)")
	}

	if apr.ContainsAny("string2", "flag2", "integer2") {
		t.Error("Contains unexpected parameter(s)")
	}

	if val := apr.MustGetValue("string"); val != "string" {
		t.Error("Bad val for -string")
	}

	if val := apr.GetValueOrDefault("string2", "default"); val != "default" {
		t.Error("Bad val for -string2")
	}

	if _, ok := apr.GetValue("string2"); ok {
		t.Error("Should not be able to get missing parameter string2")
	}

	if val, ok := apr.GetValue("string"); !ok || val != "string" {
		t.Error("Bad val for --string")
	}

	if val, ok := apr.GetInt("integer"); !ok || val != 1234 {
		t.Error("Bad val for --integer")
	}

	if val := apr.GetIntOrDefault("integer2", 5678); val != 5678 {
		t.Error("Bad val for --integer2")
	}

	trueFlags := apr.AnyFlagsEqualTo(true)
	falseFlags := apr.AnyFlagsEqualTo(false)

	if trueFlags.Size() != 1 || falseFlags.Size() != 1 {
		t.Error("AnyFlagsEqualTo error")
	}

	trueSet := apr.FlagsEqualTo([]string{"flag"}, true)
	falseSet := apr.FlagsEqualTo([]string{"flag"}, false)

	if trueSet.Size() != 1 && falseSet.Size() != 0 {
		t.Error("FlagsEqualTo error")
	}

	expectedArgs := []string{"a", "b", "c"}

	if apr.NArg() != 3 || apr.Arg(0) != "a" || !reflect.DeepEqual(apr.args, expectedArgs) {
		t.Error("Arg list issues")
	}
}
