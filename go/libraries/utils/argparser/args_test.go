// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package argparser

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var forceOpt = &Option{"force", "f", "", OptionalFlag, "force desc", nil, false}
var messageOpt = &Option{"message", "m", "msg", OptionalValue, "msg desc", nil, false}
var fileTypeOpt = &Option{"file-type", "", "", OptionalValue, "file type", nil, false}
var notOpt = &Option{"not", "", "", OptionalValue, "not desc", nil, true}

func TestParsing(t *testing.T) {
	tests := []struct {
		name         string
		options      []*Option
		args         []string
		expectedOpts map[string]string
		expectedArgs []string
		expectedErr  string
	}{
		{
			name:         "empty",
			options:      []*Option{},
			args:         []string{},
			expectedOpts: map[string]string{},
			expectedArgs: []string{},
		},
		{
			name:         "no options",
			options:      []*Option{},
			args:         []string{"a", "b", "c"},
			expectedOpts: map[string]string{},
			expectedArgs: []string{"a", "b", "c"},
		},
		{
			name:        "-h",
			options:     []*Option{},
			args:        []string{"a", "-h", "c"},
			expectedErr: "Help",
		},
		{
			name:        "--help",
			options:     []*Option{},
			args:        []string{"a", "--help", "c"},
			expectedErr: "Help",
		},
		{
			name:         "force",
			options:      []*Option{forceOpt},
			args:         []string{"--force", "b", "c"},
			expectedOpts: map[string]string{"force": ""},
			expectedArgs: []string{"b", "c"},
		},
		{
			name:         "force abbrev",
			options:      []*Option{forceOpt},
			args:         []string{"b", "-f", "c"},
			expectedOpts: map[string]string{"force": ""},
			expectedArgs: []string{"b", "c"},
		},
		{
			name:         "message",
			options:      []*Option{forceOpt, messageOpt},
			args:         []string{"-m", "b", "c"},
			expectedOpts: map[string]string{"message": "b"},
			expectedArgs: []string{"c"},
		},
		{
			name:         "message equals value",
			options:      []*Option{forceOpt, messageOpt},
			args:         []string{"b", "--message=value", "c"},
			expectedOpts: map[string]string{"message": "value"},
			expectedArgs: []string{"b", "c"},
		},
		{
			name:         "message colon value",
			options:      []*Option{forceOpt, messageOpt},
			args:         []string{"b", "--message:value", "c"},
			expectedOpts: map[string]string{"message": "value"},
			expectedArgs: []string{"b", "c"},
		},
		{
			name:         "empty string",
			options:      []*Option{forceOpt, messageOpt},
			args:         []string{"b", "--message=value", ""},
			expectedOpts: map[string]string{"message": "value"},
			expectedArgs: []string{"b", ""},
		},
		{
			name:         "value attached to flag",
			options:      []*Option{forceOpt},
			args:         []string{"-fvalue"},
			expectedOpts: map[string]string{"force": ""},
			expectedArgs: []string{"value"},
		},
		{
			name:         "-mvalue",
			options:      []*Option{forceOpt, messageOpt},
			args:         []string{"b", "-mvalue", "c"},
			expectedOpts: map[string]string{"message": "value"},
			expectedArgs: []string{"b", "c"},
		},
		{
			name:        "--messagevalue",
			options:     []*Option{forceOpt, messageOpt},
			args:        []string{"b", "--messagevalue", "c"},
			expectedErr: "error: unknown option `messagevalue'",
		},
		{
			name:         "-fm football",
			options:      []*Option{forceOpt, messageOpt},
			args:         []string{"-fm football"},
			expectedOpts: map[string]string{"message": "football", "force": ""},
			expectedArgs: []string{},
		},
		{
			name:         "-ffootball",
			options:      []*Option{forceOpt, messageOpt},
			args:         []string{"-ffootball"},
			expectedOpts: map[string]string{"force": ""},
			expectedArgs: []string{"football"},
		},
		{
			name:         "-m f",
			options:      []*Option{forceOpt, messageOpt},
			args:         []string{"-m f"},
			expectedOpts: map[string]string{"message": "f"},
			expectedArgs: []string{},
		},
		{
			name:        "-fm",
			options:     []*Option{forceOpt, messageOpt},
			args:        []string{"-fm"},
			expectedErr: "error: no value for option `m'",
		},
		{
			name:         "-m f value",
			options:      []*Option{forceOpt, messageOpt},
			args:         []string{"-m f", "value"},
			expectedOpts: map[string]string{"message": "f"},
			expectedArgs: []string{"value"},
		},
		{
			name:         "--not string list value",
			options:      []*Option{forceOpt, messageOpt, notOpt},
			args:         []string{"-m f", "value", "--not", "main", "branch"},
			expectedOpts: map[string]string{"message": "f", "not": "main,branch"},
			expectedArgs: []string{"value"},
		},
		{
			name:         "-fm value",
			options:      []*Option{forceOpt, messageOpt},
			args:         []string{"-fm", "value"},
			expectedOpts: map[string]string{"message": "value", "force": ""},
			expectedArgs: []string{},
		},
		{
			name:         "file-type not force",
			options:      []*Option{forceOpt, messageOpt, fileTypeOpt},
			args:         []string{"--file-type=csv"},
			expectedOpts: map[string]string{"file-type": "csv"},
			expectedArgs: []string{},
		},
		{
			name:        "unsupported arg",
			options:     []*Option{forceOpt, messageOpt},
			args:        []string{"-v"},
			expectedErr: "error: unknown option `v'",
		},
		{
			name:        "duplicate arg",
			options:     []*Option{forceOpt, messageOpt},
			args:        []string{"-f", "-f"},
			expectedErr: "error: multiple values provided for `force'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewArgParserWithVariableArgs("test")

			for _, opt := range test.options {
				parser.SupportOption(opt)
			}

			exp := &ArgParseResults{test.expectedOpts, test.expectedArgs, parser, NO_POSITIONAL_ARGS}

			res, err := parser.Parse(test.args)
			if test.expectedErr != "" {
				require.Error(t, err, test.expectedErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, exp, res)
			}
		})
	}
}

func TestValidation(t *testing.T) {
	ap := NewArgParserWithVariableArgs("test")
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

	if apr.NArg() != 3 || apr.Arg(0) != "a" || !reflect.DeepEqual(apr.Args, expectedArgs) {
		t.Error("Arg list issues")
	}
}

func TestDropValue(t *testing.T) {
	ap := NewArgParserWithVariableArgs("test")

	ap.SupportsString("string", "", "string_value", "A string")
	ap.SupportsFlag("flag", "", "A flag")

	apr, err := ap.Parse([]string{"--string", "str", "--flag", "1234"})
	if err != nil {
		t.Fatal(err.Error())
	}

	newApr1 := apr.DropValue("string")
	require.NotEqualf(t, apr, newApr1, "Original value and new value are equal")

	_, hasVal := newApr1.GetValue("string")
	if hasVal {
		t.Error("DropValue failed to drop string")
	}
	_, hasVal = newApr1.GetValue("flag")
	if !hasVal {
		t.Error("DropValue dropped the wrong value")
	}
	if newApr1.NArg() != 1 || newApr1.Arg(0) != "1234" {
		t.Error("DropValue didn't preserve args")
	}

	newApr2 := apr.DropValue("flag")
	require.NotEqualf(t, apr, newApr2, "DropValue fails to drop flag")

	_, hasVal = newApr2.GetValue("string")
	if !hasVal {
		t.Error("DropValue dropped the wrong value")
	}
	_, hasVal = newApr2.GetValue("flag")
	if hasVal {
		t.Error("DropValue failed to drop flag")
	}
	if newApr2.NArg() != 1 || newApr2.Arg(0) != "1234" {
		t.Error("DropValue didn't preserve args")
	}

}
