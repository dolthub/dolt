// Copyright 2019 Liquidata, Inc.
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

var forceOpt = &Option{"force", "f", "", OptionalFlag, "force desc", nil}
var messageOpt = &Option{"message", "m", "msg", OptionalValue, "msg desc", nil}

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
			name: "empty",
			options: []*Option{},
			args: []string{},
			expectedOpts: map[string]string{},
			expectedArgs: []string{},
		},
		{
			name: "no options",
			options: []*Option{},
			args: []string{"a", "b", "c"},
			expectedOpts: map[string]string{},
			expectedArgs: []string{"a", "b", "c"},
		},
		{
			name: "-h",
			options: []*Option{},
			args: []string{"a", "-h", "c"},
			expectedErr: "Help",
		},
		{
			name: "--help",
			options: []*Option{},
			args: []string{"a", "--help", "c"},
			expectedErr: "Help",
		},
		{
			name: "force",
			options: []*Option{forceOpt},
			args: []string{"--force", "b", "c"},
			expectedOpts: map[string]string{"force": ""},
			expectedArgs: []string{"b", "c"},
		},
		{
			name: "force abbrev",
			options: []*Option{forceOpt},
			args: []string{"b", "-f", "c"},
			expectedOpts: map[string]string{"force": ""},
			expectedArgs: []string{"b", "c"},
		},
		{
			name: "message",
			options: []*Option{forceOpt, messageOpt},
			args: []string{"-m", "b", "c"},
			expectedOpts: map[string]string{"message": "b"},
			expectedArgs: []string{"c"},
		},
		{
			name: "message equals value",
			options: []*Option{forceOpt, messageOpt},
			args: []string{"b", "--message=value", "c"},
			expectedOpts: map[string]string{"message": "value"},
			expectedArgs: []string{"b", "c"},
		},
		{
			name: "empty string",
			options: []*Option{forceOpt, messageOpt},
			args: []string{"b", "--message=value", ""},
			expectedOpts: map[string]string{"message": "value"},
			expectedArgs: []string{"b", ""},
		},
		{
			name: "force abbrev w/o space",
			options: []*Option{forceOpt},
			args: []string{"bbb", "-fccc"},
			expectedOpts: map[string]string{"force": ""},
			expectedArgs: []string{"bbb", "ccc"},
		},
		{
			name: "-mvalue",
			options: []*Option{forceOpt, messageOpt},
			args: []string{"b", "-mvalue", "c"},
			expectedOpts: map[string]string{"message": "value"},
			expectedArgs: []string{"b", "c"},
		},
		{
			name: "--messagevalue",
			options: []*Option{forceOpt, messageOpt},
			args: []string{"b", "-messagevalue", "c"},
			expectedOpts: map[string]string{"message": "value"},
			expectedArgs: []string{"b", "c"},
		},
		{
			name: "-fmfootball",
			options: []*Option{forceOpt, messageOpt},
			args: []string{"-fmfootball"},
			expectedOpts: map[string]string{"message": "football", "force": ""},
			expectedArgs: []string{},
		},
		{
			name: "-ffootball",
			options: []*Option{forceOpt, messageOpt},
			args: []string{"-ffootball"},
			expectedOpts: map[string]string{"force": ""},
			expectedArgs: []string{"football"},
		},
		{
			name: "-mf",
			options: []*Option{forceOpt, messageOpt},
			args: []string{"-mf"},
			expectedOpts: map[string]string{"message": "f"},
			expectedArgs: []string{},
		},
		{
			name: "-fm",
			options: []*Option{forceOpt, messageOpt},
			args: []string{"-fm"},
			expectedErr: "error: no value for option `m'",
		},
		{
			name: "-mf value",
			options: []*Option{forceOpt, messageOpt},
			args: []string{"-mf", "value"},
			expectedOpts: map[string]string{"message": "f"},
			expectedArgs: []string{"value"},
		},
		{
			name: "-fm value",
			options: []*Option{forceOpt, messageOpt},
			args: []string{"-fm", "value"},
			expectedOpts: map[string]string{"message": "value", "force": ""},
			expectedArgs: []string{},
		},
		{
			name: "unsupported arg",
			options: []*Option{forceOpt, messageOpt},
			args: []string{"-v"},
			expectedErr: "error: unknown option `v'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewArgParser()

			for _, opt := range test.options {
				parser.SupportOption(opt)
			}

			exp := &ArgParseResults{test.expectedOpts, test.expectedArgs, parser}

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
