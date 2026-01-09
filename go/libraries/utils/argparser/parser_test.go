// Copyright 2020 Dolthub, Inc.
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createParserWithOptionalArgs() *ArgParser {
	ap := NewArgParserWithMaxArgs("test", 16)
	ap.SupportsFlag("flag", "f", "flag")
	ap.SupportsString("param", "p", "param", "")
	ap.SupportsOptionalString("optional", "o", "optional", "")

	return ap
}

func TestArgParser(t *testing.T) {
	tests := []struct {
		ap              *ArgParser
		args            []string
		expectedErr     error
		expectedOptions map[string]string
		expectedArgs    []string
	}{
		{
			NewArgParserWithVariableArgs("test"),
			[]string{},
			nil,
			map[string]string{},
			[]string{},
		},
		{
			NewArgParserWithVariableArgs("test"),
			[]string{"arg1", "arg2"},
			nil,
			map[string]string{},
			[]string{"arg1", "arg2"},
		},
		{
			NewArgParserWithVariableArgs("test"),
			[]string{"--unknown_flag"},
			UnknownArgumentParam{"unknown_flag"},
			map[string]string{},
			[]string{},
		},
		{
			NewArgParserWithVariableArgs("test"),
			[]string{"--help"},
			ErrHelp,
			map[string]string{},
			[]string{},
		},
		{
			NewArgParserWithVariableArgs("test"),
			[]string{"-h"},
			ErrHelp,
			map[string]string{},
			[]string{},
		},
		{
			NewArgParserWithVariableArgs("test"),
			[]string{"help"},
			nil,
			map[string]string{},
			[]string{"help"},
		},
		{
			NewArgParserWithVariableArgs("test").SupportsString("param", "p", "", ""),
			[]string{"--param", "value", "arg1"},
			nil,
			map[string]string{"param": "value"},
			[]string{"arg1"},
		},
		{
			NewArgParserWithVariableArgs("test").SupportsString("param", "p", "", ""),
			[]string{"-pvalue"},
			nil,
			map[string]string{"param": "value"},
			[]string{},
		},
		{
			NewArgParserWithVariableArgs("test").SupportsString("param", "p", "", ""),
			[]string{"--paramvalue"},
			UnknownArgumentParam{"paramvalue"},
			map[string]string{},
			[]string{},
		},
		{
			createParserWithOptionalArgs(),
			[]string{"foo", "bar"},
			nil,
			map[string]string{},
			[]string{"foo", "bar"},
		},
		{
			createParserWithOptionalArgs(),
			[]string{"-o", "-f", "foo", "bar"},
			nil,
			map[string]string{"flag": "", "optional": ""},
			[]string{"foo", "bar"},
		},
		{
			createParserWithOptionalArgs(),
			[]string{"-o", "optional value", "-f", "foo", "bar"},
			nil,
			map[string]string{"flag": "", "optional": "optional value"},
			[]string{"foo", "bar"},
		},
		{
			createParserWithOptionalArgs(),
			[]string{"-o", "--", "foo", "bar"},
			nil,
			map[string]string{"optional": ""},
			[]string{"foo", "bar"},
		},
		{
			createParserWithOptionalArgs(),
			[]string{"-p", "value", "-o"},
			nil,
			map[string]string{"param": "value", "optional": ""},
			[]string{},
		},
		{
			createParserWithOptionalArgs(),
			[]string{"-p", "value", "-o", "--"},
			nil,
			map[string]string{"param": "value", "optional": ""},
			[]string{},
		},
		{
			createParserWithOptionalArgs(),
			[]string{"-o", "-p", "value"},
			nil,
			map[string]string{"param": "value", "optional": ""},
			[]string{},
		},
		{
			NewArgParserWithVariableArgs("test").SupportsString("param", "p", "", ""),
			[]string{"--param", "value", "arg1", "--", "table1", "table2"},
			nil,
			map[string]string{"param": "value"},
			[]string{"arg1", "table1", "table2"},
		},
	}

	for _, test := range tests {
		apr, err := test.ap.Parse(test.args)
		require.Equal(t, test.expectedErr, err)

		if err == nil {
			assert.Equal(t, test.expectedOptions, apr.options)
			assert.Equal(t, test.expectedArgs, apr.Args)
		}
	}
}

func TestArgParserSet(t *testing.T) {
	ap := createParserWithOptionalArgs()
	apr, err := ap.Parse([]string{"-o", "optional value", "-f", "foo", "bar"})
	require.NoError(t, err)

	apr, err = apr.SetArgument("param", "abcdefg")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"flag": "", "optional": "optional value", "param": "abcdefg"}, apr.options)

	apr, err = apr.SetArgument("garbage", "garbage value")
	require.Error(t, err)
}

func TestRepeatableFlags(t *testing.T) {
	ap := NewArgParserWithVariableArgs("test")
	ap.SupportsRepeatableFlag("verbose", "v", "Verbose output")

	testCases := []struct {
		args          []string
		expectedCount int
		expectError   bool
	}{
		{[]string{"-v"}, 1, false},
		{[]string{"-vv"}, 2, false},
		{[]string{"-vvv"}, 3, false},
		{[]string{"-vvvv"}, 4, false},
		{[]string{"--verbose"}, 1, false},
		{[]string{"--verbose", "--verbose"}, 2, false},
		{[]string{"-v", "-v"}, 2, false},
		{[]string{"-v", "-vv"}, 3, false},
		{[]string{"-vv", "-v"}, 3, false},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("args=%v", test.args), func(t *testing.T) {
			apr, err := ap.Parse(test.args)
			if test.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			count, ok := apr.GetFlagCount("verbose")
			assert.True(t, ok)
			assert.Equal(t, test.expectedCount, count)
		})
	}
}

func TestRepeatableFlagsWithOtherOptions(t *testing.T) {
	ap := NewArgParserWithVariableArgs("test")
	ap.SupportsRepeatableFlag("verbose", "v", "Verbose output")
	ap.SupportsString("param", "p", "param", "")
	ap.SupportsFlag("flag", "f", "flag")

	testCases := []struct {
		name            string
		args            []string
		expectedVerbose int
		expectedParam   string
		expectedFlag    bool
		expectedArgs    []string
		expectError     bool
	}{
		{"verbose with param", []string{"-v", "-p", "value"}, 1, "value", false, []string{}, false},
		{"double verbose with param", []string{"-vv", "-p", "value"}, 2, "value", false, []string{}, false},
		{"interleaved verbose and flag", []string{"-v", "-f", "-v"}, 2, "", true, []string{}, false},
		{"verbose with flag and param", []string{"-vv", "-f", "-p", "test", "-v"}, 3, "test", true, []string{}, false},
		{"combined flags", []string{"-vfp", "value"}, 1, "value", true, []string{}, false},
		{"combined repeatable flags", []string{"-vvf", "-p", "data"}, 2, "data", true, []string{}, false},
		{"verbose with positional args", []string{"-vv", "arg1", "arg2"}, 2, "", false, []string{"arg1", "arg2"}, false},
		{"mixed order", []string{"-p", "value", "-vv", "-f", "-v", "arg1"}, 3, "value", true, []string{"arg1"}, false},
		{"long form mixed", []string{"--verbose", "--param", "test", "--flag", "--verbose"}, 2, "test", true, []string{}, false},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			apr, err := ap.Parse(test.args)
			if test.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Check verbose count
			verboseCount, ok := apr.GetFlagCount("verbose")
			if test.expectedVerbose > 0 {
				assert.True(t, ok)
				assert.Equal(t, test.expectedVerbose, verboseCount)
			} else {
				assert.False(t, ok)
				assert.Equal(t, 0, verboseCount)
			}

			// Check param value
			if test.expectedParam != "" {
				paramValue, exists := apr.GetValue("param")
				assert.True(t, exists)
				assert.Equal(t, test.expectedParam, paramValue)
			} else {
				assert.False(t, apr.Contains("param"))
			}

			// Check flag
			assert.Equal(t, test.expectedFlag, apr.Contains("flag"))

			// Check positional arguments
			assert.Equal(t, test.expectedArgs, apr.Args)
		})
	}
}

func TestGetFlagCountErrorHandling(t *testing.T) {
	ap := NewArgParserWithVariableArgs("test")
	ap.SupportsRepeatableFlag("verbose", "v", "Verbose output")
	ap.SupportsFlag("flag", "f", "flag")

	apr, err := ap.Parse([]string{"-vv", "-f"})
	require.NoError(t, err)

	// Test successful cases
	count, ok := apr.GetFlagCount("verbose")
	assert.True(t, ok)
	assert.Equal(t, 2, count)

	count, ok = apr.GetFlagCount("flag")
	assert.True(t, ok)
	assert.Equal(t, 1, count)

	// Test non-existent flag
	count, ok = apr.GetFlagCount("nonexistent")
	assert.False(t, ok)
	assert.Equal(t, 0, count)

	// Test flag that wasn't set
	ap.SupportsRepeatableFlag("debug", "d", "Debug output")
	count, ok = apr.GetFlagCount("debug")
	assert.False(t, ok)
	assert.Equal(t, 0, count)
}
