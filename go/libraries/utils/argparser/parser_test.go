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
			[]string{"arg1", "--", "table1", "table2"},
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
