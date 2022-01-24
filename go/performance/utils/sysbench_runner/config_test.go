// Copyright 2019-2022 Dolthub, Inc.
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

package sysbench_runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var testIdFunc = func() string { return "id" }

func TestConfigTestGetTests(t *testing.T) {
	empty := &ConfigTest{Name: "test_name"}

	one := &ConfigTest{Name: "test_one", N: 3}
	two := &ConfigTest{Name: "test_two", N: 2}
	three := &ConfigTest{Name: "test_three", N: 1}

	opts := &ConfigTest{
		Name:    "test_options",
		N:       1,
		Options: []string{"--create_secondary=on", "--auto_inc=off"},
	}

	serverConfig := &ServerConfig{Server: MySql, Version: "test-version", Host: "localhost", ResultsFormat: CsvFormat}

	tests := []struct {
		description   string
		config        *Config
		expectedTests []*Test
		expectedError error
	}{
		{
			description: "should error if no test name is defined",
			config: &Config{
				Servers: []*ServerConfig{serverConfig},
				Tests: []*ConfigTest{
					{Name: ""},
				},
			},
			expectedTests: nil,
			expectedError: ErrTestNameNotDefined,
		},
		{
			description: "should create single test if N is < 1",
			config: &Config{
				Servers: []*ServerConfig{serverConfig},
				Tests:   []*ConfigTest{empty},
			},
			expectedTests: []*Test{
				{
					id:     testIdFunc(),
					Name:   "test_name",
					Params: fromConfigTestParams(empty, serverConfig),
				},
			},
		},
		{
			description: "should return a test for each N defined on the ConfigTest",
			config: &Config{
				Servers: []*ServerConfig{serverConfig},
				Tests:   []*ConfigTest{one, two, three},
			},
			expectedTests: []*Test{
				{id: testIdFunc(), Name: "test_one", Params: fromConfigTestParams(one, serverConfig)},
				{id: testIdFunc(), Name: "test_one", Params: fromConfigTestParams(one, serverConfig)},
				{id: testIdFunc(), Name: "test_one", Params: fromConfigTestParams(one, serverConfig)},
				{id: testIdFunc(), Name: "test_two", Params: fromConfigTestParams(two, serverConfig)},
				{id: testIdFunc(), Name: "test_two", Params: fromConfigTestParams(two, serverConfig)},
				{id: testIdFunc(), Name: "test_three", Params: fromConfigTestParams(three, serverConfig)},
			},
		},
		{
			description: "should apply user options to test params",
			config: &Config{
				Servers: []*ServerConfig{serverConfig},
				Tests:   []*ConfigTest{opts},
			},
			expectedTests: []*Test{
				{id: testIdFunc(), Name: "test_options", Params: fromConfigTestParams(opts, serverConfig)},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			for _, s := range test.config.Servers {
				actual, err := GetTests(test.config, s, testIdFunc)
				assert.Equal(t, test.expectedError, err)
				assert.Equal(t, len(test.expectedTests), len(actual))
				assert.ElementsMatch(t, test.expectedTests, actual)
			}
		})
	}
}
