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

package benchmark_runner

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigTestGetTests(t *testing.T) {
	empty := &testConfigImpl{Name: "test_name"}

	one := &testConfigImpl{Name: "test_one", N: 3}
	two := &testConfigImpl{Name: "test_two", N: 2}
	three := &testConfigImpl{Name: "test_three", N: 1}

	opts := &testConfigImpl{
		Name:    "test_options",
		N:       1,
		Options: []string{"--create_secondary=on", "--auto_inc=off"},
	}

	serverConfig := &doltServerConfigImpl{Version: "test-version", Host: "localhost", ResultsFormat: CsvFormat}

	tests := []struct {
		description   string
		config        SysbenchConfig
		expectedTests []testTest
		expectedError error
	}{
		{
			description: "should error if no test name is defined",
			config: &sysbenchRunnerConfigImpl{
				Servers: []ServerConfig{serverConfig},
				Tests: []TestConfig{
					&testConfigImpl{Name: ""},
				},
			},
			expectedTests: nil,
			expectedError: ErrTestNameNotDefined,
		},
		{
			description: "should create single test if N is < 1",
			config: &sysbenchRunnerConfigImpl{
				Servers: []ServerConfig{serverConfig},
				Tests:   []TestConfig{empty},
			},
			expectedTests: []testTest{
				&testTestImpl{
					&sysbenchTestImpl{
						Name:   "test_name",
						Params: serverConfig.GetTestingParams(empty),
					},
				},
			},
		},
		{
			description: "should return a test for each N defined on the TestConfigImpl",
			config: &sysbenchRunnerConfigImpl{
				Servers: []ServerConfig{serverConfig},
				Tests:   []TestConfig{one, two, three},
			},
			expectedTests: []testTest{
				&testTestImpl{&sysbenchTestImpl{Name: "test_one", Params: serverConfig.GetTestingParams(one)}},
				&testTestImpl{&sysbenchTestImpl{Name: "test_one", Params: serverConfig.GetTestingParams(one)}},
				&testTestImpl{&sysbenchTestImpl{Name: "test_one", Params: serverConfig.GetTestingParams(one)}},
				&testTestImpl{&sysbenchTestImpl{Name: "test_two", Params: serverConfig.GetTestingParams(two)}},
				&testTestImpl{&sysbenchTestImpl{Name: "test_two", Params: serverConfig.GetTestingParams(two)}},
				&testTestImpl{&sysbenchTestImpl{Name: "test_three", Params: serverConfig.GetTestingParams(three)}},
			},
		},
		{
			description: "should apply user options to test params",
			config: &sysbenchRunnerConfigImpl{
				Servers: []ServerConfig{serverConfig},
				Tests:   []TestConfig{opts},
			},
			expectedTests: []testTest{
				&testTestImpl{&sysbenchTestImpl{Name: "test_options", Params: serverConfig.GetTestingParams(opts)}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			svs := test.config.GetServerConfigs()
			for _, s := range svs {
				actual, err := GetTests(test.config, s)
				assert.Equal(t, test.expectedError, err)
				assert.Equal(t, len(test.expectedTests), len(actual))
				updatedExpected := make([]SysbenchTest, len(actual))
				for idx, a := range actual {
					e := test.expectedTests[idx]
					e.SetId(a.GetId())
					updatedExpected[idx] = e.GetSysbenchTest()
				}
				assert.ElementsMatch(t, updatedExpected, actual)
			}
		})
	}
}

type testTest interface {
	SetId(id string)
	GetSysbenchTest() SysbenchTest
	SysbenchTest
}

type testTestImpl struct {
	*sysbenchTestImpl
}

func (t *testTestImpl) GetSysbenchTest() SysbenchTest {
	return t.sysbenchTestImpl
}

func (t *testTestImpl) SetId(id string) {
	t.id = id
}

var _ testTest = &testTestImpl{}
