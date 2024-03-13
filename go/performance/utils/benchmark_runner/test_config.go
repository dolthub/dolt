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

import "github.com/google/uuid"

type TestConfig interface {
	GetName() string
	GetOptions() []string
	AppendOption(opt string)
	GetTests(serverConfig ServerConfig) ([]Test, error)
	NewId() string
}

type testConfigImpl struct {
	// Name is the test name
	Name string

	// N is the number of times a test should run
	N int

	// Options are additional sysbench test options a user can supply to run with this test
	Options []string

	// FromScript is a boolean indicating that this test is from a lua script
	FromScript bool
}

var _ TestConfig = &testConfigImpl{}

func NewTestConfig(name string, opts []string, fromScript bool) *testConfigImpl {
	options := make([]string, 0)
	options = append(options, opts...)
	return &testConfigImpl{
		Name:       name,
		N:          1,
		Options:    options,
		FromScript: fromScript,
	}
}

func (ct *testConfigImpl) NewId() string {
	return uuid.New().String()
}

func (ct *testConfigImpl) GetName() string {
	return ct.Name
}

func (ct *testConfigImpl) GetOptions() []string {
	return ct.Options
}

func (ct *testConfigImpl) AppendOption(opt string) {
	ct.Options = append(ct.Options, opt)
}

func (ct *testConfigImpl) GetTests(serverConfig ServerConfig) ([]Test, error) {
	if ct.Name == "" {
		return nil, ErrTestNameNotDefined
	}
	if ct.N < 1 {
		ct.N = 1
	}

	params := serverConfig.GetTestingParams(ct)
	tests := make([]Test, 0)

	for i := 0; i < ct.N; i++ {
		//p := make([]string, params.Len())
		//copy(p, params)
		tests = append(tests, NewSysbenchTest(ct.NewId(), ct.Name, params, ct.FromScript))
	}

	return tests, nil
}
