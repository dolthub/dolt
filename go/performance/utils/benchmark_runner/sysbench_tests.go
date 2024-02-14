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

type sysbenchTestParamsImpl struct {
	params []string
}

var _ SysbenchTestParams = &sysbenchTestParamsImpl{}

func (s *sysbenchTestParamsImpl) ToSlice() []string {
	return s.params
}

func (s *sysbenchTestParamsImpl) Append(params ...string) {
	s.params = append(s.params, params...)
}

func NewSysbenchTestParams() *sysbenchTestParamsImpl {
	return &sysbenchTestParamsImpl{params: make([]string, 0)}
}

// sysbenchTestImpl is a single sysbench test
type sysbenchTestImpl struct {
	id string

	// Name is the test name
	Name string

	// Params are the parameters passed to sysbench
	Params TestParams

	// FromScript indicates if this test is from a lua script
	FromScript bool
}

var _ SysbenchTest = &sysbenchTestImpl{}

func NewSysbenchTest(id, name string, params TestParams, fromScript bool) *sysbenchTestImpl {
	return &sysbenchTestImpl{
		id:         id,
		Name:       name,
		Params:     params,
		FromScript: fromScript,
	}
}

func (t *sysbenchTestImpl) GetId() string {
	return t.id
}

func (t *sysbenchTestImpl) GetName() string {
	return t.Name
}

func (t *sysbenchTestImpl) GetParamsToSlice() []string {
	return t.Params.ToSlice()
}

func (t *sysbenchTestImpl) GetFromScript() bool {
	return t.FromScript
}

// PrepareArgs returns a test's args for sysbench's prepare step
func (t *sysbenchTestImpl) GetPrepareArgs(serverConfig ServerConfig) []string {
	return withCommand(t.Params, sysbenchPrepareCommand)
}

// Run returns a test's args for sysbench's run step
func (t *sysbenchTestImpl) GetRunArgs(serverConfig ServerConfig) []string {
	return withCommand(t.Params, sysbenchRunCommand)
}

// Cleanup returns a test's args for sysbench's cleanup step
func (t *sysbenchTestImpl) GetCleanupArgs(serverConfig ServerConfig) []string {
	return withCommand(t.Params, sysbenchCleanupCommand)
}

func withCommand(params TestParams, command string) []string {
	c := make([]string, 0)
	c = append(c, params.ToSlice()...)
	return append(c, command)
}
