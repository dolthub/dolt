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

package dolt_ci

import (
    "strings"
    "testing"

    "github.com/stretchr/testify/require"
)

func TestParseWorkflow(t *testing.T) {
    yml := `name: test workflow
on:
  push:
    branches:
      - main
      - alt
  pull_request:
    activities:
      - opened
      - synchronized
    branches:
      - main
      - alt-2
  workflow_dispatch:

jobs:
  - name: my workflow job
    steps:
      - name: my workflow step
        saved_query_name: sq 1
        expected_columns: ">= 2"
        expected_rows: "16"
`

	wf, err := ParseWorkflowConfig(strings.NewReader(yml))
	require.NoError(t, err)
	require.NotNil(t, wf)

    require.Equal(t, "test workflow", wf.Name.Value)
	require.Equal(t, len(wf.On.Push.Branches), 2)
	require.Equal(t, len(wf.On.PullRequest.Branches), 2)
	require.Equal(t, len(wf.On.PullRequest.Activities), 2)
    require.Equal(t, wf.On.Push.Branches[0].Value, "main")
    require.Equal(t, wf.On.Push.Branches[1].Value, "alt")
    require.Equal(t, wf.On.PullRequest.Branches[0].Value, "main")
    require.Equal(t, wf.On.PullRequest.Branches[1].Value, "alt-2")
    require.Equal(t, wf.On.PullRequest.Activities[0].Value, "opened")
    require.Equal(t, wf.On.PullRequest.Activities[1].Value, "synchronized")
	require.Equal(t, len(wf.Jobs), 1)
    require.Equal(t, wf.Jobs[0].Name.Value, "my workflow job")
	require.Equal(t, len(wf.Jobs[0].Steps), 1)
	sq, ok := wf.Jobs[0].Steps[0].(*SavedQueryStep)
	require.True(t, ok)
    require.Equal(t, sq.Name.Value, "my workflow step")

	// validate config passes for saved_query only
	err = ValidateWorkflowConfig(wf)
	require.NoError(t, err)
}

func TestParseWorkflowWithDoltTestStep(t *testing.T) {
    yml := `name: workflow with dolt test
on:
  workflow_dispatch: {}

jobs:
  - name: test job
    steps:
      - name: run dolt tests
        dolt_test_groups:
          - group_a
          - group_b
`

	wf, err := ParseWorkflowConfig(strings.NewReader(yml))
	require.NoError(t, err)
	require.NotNil(t, wf)

    require.Equal(t, "workflow with dolt test", wf.Name.Value)
	require.Equal(t, 1, len(wf.Jobs))
    require.Equal(t, "test job", wf.Jobs[0].Name.Value)
	require.Equal(t, 1, len(wf.Jobs[0].Steps))

	dt, ok := wf.Jobs[0].Steps[0].(*DoltTestStep)
	require.True(t, ok)
    require.Equal(t, "run dolt tests", dt.Name.Value)
	require.Equal(t, 2, len(dt.TestGroups))
	require.Equal(t, "group_a", dt.TestGroups[0].Value)
	require.Equal(t, "group_b", dt.TestGroups[1].Value)

	// validate config passes for dolt_test step
	err = ValidateWorkflowConfig(wf)
	require.NoError(t, err)
}

func TestParseWorkflowWithAmbiguousStepReturnsError(t *testing.T) {
    // This step contains both saved_query_* and dolt_test_* keys
    yml := `name: ambiguous workflow
on:
  workflow_dispatch: {}

jobs:
  - name: job
    steps:
      - name: ambiguous step
        saved_query_name: my_query
        dolt_test_groups:
          - group_a
`

	_, err := ParseWorkflowConfig(strings.NewReader(yml))
	require.Error(t, err)
	require.Contains(t, err.Error(), "defines both saved_query_* fields and dolt_test_* fields")
}

func TestParseWorkflowWithDoltTestTestsOnly(t *testing.T) {
    yml := `name: workflow with dolt test tests only
on:
  workflow_dispatch: {}

jobs:
  - name: test job
    steps:
      - name: run dolt tests list
        dolt_test_tests:
          - test_a
          - test_b
`

	wf, err := ParseWorkflowConfig(strings.NewReader(yml))
	require.NoError(t, err)
	require.NotNil(t, wf)

    require.Equal(t, "workflow with dolt test tests only", wf.Name.Value)
	require.Equal(t, 1, len(wf.Jobs))
    require.Equal(t, "test job", wf.Jobs[0].Name.Value)
	require.Equal(t, 1, len(wf.Jobs[0].Steps))

	dt, ok := wf.Jobs[0].Steps[0].(*DoltTestStep)
	require.True(t, ok)
    require.Equal(t, "run dolt tests list", dt.Name.Value)
	require.Equal(t, 2, len(dt.Tests))
	require.Equal(t, "test_a", dt.Tests[0].Value)
	require.Equal(t, "test_b", dt.Tests[1].Value)
	require.Equal(t, 0, len(dt.TestGroups))

	// validate config passes
	err = ValidateWorkflowConfig(wf)
	require.NoError(t, err)
}

func TestParseWorkflowWithDoltTestGroupsAndTests(t *testing.T) {
    yml := `name: workflow with dolt test groups and tests
on:
  workflow_dispatch: {}

jobs:
  - name: test job
    steps:
      - name: run dolt groups and tests
        dolt_test_groups:
          - group_a
          - group_b
        dolt_test_tests:
          - test_c
`

	wf, err := ParseWorkflowConfig(strings.NewReader(yml))
	require.NoError(t, err)
	require.NotNil(t, wf)

    dt, ok := wf.Jobs[0].Steps[0].(*DoltTestStep)
	require.True(t, ok)
	require.Equal(t, 2, len(dt.TestGroups))
	require.Equal(t, 1, len(dt.Tests))
	require.Equal(t, "group_a", dt.TestGroups[0].Value)
	require.Equal(t, "group_b", dt.TestGroups[1].Value)
	require.Equal(t, "test_c", dt.Tests[0].Value)

	// validate config passes
	err = ValidateWorkflowConfig(wf)
	require.NoError(t, err)
}
