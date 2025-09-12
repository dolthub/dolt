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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseWorkflow(t *testing.T) {
	workflowName := "test workflow"
	mainBranch := "main"
	altBranch := "alt"
	alt2Branch := "alt-2"
	opened := "opened"
	synchronized := "synchronized"
	jobName := "my workflow job"
	stepName := "my workflow step"
	savedQueryName := "sq 1"
	expectedCols := ">= 2"
	expectedRows := "16"

	ymlTemplate := `name: %s
on:
  push:
    branches:
      - %s
      - %s
  pull_request:
    activities:
      - %s
      - %s
    branches:
      - %s
      - %s
  workflow_dispatch:

jobs:
  - name: %s
    steps:
      - name: %s
        saved_query_name: %s
        expected_columns: "%s"
        expected_rows: "%s"
`

	yml := fmt.Sprintf(ymlTemplate,
		workflowName,
		mainBranch,
		altBranch,
		opened,
		synchronized,
		mainBranch,
		alt2Branch,
		jobName,
		stepName,
		savedQueryName,
		expectedCols,
		expectedRows,
	)

	wf, err := ParseWorkflowConfig(strings.NewReader(yml))
	require.NoError(t, err)
	require.NotNil(t, wf)

	require.Equal(t, workflowName, wf.Name.Value)
	require.Equal(t, len(wf.On.Push.Branches), 2)
	require.Equal(t, len(wf.On.PullRequest.Branches), 2)
	require.Equal(t, len(wf.On.PullRequest.Activities), 2)
	require.Equal(t, wf.On.Push.Branches[0].Value, mainBranch)
	require.Equal(t, wf.On.Push.Branches[1].Value, altBranch)
	require.Equal(t, wf.On.PullRequest.Branches[0].Value, mainBranch)
	require.Equal(t, wf.On.PullRequest.Branches[1].Value, alt2Branch)
	require.Equal(t, wf.On.PullRequest.Activities[0].Value, opened)
	require.Equal(t, wf.On.PullRequest.Activities[1].Value, synchronized)
	require.Equal(t, len(wf.Jobs), 1)
	require.Equal(t, wf.Jobs[0].Name.Value, jobName)
	require.Equal(t, len(wf.Jobs[0].Steps), 1)
	sq, ok := wf.Jobs[0].Steps[0].(*SavedQueryStep)
	require.True(t, ok)
	require.Equal(t, sq.Name.Value, stepName)

	// validate config passes for saved_query only
	err = ValidateWorkflowConfig(wf)
	require.NoError(t, err)
}

func TestParseWorkflowWithDoltTestStep(t *testing.T) {
	workflowName := "workflow with dolt test"
	jobName := "test job"
	stepName := "run dolt tests"

	ymlTemplate := `name: %s
on:
  workflow_dispatch: {}

jobs:
  - name: %s
    steps:
      - name: %s
        dolt_test_groups:
          - group_a
          - group_b
`

	yml := fmt.Sprintf(ymlTemplate, workflowName, jobName, stepName)

	wf, err := ParseWorkflowConfig(strings.NewReader(yml))
	require.NoError(t, err)
	require.NotNil(t, wf)

	require.Equal(t, workflowName, wf.Name.Value)
	require.Equal(t, 1, len(wf.Jobs))
	require.Equal(t, jobName, wf.Jobs[0].Name.Value)
	require.Equal(t, 1, len(wf.Jobs[0].Steps))

	dt, ok := wf.Jobs[0].Steps[0].(*DoltTestStep)
	require.True(t, ok)
	require.Equal(t, stepName, dt.Name.Value)
	require.Equal(t, 2, len(dt.TestGroups))
	require.Equal(t, "group_a", dt.TestGroups[0].Value)
	require.Equal(t, "group_b", dt.TestGroups[1].Value)

	// validate config passes for dolt_test step
	err = ValidateWorkflowConfig(wf)
	require.NoError(t, err)
}

func TestParseWorkflowWithAmbiguousStepReturnsError(t *testing.T) {
	workflowName := "ambiguous workflow"
	jobName := "job"
	stepName := "ambiguous step"

	// This step contains both saved_query_* and dolt_test_* keys
	ymlTemplate := `name: %s
on:
  workflow_dispatch: {}

jobs:
  - name: %s
    steps:
      - name: %s
        saved_query_name: my_query
        dolt_test_groups:
          - group_a
`

	yml := fmt.Sprintf(ymlTemplate, workflowName, jobName, stepName)

	_, err := ParseWorkflowConfig(strings.NewReader(yml))
	require.Error(t, err)
	require.Contains(t, err.Error(), "defines both saved_query_* fields and dolt_test_* fields")
}

func TestParseWorkflowWithDoltTestTestsOnly(t *testing.T) {
	workflowName := "workflow with dolt test tests only"
	jobName := "test job"
	stepName := "run dolt tests list"

	ymlTemplate := `name: %s
on:
  workflow_dispatch: {}

jobs:
  - name: %s
    steps:
      - name: %s
        dolt_test_tests:
          - test_a
          - test_b
`

	yml := fmt.Sprintf(ymlTemplate, workflowName, jobName, stepName)

	wf, err := ParseWorkflowConfig(strings.NewReader(yml))
	require.NoError(t, err)
	require.NotNil(t, wf)

	require.Equal(t, workflowName, wf.Name.Value)
	require.Equal(t, 1, len(wf.Jobs))
	require.Equal(t, jobName, wf.Jobs[0].Name.Value)
	require.Equal(t, 1, len(wf.Jobs[0].Steps))

	dt, ok := wf.Jobs[0].Steps[0].(*DoltTestStep)
	require.True(t, ok)
	require.Equal(t, stepName, dt.Name.Value)
	require.Equal(t, 2, len(dt.Tests))
	require.Equal(t, "test_a", dt.Tests[0].Value)
	require.Equal(t, "test_b", dt.Tests[1].Value)
	require.Equal(t, 0, len(dt.TestGroups))

	// validate config passes
	err = ValidateWorkflowConfig(wf)
	require.NoError(t, err)
}

func TestParseWorkflowWithDoltTestGroupsAndTests(t *testing.T) {
	workflowName := "workflow with dolt test groups and tests"
	jobName := "test job"
	stepName := "run dolt groups and tests"

	ymlTemplate := `name: %s
on:
  workflow_dispatch: {}

jobs:
  - name: %s
    steps:
      - name: %s
        dolt_test_groups:
          - group_a
          - group_b
        dolt_test_tests:
          - test_c
`

	yml := fmt.Sprintf(ymlTemplate, workflowName, jobName, stepName)

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
