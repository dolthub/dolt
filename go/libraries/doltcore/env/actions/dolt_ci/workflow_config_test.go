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
	require.Equal(t, wf.Jobs[0].Steps[0].Name.Value, stepName)

	// todo: check expected stuff
}
