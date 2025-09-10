// Copyright 2024 Dolthub, Inc.
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
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"

	"gopkg.in/yaml.v3"
)

type Step struct {
	Name                yaml.Node `yaml:"name"`
	SavedQueryName      yaml.Node `yaml:"saved_query_name"`
	SavedQueryStatement yaml.Node `yaml:"saved_query_statement"`
	ExpectedColumns     yaml.Node `yaml:"expected_columns,omitempty"`
	ExpectedRows        yaml.Node `yaml:"expected_rows,omitempty"`
	DoltTest            *DoltTest `yaml:"dolt_test,omitempty"`
}

type Job struct {
	Name  yaml.Node `yaml:"name"`
	Steps []Step    `yaml:"steps"`
}

// DoltTest defines selectors for dolt_test_run
type DoltTest struct {
	Tests  []yaml.Node `yaml:"tests"`
	Groups []yaml.Node `yaml:"groups"`
}

type Push struct {
	Branches []yaml.Node `yaml:"branches"`
}

type PullRequest struct {
	Branches   []yaml.Node `yaml:"branches"`
	Activities []yaml.Node `yaml:"activities"`
}

type WorkflowDispatch struct{}

type On struct {
	Push             *Push             `yaml:"push,omitempty"`
	PullRequest      *PullRequest      `yaml:"pull_request,omitempty"`
	WorkflowDispatch *WorkflowDispatch `yaml:"workflow_dispatch,omitempty"`
}

type WorkflowConfig struct {
	Name yaml.Node `yaml:"name"`
	On   On        `yaml:"on"`
	Jobs []Job     `yaml:"jobs"`
}

func ParseWorkflowConfig(r io.Reader) (workflow *WorkflowConfig, err error) {
	workflow = &WorkflowConfig{}

	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)

	err = decoder.Decode(workflow)

	// todo: read config again and check for raw fields, like push and pull request and workflow dispatch
	return
}

func WorkflowConfigToYaml(workflow *WorkflowConfig) (r io.Reader, err error) {
	if workflow == nil {
		err = errors.New("workflow config is nil")
		return
	}

	var b []byte
	b, err = yaml.Marshal(workflow)
	if err != nil {
		return
	}

	r = bytes.NewReader(b)
	return
}

func ValidateWorkflowConfig(workflow *WorkflowConfig) error {
	if workflow.On.WorkflowDispatch == nil && workflow.On.Push == nil && workflow.On.PullRequest == nil {
		return fmt.Errorf("invalid config: no event triggers defined for workflow")
	}

	if workflow.On.Push != nil {

		branches := make(map[string]bool)
		for _, branch := range workflow.On.Push.Branches {
			_, ok := branches[branch.Value]
			if ok {
				return fmt.Errorf("invalid config: on push branch duplicated: %s", branch.Value)
			}
			if !ref.IsValidBranchName(branch.Value) {
				return fmt.Errorf("invalid branch name: %s", branch.Value)
			}
			branches[branch.Value] = true
		}
	}

	if workflow.On.PullRequest != nil {
		branches := make(map[string]bool)
		for _, branch := range workflow.On.PullRequest.Branches {
			_, ok := branches[branch.Value]
			if ok {
				return fmt.Errorf("invalid config: on pull request branch duplicated: %s", branch.Value)
			}
			if !ref.IsValidBranchName(branch.Value) {
				return fmt.Errorf("invalid branch name: %s", branch.Value)
			}
			branches[branch.Value] = true
		}

		activities := make(map[string]bool)
		for _, activity := range workflow.On.PullRequest.Activities {
			_, err := ToWorkflowEventTriggerActivityType(activity.Value)
			if err != nil {
				return fmt.Errorf("invalid config: unknown activity type: %s", activity.Value)
			}
			_, ok := activities[activity.Value]
			if ok {
				return fmt.Errorf("invalid config: on pull request activities duplicated: %s", activity.Value)
			} else {
				activities[activity.Value] = true
			}
		}
	}

	jobs := make(map[string]bool)
	steps := make(map[string]bool)

	if len(workflow.Jobs) == 0 {
		return fmt.Errorf("invalid config: no jobs defined for workflow: %s", workflow.Name.Value)
	}

	for _, job := range workflow.Jobs {
		if len(job.Steps) == 0 {
			return fmt.Errorf("invalid config: no steps defined for job: %s", job.Name.Value)
		}

		_, ok := jobs[job.Name.Value]
		if ok {
			return fmt.Errorf("invalid config: job duplicated: %s", job.Name.Value)
		} else {
			jobs[job.Name.Value] = true
		}

		for _, step := range job.Steps {
			_, ok := steps[step.Name.Value]
			if ok {
				return fmt.Errorf("invalid config: step duplicated: %s", step.Name.Value)
			} else {
				steps[step.Name.Value] = true
			}

			isSavedQuery := step.SavedQueryName.Value != ""
			isDoltTest := step.DoltTest != nil
			if !isSavedQuery && !isDoltTest {
				return fmt.Errorf("invalid config: step %s must specify saved_query_name or dolt_test", step.Name.Value)
			}
			if isSavedQuery && isDoltTest {
				return fmt.Errorf("invalid config: step %s cannot specify both saved_query_name and dolt_test", step.Name.Value)
			}
		}
	}

	return nil
}
