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
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"

	"gopkg.in/yaml.v3"
)

const (
	nameStepKey                = "name"
	savedQueryNameStepKey      = "saved_query_name"
	savedQueryStatementStepKey = "saved_query_statement"
	expectedRowsStepKey        = "expected_rows"
	expectedColumnsStepKey     = "expected_columns"
	doltTestGroupsStepKey      = "dolt_test_groups"
	doltTestTestsStepKey       = "dolt_test_tests"
	doltTestStatementsStepKey  = "dolt_test_statements"
)

// Step is the interface implemented by all workflow step types.
// It intentionally exposes only the step name to keep the interface generic.
// Step-type–specific fields should be accessed via type assertions.
type Step interface {
	// GetName returns the string value of the step name.
	GetName() string
}

// Steps is a slice of Step that implements YAML marshal/unmarshal to support
// polymorphic step decoding/encoding.
type Steps []Step

var allowedStepKeysLowered = map[string]bool{
	nameStepKey:                true,
	savedQueryNameStepKey:      true,
	savedQueryStatementStepKey: true,
	expectedRowsStepKey:        true,
	expectedColumnsStepKey:     true,
	doltTestGroupsStepKey:      true,
	doltTestTestsStepKey:       true,
	doltTestStatementsStepKey:  true,
}

// SavedQueryStep represents a step that executes a saved query and (optionally)
// validates expected row / column counts.
type SavedQueryStep struct {
	Name                yaml.Node `yaml:"name"`
	SavedQueryName      yaml.Node `yaml:"saved_query_name"`
	SavedQueryStatement yaml.Node `yaml:"saved_query_statement"`
	ExpectedColumns     yaml.Node `yaml:"expected_columns,omitempty"`
	ExpectedRows        yaml.Node `yaml:"expected_rows,omitempty"`
}

var _ Step = (*SavedQueryStep)(nil)

func (s *SavedQueryStep) GetName() string { return s.Name.Value }

// DoltTestStep represents a step that runs Dolt tests, either by groups or by
// explicit test names. At least one of groups or tests must be provided.
type DoltTestStep struct {
	Name       yaml.Node   `yaml:"name"`
	TestGroups []yaml.Node `yaml:"dolt_test_groups,omitempty"`
	Tests      []yaml.Node `yaml:"dolt_test_tests,omitempty"`

	// DoltTestStatements is populated by the `dolt ci view` command to show the
	// underlying queries associated with the tests selected by this step.
	DoltTestStatements []yaml.Node `yaml:"dolt_test_statements,omitempty"`
}

var _ Step = (*DoltTestStep)(nil)

func (s *DoltTestStep) GetName() string { return s.Name.Value }

func (s *Steps) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		*s = nil
		return nil
	}
	if value.Kind != yaml.SequenceNode {
		return fmt.Errorf("steps must be a YAML sequence")
	}

	result := make([]Step, 0, len(value.Content))

	for _, item := range value.Content {
		if item.Kind != yaml.MappingNode {
			return fmt.Errorf("each step must be a YAML mapping")
		}

		// Discover the concrete step type by inspecting keys.
		// yaml.v3 represents a mapping node's Content as alternating key/value nodes:
		// [key0, value0, key1, value1, ...]. Keys are at even indices; the corresponding
		// value is at i+1. We increment i by 2 to visit only keys here.
		isSavedQuery := false
		isDoltTest := false
		for i := 0; i+1 < len(item.Content); i += 2 {
			key := item.Content[i]
			loweredKey := strings.ToLower(key.Value)
			switch loweredKey {
			case savedQueryNameStepKey, savedQueryStatementStepKey, expectedRowsStepKey, expectedColumnsStepKey:
				isSavedQuery = true
			case doltTestGroupsStepKey, doltTestTestsStepKey:
				isDoltTest = true

				// ignore all other non workflow-step type keys
			}
		}

		// Try to extract a name for a clearer error message
		// and use later on
		var stepName string
		for i := 0; i+1 < len(item.Content); i += 2 {
			key := item.Content[i]
			if strings.ToLower(key.Value) == "name" {
				stepName = item.Content[i+1].Value
				break
			}
		}

		if isSavedQuery && isDoltTest {
			if stepName == "" {
				return fmt.Errorf("invalid config: step defines both saved_query_* fields and dolt_test_* fields")
			}
			return fmt.Errorf("invalid config: step '%s' defines both saved_query_* fields and dolt_test_* fields", stepName)
		}

		// Validate keys regardless of detected type to catch typos like
		// "expected_colums". Keys are validated case-insensitively by
		// lowercasing before comparison.

		// Validate all mapping keys (case-insensitive)
		for i := 0; i+1 < len(item.Content); i += 2 {
			keyNode := item.Content[i]
			key := keyNode.Value
			if allowedStepKeysLowered[strings.ToLower(key)] {
				continue
			}
			if stepName == "" {
				return fmt.Errorf("invalid config: unknown field %q", key)
			}
			return fmt.Errorf("invalid config: unknown field %q in step %q", key, stepName)
		}

		switch {
		case isSavedQuery:
			var sq SavedQueryStep
			if err := item.Decode(&sq); err != nil {
				return err
			}
			result = append(result, &sq)
		case isDoltTest:
			var dt DoltTestStep
			if err := item.Decode(&dt); err != nil {
				return err
			}
			result = append(result, &dt)
		default:
			return fmt.Errorf("unknown step type; keys must include saved_query_* or dolt_test_*")
		}
	}

	*s = result
	return nil
}

// MarshalYAML implements yaml.Marshaler for Steps by returning the underlying
// slice so the YAML library can marshal each element by its concrete type.
func (s Steps) MarshalYAML() (interface{}, error) {
	out := make([]interface{}, len(s))
	for i, st := range s {
		out[i] = st
	}
	return out, nil
}

type Job struct {
	Name  yaml.Node `yaml:"name"`
	Steps Steps     `yaml:"steps"`
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
			stepName := step.GetName()
			if stepName == "" {
				return fmt.Errorf("invalid config: step name is missing in job: %s", job.Name.Value)
			}
			if _, ok := steps[stepName]; ok {
				return fmt.Errorf("invalid config: step duplicated: %s", stepName)
			}
			steps[stepName] = true

			// Validate by concrete type (and ensure exactly one supported type)
			switch st := step.(type) {
			case *SavedQueryStep:
				if st.SavedQueryName.Value == "" {
					return fmt.Errorf("invalid config: step %s is missing saved_query_name", stepName)
				}
			case *DoltTestStep:
				if len(st.TestGroups) == 0 && len(st.Tests) == 0 {
					return fmt.Errorf("invalid config: dolt test step %s requires at least one group or test", stepName)
				}
				// Disallow redundant wildcard in both groups and tests, which would double-run the same full set
				if len(st.TestGroups) == 1 && st.TestGroups[0].Value == "*" && len(st.Tests) == 1 && st.Tests[0].Value == "*" {
					return fmt.Errorf("invalid config: dolt test step %s specifies wildcard for both dolt_test_groups and dolt_test_tests; specify a wildcard in only one field", stepName)
				}
			default:
				return fmt.Errorf("invalid config: unknown or unsupported step type for step: %s (must be exactly one of saved_query or dolt_test)", stepName)
			}
		}
	}

	return nil
}
