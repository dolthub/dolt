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
	"io"

	"gopkg.in/yaml.v3"
)

type Step struct {
	Name            string `yaml:"name"`
	SavedQueryName  string `yaml:"saved_query_name"`
	ExpectedColumns string `yaml:"expected_columns"`
	ExpectedRows    string `yaml:"expected_rows"`
}

type Job struct {
	Name  string `yaml:"name"`
	Steps []Step `yaml:"steps"`
}

type Push struct {
	Branches []string `yaml:"branches"`
}

type PullRequest struct {
	Branches   []string `yaml:"branches"`
	Activities []string `yaml:"activities"`
}

type WorkflowDispatch struct{}

type On struct {
	Push             *Push             `yaml:"push,omitempty"`
	PullRequest      *PullRequest      `yaml:"pull_request,omitempty"`
	WorkflowDispatch *WorkflowDispatch `yaml:"workflow_dispatch,omitempty"`
}

type WorkflowConfig struct {
	Name string `yaml:"name"`
	On   On     `yaml:"on"`
	Jobs []Job  `yaml:"jobs"`
}

func ParseWorkflowConfig(r io.Reader) (workflow *WorkflowConfig, err error) {
	workflow = &WorkflowConfig{}

	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)

	err = decoder.Decode(workflow)
	return
}

func ValidateWorkflowConfig(workflow *WorkflowConfig) error {
	// todo: ensure branch names exist only once for each event
	// todo: ensure activities exist only once for each event
	return nil
}
