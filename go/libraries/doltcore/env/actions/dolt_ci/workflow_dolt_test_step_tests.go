// Copyright 2025 Dolthub, Inc.
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

// WorkflowDoltTestStepTestId is the ID type for workflow_dolt_test_step_tests rows.
type WorkflowDoltTestStepTestId string

// WorkflowDoltTestStepTest models a row in workflow_dolt_test_step_tests, which
// declares an individual test to run for a given Dolt Test step.
type WorkflowDoltTestStepTest struct {
	Id                       *WorkflowDoltTestStepTestId `db:"id"`
	TestName                 string                      `db:"test_name"`
	WorkflowDoltTestStepIdFK *WorkflowDoltTestStepId     `db:"workflow_dolt_test_step_id_fk"`
}
