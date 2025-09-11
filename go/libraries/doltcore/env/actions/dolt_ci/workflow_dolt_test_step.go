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

// WorkflowDoltTestStepId is the ID type for workflow_dolt_test_steps rows.
type WorkflowDoltTestStepId string

// WorkflowDoltTestStep models a row in workflow_dolt_test_steps, which attaches
// a Dolt Test step to a generic workflow step.
type WorkflowDoltTestStep struct {
	Id                   *WorkflowDoltTestStepId `db:"id"`
	WorkflowStepIdFK     *WorkflowStepId         `db:"workflow_step_id_fk"`
}
