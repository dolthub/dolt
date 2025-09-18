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

// WorkflowDoltTestStepGroupId is the ID type for workflow_dolt_test_step_groups rows.
type WorkflowDoltTestStepGroupId string

// WorkflowDoltTestStepGroup models a row in workflow_dolt_test_step_groups, which
// declares a named group to run for a given Dolt Test step.
type WorkflowDoltTestStepGroup struct {
	Id                       *WorkflowDoltTestStepGroupId `db:"id"`
	GroupName                string                       `db:"group_name"`
	WorkflowDoltTestStepIdFK *WorkflowDoltTestStepId      `db:"workflow_dolt_test_step_id_fk"`
}
