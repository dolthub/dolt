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
	"errors"
	"time"
)

var ErrUnknownWorkflowStepType = errors.New("unknown workflow step type")

type WorkflowStepType int

const (
	WorkflowStepTypeUnspecified WorkflowStepType = iota
	WorkflowStepTypeSavedQuery
)

type WorkflowStepId string

type WorkflowStep struct {
	Id              *WorkflowStepId  `db:"id"`
	Name            string           `db:"name"`
	WorkflowJobIdFK *WorkflowJobId   `db:"workflow_job_id_fk"`
	StepType        WorkflowStepType `db:"step_type"`
	StepOrder       int              `db:"step_order"`
	CreatedAt       time.Time        `db:"created_at"`
	UpdatedAt       time.Time        `db:"updated_at"`
}

func toWorkflowStepType(t int) (WorkflowStepType, error) {
	switch t {
	case int(WorkflowStepTypeSavedQuery):
		return WorkflowStepTypeSavedQuery, nil
	default:
		return WorkflowStepTypeUnspecified, ErrUnknownWorkflowStepType
	}
}
