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
)

var ErrUnknownWorkflowSavedQueryExpectedResultsType = errors.New("unknown workflow saved query expected results type")

type WorkflowSavedQueryExpectedResultsType int

const (
	WorkflowSavedQueryExpectedResultsTypeUnspecified WorkflowSavedQueryExpectedResultsType = iota
	WorkflowSavedQueryExpectedResultsTypeRowColumnCount
)

type WorkflowSavedQueryStepId string

type WorkflowSavedQueryStep struct {
	Id                            *WorkflowSavedQueryStepId             `db:"id"`
	SavedQueryName                string                                `db:"saved_query_name"`
	WorkflowStepIdFK              *WorkflowStepId                       `db:"workflow_step_id_fk"`
	SavedQueryExpectedResultsType WorkflowSavedQueryExpectedResultsType `db:"saved_query_expected_results_type"`
}

// ToWorkflowSavedQueryExpectedResultsType is used to convert an int to a valid WorkflowSavedQueryExpectedResultsType
func ToWorkflowSavedQueryExpectedResultsType(t int) (WorkflowSavedQueryExpectedResultsType, error) {
	switch t {
	case int(WorkflowSavedQueryExpectedResultsTypeRowColumnCount):
		return WorkflowSavedQueryExpectedResultsTypeRowColumnCount, nil
	case int(WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified):
		return WorkflowSavedQueryExpectedResultsTypeUnspecified, nil
	default:
		return WorkflowSavedQueryExpectedResultsTypeUnspecified, ErrUnknownWorkflowSavedQueryExpectedResultsType
	}
}
