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

var ErrUnknownWorkflowSavedQueryExpectedRowColumnComparisonType = errors.New("unknown workflow saved query expected row column comparison type")

type WorkflowSavedQueryExpectedRowColumnComparisonType int

const (
	WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified WorkflowSavedQueryExpectedRowColumnComparisonType = iota
	WorkflowSavedQueryExpectedRowColumnComparisonTypeEquals
	WorkflowSavedQueryExpectedRowColumnComparisonTypeNotEquals
	WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThan
	WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThan
	WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThanOrEqual
	WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThanOrEqual
)

type WorkflowSavedQueryExpectedRowColumnResultId string

type WorkflowSavedQueryExpectedRowColumnResult struct {
	Id                                *WorkflowSavedQueryExpectedRowColumnResultId      `db:"id"`
	WorkflowSavedQueryStepIdFK        *WorkflowSavedQueryStepId                         `db:"saved_query_step_id_fk"`
	ExpectedRowCountComparisonType    WorkflowSavedQueryExpectedRowColumnComparisonType `db:"expected_row_comparison_type"`
	ExpectedColumnCountComparisonType WorkflowSavedQueryExpectedRowColumnComparisonType `db:"expected_column_comparison_type"`
	ExpectedRowCount                  int64                                             `db:"expected_row_count"`
	ExpectedColumnCount               int64                                             `db:"expected_column_count"`
	CreatedAt                         time.Time                                         `db:"created_at"`
	UpdateAt                          time.Time                                         `db:"updated_at"`
}

func toWorkflowSavedQueryExpectedRowColumnComparisonResultType(t int) (WorkflowSavedQueryExpectedRowColumnComparisonType, error) {
	switch t {
	case int(WorkflowSavedQueryExpectedRowColumnComparisonTypeEquals):
		return WorkflowSavedQueryExpectedRowColumnComparisonTypeEquals, nil
	case int(WorkflowSavedQueryExpectedRowColumnComparisonTypeNotEquals):
		return WorkflowSavedQueryExpectedRowColumnComparisonTypeNotEquals, nil
	case int(WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThan):
		return WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThan, nil
	case int(WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThan):
		return WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThan, nil
	case int(WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThanOrEqual):
		return WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThanOrEqual, nil
	case int(WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThanOrEqual):
		return WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThanOrEqual, nil
	case int(WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified):
		return WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified, nil
	default:
		return WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified, ErrUnknownWorkflowSavedQueryExpectedRowColumnComparisonType
	}
}
