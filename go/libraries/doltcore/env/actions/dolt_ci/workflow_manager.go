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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const (
	doltCITimeFormat = "2006-01-02 15:04:05"
)

var ErrWorkflowNameIsNil = errors.New("workflow name is nil")
var ErrWorkflowNotFound = errors.New("workflow not found")
var ErrMultipleWorkflowsFound = errors.New("multiple workflows found")

var ExpectedDoltCITablesOrdered = []doltdb.TableName{
	doltdb.TableName{Name: doltdb.WorkflowsTableName},
	doltdb.TableName{Name: doltdb.WorkflowEventsTableName},
	doltdb.TableName{Name: doltdb.WorkflowEventTriggersTableName},
	doltdb.TableName{Name: doltdb.WorkflowEventTriggerBranchesTableName},
	doltdb.TableName{Name: doltdb.WorkflowEventTriggerActivitiesTableName},
	doltdb.TableName{Name: doltdb.WorkflowJobsTableName},
	doltdb.TableName{Name: doltdb.WorkflowStepsTableName},
	doltdb.TableName{Name: doltdb.WorkflowSavedQueryStepsTableName},
	doltdb.TableName{Name: doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName},
}

type QueryFunc func(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, *sql.QueryFlags, error)

type WorkflowManager interface {
	StoreAndCommit(ctx *sql.Context, db sqle.Database, config *WorkflowConfig) error
}

type doltWorkflowManager struct {
	commiterName  string
	commiterEmail string
	queryFunc     QueryFunc
}

var _ WorkflowManager = &doltWorkflowManager{}

func NewWorkflowManager(commiterName, commiterEmail string, queryFunc QueryFunc) *doltWorkflowManager {
	return &doltWorkflowManager{
		commiterName:  commiterName,
		commiterEmail: commiterEmail,
		queryFunc:     queryFunc,
	}
}

// selects

func (d *doltWorkflowManager) selectAllFromWorkflowsTableQuery() string {
	return fmt.Sprintf("select * from %s;", doltdb.WorkflowsTableName)
}

func (d *doltWorkflowManager) selectOneFromWorkflowsTableQuery(workflowName string) string {
	return fmt.Sprintf("select * from %s where name = '%s' limit 1;", doltdb.WorkflowsTableName, workflowName)
}

func (d *doltWorkflowManager) selectAllFromWorkflowEventsTableByWorkflowNameQuery(workflowName string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsWorkflowNameFkColName, workflowName)
}

func (d *doltWorkflowManager) selectAllFromWorkflowJobsTableByWorkflowNameQuery(workflowName string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowJobsTableName, doltdb.WorkflowJobsWorkflowNameFkColName, workflowName)
}

func (d *doltWorkflowManager) selectAllFromSavedQueryStepExpectedRowColumnResultsTableBySavedQueryStepIdQuery(savedQueryStepID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s' limit 1;", doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName, savedQueryStepID)
}

func (d *doltWorkflowManager) selectAllFromSavedQueryStepsTableByWorkflowStepIdQuery(stepID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s' limit 1;", doltdb.WorkflowSavedQueryStepsTableName, doltdb.WorkflowSavedQueryStepsWorkflowStepIdFkColName, stepID)
}

func (d *doltWorkflowManager) selectAllFromWorkflowStepsTableByWorkflowJobIdQuery(jobID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s'", doltdb.WorkflowStepsTableName, doltdb.WorkflowStepsWorkflowJobIdFkColName, jobID)
}

func (d *doltWorkflowManager) selectAllFromWorkflowEventTriggersTableByWorkflowEventIdQuery(eventID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName, eventID)
}

func (d *doltWorkflowManager) selectAllFromWorkflowEventTriggerBranchesTableByEventTriggerIdQuery(triggerID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventTriggerBranchesTableName, doltdb.WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName, triggerID)
}

func (d *doltWorkflowManager) selectAllFromWorkflowEventTriggerActivitiesTableByEventTriggerIdQuery(triggerID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventTriggerActivitiesTableName, doltdb.WorkflowEventTriggerActivitiesWorkflowEventTriggersIdFkColName, triggerID)
}

func (d *doltWorkflowManager) insertIntoWorkflowsTableQuery(workflowName string) (string, string) {
	return workflowName, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`) values ('%s', now(), now());", doltdb.WorkflowsTableName, doltdb.WorkflowsNameColName, doltdb.WorkflowsCreatedAtColName, doltdb.WorkflowsUpdatedAtColName, workflowName)
}

func (d *doltWorkflowManager) insertIntoWorkflowEventsTableQuery(workflowName string, eventType int) (string, string) {
	eventID := uuid.NewString()
	return eventID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`) values ('%s', '%s', %d);", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsIdPkColName, doltdb.WorkflowEventsWorkflowNameFkColName, doltdb.WorkflowEventsEventTypeColName, eventID, workflowName, eventType)
}

func (d *doltWorkflowManager) insertIntoWorkflowEventTriggersTableQuery(eventID string, triggerType int) (string, string) {
	triggerID := uuid.NewString()
	return triggerID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`) values ('%s', '%s', %d);", doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersIdPkColName, doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName, doltdb.WorkflowEventTriggersEventTriggerTypeColName, triggerID, eventID, triggerType)
}

func (d *doltWorkflowManager) insertIntoWorkflowEventTriggerBranchesTableQuery(triggerID, branch string) (string, string) {
	branchID := uuid.NewString()
	return branchID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`) values ('%s', '%s', '%s');", doltdb.WorkflowEventTriggerBranchesTableName, doltdb.WorkflowEventTriggerBranchesIdPkColName, doltdb.WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName, doltdb.WorkflowEventTriggerBranchesBranchColName, branchID, triggerID, branch)
}

func (d *doltWorkflowManager) insertIntoWorkflowEventTriggerActivitiesTableQuery(triggerID, activity string) (string, string) {
	activityID := uuid.NewString()
	return activityID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`) values ('%s', '%s', '%s');", doltdb.WorkflowEventTriggerActivitiesTableName, doltdb.WorkflowEventTriggerActivitiesIdPkColName, doltdb.WorkflowEventTriggerActivitiesWorkflowEventTriggersIdFkColName, doltdb.WorkflowEventTriggerActivitiesActivityColName, activityID, triggerID, activity)
}

func (d *doltWorkflowManager) insertIntoWorkflowJobsTableQuery(jobName, workflowName string) (string, string) {
	jobID := uuid.NewString()
	return jobID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`, `%s`, `%s`) values ('%s', '%s', '%s', now(), now());", doltdb.WorkflowJobsTableName, doltdb.WorkflowJobsIdPkColName, doltdb.WorkflowJobsNameColName, doltdb.WorkflowJobsWorkflowNameFkColName, doltdb.WorkflowJobsCreatedAtColName, doltdb.WorkflowJobsUpdatedAtColName, jobID, jobName, workflowName)
}

func (d *doltWorkflowManager) insertIntoWorkflowStepsTableQuery(stepName, jobID string, stepOrder, stepType int) (string, string) {
	stepID := uuid.NewString()
	return stepID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`, `%s`, `%s`, `%s`, `%s`) values ('%s', '%s', '%s', %d, %d, now(), now());", doltdb.WorkflowStepsTableName, doltdb.WorkflowStepsIdPkColName, doltdb.WorkflowStepsNameColName, doltdb.WorkflowStepsWorkflowJobIdFkColName, doltdb.WorkflowStepsStepOrderColName, doltdb.WorkflowStepsStepTypeColName, doltdb.WorkflowStepsCreatedAtColName, doltdb.WorkflowStepsUpdatedAtColName, stepID, stepName, jobID, stepOrder, stepType)
}

func (d *doltWorkflowManager) insertIntoWorkflowSavedQueryStepsTableQuery(savedQueryName, stepID string, expectedResultsType int) (string, string) {
	savedQueryStepID := uuid.NewString()
	return savedQueryStepID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`, `%s`) values ('%s', '%s', '%s', %d);", doltdb.WorkflowSavedQueryStepsTableName, doltdb.WorkflowSavedQueryStepsIdPkColName, doltdb.WorkflowSavedQueryStepsWorkflowStepIdFkColName, doltdb.WorkflowSavedQueryStepsSavedQueryNameColName, doltdb.WorkflowSavedQueryStepsExpectedResultsTypeColName, savedQueryStepID, stepID, savedQueryName, expectedResultsType, stepID)
}

func (d *doltWorkflowManager) insertIntoWorkflowSavedQueryStepExpectedRowColumnResultsTableQuery(savedQueryStepID string, expectedColumnComparisonType, expectedRowComparisonType int, expectedColumnCount, expectedRowCount int64) (string, string) {
	expectedResultID := uuid.NewString()
	return expectedResultID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`,`%s`, `%s`, `%s`, `%s`, `%s`) values ('%s', '%s', %d, %d, %d, %d, now(), now());", doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsIdPkColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountComparisonTypeColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountComparisonTypeColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsCreatedAtColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsUpdatedAtColName, expectedResultID, savedQueryStepID, expectedColumnComparisonType, expectedRowComparisonType, expectedColumnCount, expectedRowCount)
}

// updates

func (d *doltWorkflowManager) updateWorkflowJobsTableQuery(jobID, jobName string) string {
	return fmt.Sprintf("update %s set `%s` = '%s', `%s` = now(), where `%s` = '%s';", doltdb.WorkflowJobsTableName, doltdb.WorkflowJobsNameColName, jobName, doltdb.WorkflowJobsUpdatedAtColName, doltdb.WorkflowJobsIdPkColName, jobID)
}

func (d *doltWorkflowManager) updateWorkflowStepsTableQuery(stepID, stepName string) string {
	return fmt.Sprintf("update %s set `%s` = '%s', `%s` = now(), where `%s` = '%s';", doltdb.WorkflowStepsTableName, doltdb.WorkflowStepsNameColName, stepName, doltdb.WorkflowStepsUpdatedAtColName, doltdb.WorkflowStepsIdPkColName, stepID)
}

func (d *doltWorkflowManager) updateWorkflowSavedQueryStepsExpectedRowColumnResultsTableQuery(expectedResultID, expectedColumnComparisonType, expectedRowComparisonType int, expectedColumnCount, expectedRowCount int64) string {
	return fmt.Sprintf("update %s set `%s` = '%d', `%s` = '%d', `%s` = '%d', `%s` = '%d', `%s` = now(), where `%s` = '%s';", doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountComparisonTypeColName, expectedColumnComparisonType, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountComparisonTypeColName, expectedRowComparisonType, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountColName, expectedColumnCount, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountColName, expectedRowCount, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsUpdatedAtColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsIdPkColName, expectedResultID)
}

// deletes

func (d *doltWorkflowManager) deleteFromWorkflowsTableByWorkflowNameQuery(workflowName string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowsTableName, doltdb.WorkflowsNameColName, workflowName)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventsTableByWorkflowNameQuery(workflowName string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsWorkflowNameFkColName, workflowName)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventsTableByWorkflowEventIdQuery(eventId string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsIdPkColName, eventId)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventTriggersTableByWorkflowEventTriggerIdQuery(triggerID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersIdPkColName, triggerID)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventTriggerBranchesTableByEventTriggerBranchIdQuery(branchID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowEventTriggerBranchesTableName, doltdb.WorkflowEventTriggerBranchesIdPkColName, branchID)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventTriggerActivitiesTableByEventTriggerActivityIdQuery(activityID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowEventTriggerActivitiesTableName, doltdb.WorkflowEventTriggerActivitiesIdPkColName, activityID)
}

func (d *doltWorkflowManager) deleteFromWorkflowJobsTableByWorkflowJobIdQuery(jobID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowJobsTableName, doltdb.WorkflowJobsIdPkColName, jobID)
}

func (d *doltWorkflowManager) deleteFromWorkflowStepsTableByWorkflowStepIdQuery(stepID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowStepsTableName, doltdb.WorkflowStepsIdPkColName, stepID)
}

func (d *doltWorkflowManager) deleteFromSavedQueryStepsTableByWorkflowStepIdQuery(savedQueryStepID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowSavedQueryStepsTableName, doltdb.WorkflowSavedQueryStepsIdPkColName, savedQueryStepID)
}

func (d *doltWorkflowManager) deleteFromSavedQueryStepExpectedRowColumnResultsTableBySavedQueryStepIdQuery(savedQueryStepID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName, savedQueryStepID)
}

func (d *doltWorkflowManager) newWorkflow(cvs ColumnValues) (*Workflow, error) {
	wf := &Workflow{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowsCreatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			wf.CreatedAt = t
		case doltdb.WorkflowsUpdatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			wf.UpdatedAt = t
		case doltdb.WorkflowsNameColName:
			name := WorkflowName(cv.Value)
			wf.Name = &name
		default:
			return nil, errors.New(fmt.Sprintf("unknown workflows column: %s", cv.ColumnName))
		}
	}

	return wf, nil
}

func (d *doltWorkflowManager) newWorkflowEvent(cvs ColumnValues) (*WorkflowEvent, error) {
	we := &WorkflowEvent{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowEventsIdPkColName:
			id := WorkflowEventId(cv.Value)
			we.Id = &id
		case doltdb.WorkflowEventsEventTypeColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}
			t, err := toWorkflowEventType(i)
			if err != nil {
				return nil, err
			}
			we.EventType = t
		case doltdb.WorkflowEventsWorkflowNameFkColName:
			name := WorkflowName(cv.Value)
			we.WorkflowNameFK = &name
		default:
			return nil, errors.New(fmt.Sprintf("unknown workflow events column: %s", cv.ColumnName))
		}
	}

	return we, nil
}

func (d *doltWorkflowManager) newWorkflowJob(cvs ColumnValues) (*WorkflowJob, error) {
	wj := &WorkflowJob{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowJobsIdPkColName:
			id := WorkflowJobId(cv.Value)
			wj.Id = &id
		case doltdb.WorkflowJobsNameColName:
			wj.Name = cv.Value
		case doltdb.WorkflowJobsCreatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			wj.CreatedAt = t
		case doltdb.WorkflowJobsUpdatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			wj.UpdateAt = t
		case doltdb.WorkflowJobsWorkflowNameFkColName:
			name := WorkflowName(cv.Value)
			wj.WorkflowNameFK = &name
		default:
			return nil, errors.New(fmt.Sprintf("unknown workflow jobs column: %s", cv.ColumnName))
		}
	}

	return wj, nil
}

func (d *doltWorkflowManager) newWorkflowSavedQueryStepExpectedRowColumnResult(cvs ColumnValues) (*WorkflowSavedQueryExpectedRowColumnResult, error) {
	r := &WorkflowSavedQueryExpectedRowColumnResult{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsIdPkColName:
			id := WorkflowSavedQueryExpectedRowColumnResultId(cv.Value)
			r.Id = &id
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName:
			id := WorkflowSavedQueryStepId(cv.Value)
			r.WorkflowSavedQueryStepIdFK = &id
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountComparisonTypeColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}
			t, err := toWorkflowSavedQueryExpectedRowColumnComparisonResultType(i)
			if err != nil {
				return nil, err
			}
			r.ExpectedRowCountComparisonType = t
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountComparisonTypeColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}
			t, err := toWorkflowSavedQueryExpectedRowColumnComparisonResultType(i)
			if err != nil {
				return nil, err
			}
			r.ExpectedColumnCountComparisonType = t
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}
			r.ExpectedRowCount = int64(i)
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}
			r.ExpectedColumnCount = int64(i)
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsCreatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			r.CreatedAt = t
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsUpdatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			r.UpdateAt = t
		default:
			return nil, errors.New(fmt.Sprintf("unknown saved query expected row column results column: %s", cv.ColumnName))
		}
	}

	return r, nil
}

func (d *doltWorkflowManager) newWorkflowSavedQueryStep(cvs ColumnValues) (*WorkflowSavedQueryStep, error) {
	sq := &WorkflowSavedQueryStep{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowSavedQueryStepsIdPkColName:
			id := WorkflowSavedQueryStepId(cv.Value)
			sq.Id = &id
		case doltdb.WorkflowSavedQueryStepsSavedQueryNameColName:
			sq.SavedQueryName = cv.Value
		case doltdb.WorkflowSavedQueryStepsWorkflowStepIdFkColName:
			id := WorkflowStepId(cv.Value)
			sq.WorkflowStepIdFK = &id
		case doltdb.WorkflowSavedQueryStepsExpectedResultsTypeColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}

			t, err := toWorkflowSavedQueryExpectedResultsType(i)
			if err != nil {
				return nil, err
			}

			sq.SavedQueryExpectedResultsType = t
		default:
			return nil, errors.New(fmt.Sprintf("unknown saved query step column: %s", cv.ColumnName))
		}
	}

	return sq, nil
}

func (d *doltWorkflowManager) newWorkflowStep(cvs ColumnValues) (*WorkflowStep, error) {
	ws := &WorkflowStep{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowStepsIdPkColName:
			id := WorkflowStepId(cv.Value)
			ws.Id = &id
		case doltdb.WorkflowStepsNameColName:
			ws.Name = cv.Value
		case doltdb.WorkflowStepsWorkflowJobIdFkColName:
			id := WorkflowJobId(cv.Value)
			ws.WorkflowJobIdFK = &id
		case doltdb.WorkflowStepsStepOrderColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}

			ws.StepOrder = i
		case doltdb.WorkflowStepsStepTypeColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}

			t, err := toWorkflowStepType(i)
			if err != nil {
				return nil, err
			}

			ws.StepType = t
		case doltdb.WorkflowStepsCreatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			ws.CreatedAt = t
		case doltdb.WorkflowStepsUpdatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			ws.UpdatedAt = t
		default:
			return nil, errors.New(fmt.Sprintf("unknown workflow step column: %s", cv.ColumnName))
		}
	}

	return ws, nil
}

func (d *doltWorkflowManager) newWorkflowEventTrigger(cvs ColumnValues) (*WorkflowEventTrigger, error) {
	et := &WorkflowEventTrigger{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowEventTriggersIdPkColName:
			id := WorkflowEventTriggerId(cv.Value)
			et.Id = &id
		case doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName:
			id := WorkflowEventId(cv.Value)
			et.WorkflowEventIdFK = &id
		case doltdb.WorkflowEventTriggersEventTriggerTypeColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}
			t, err := toWorkflowEventTriggerType(i)
			if err != nil {
				return nil, err
			}
			et.EventTriggerType = t
		default:
			return nil, errors.New(fmt.Sprintf("unknown dworkflow event triggers column: %s", cv.ColumnName))
		}
	}

	return et, nil
}

func (d *doltWorkflowManager) newWorkflowEventTriggerBranch(cvs ColumnValues) (*WorkflowEventTriggerBranch, error) {
	tb := &WorkflowEventTriggerBranch{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowEventTriggerBranchesIdPkColName:
			id := WorkflowEventTriggerBranchId(cv.Value)
			tb.Id = &id
		case doltdb.WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName:
			id := WorkflowEventTriggerId(cv.Value)
			tb.WorkflowEventTriggerIdFk = &id
		case doltdb.WorkflowEventTriggerBranchesBranchColName:
			tb.Branch = cv.Value
		default:
			return nil, errors.New(fmt.Sprintf("unknown workflow event trigger branches column: %s", cv.ColumnName))
		}
	}

	return tb, nil
}

func (d *doltWorkflowManager) newWorkflowEventTriggerActivity(cvs ColumnValues) (*WorkflowEventTriggerActivity, error) {
	ta := &WorkflowEventTriggerActivity{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowEventTriggerActivitiesIdPkColName:
			id := WorkflowEventTriggerActivityId(cv.Value)
			ta.Id = &id
		case doltdb.WorkflowEventTriggerActivitiesWorkflowEventTriggersIdFkColName:
			id := WorkflowEventTriggerId(cv.Value)
			ta.WorkflowEventTriggerIdFk = &id
		case doltdb.WorkflowEventTriggerActivitiesActivityColName:
			ta.Activity = cv.Value
		default:
			return nil, errors.New(fmt.Sprintf("unknown workflow event trigger activities column: %s", cv.ColumnName))
		}
	}

	return ta, nil
}

func (d *doltWorkflowManager) validateWorkflowTables(ctx *sql.Context) error {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	_, exists := dSess.GetDoltDB(ctx, dbName)
	if !exists {
		return fmt.Errorf("database not found in database %s", dbName)
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return fmt.Errorf("roots not found in database %s", dbName)
	}

	root := roots.Working

	tables, err := root.GetTableNames(ctx, doltdb.DefaultSchemaName)
	if err != nil {
		return err
	}

	tableMap := make(map[string]struct{})
	for _, table := range tables {
		if doltdb.IsDoltCITable(table) {
			tableMap[table] = struct{}{}
		}
	}

	for _, t := range ExpectedDoltCITablesOrdered {
		_, ok := tableMap[t.Name]
		if !ok {
			return errors.New(fmt.Sprintf("expected workflow table not found: %s", t))
		}
	}

	return nil
}

func (d *doltWorkflowManager) commitWorkflow(ctx *sql.Context, workflowName string) error {
	return d.sqlWriteQuery(ctx, fmt.Sprintf("CALL DOLT_COMMIT('-Am' 'Successfully stored workflow: %s', '--author', '%s <%s>');", workflowName, d.commiterName, d.commiterEmail))
}

func (d *doltWorkflowManager) sqlWriteQuery(ctx *sql.Context, query string) error {
	_, rowIter, _, err := d.queryFunc(ctx, query)
	if err != nil {
		return err
	}
	_, err = sql.RowIterToRows(ctx, rowIter)
	return err
}

func (d *doltWorkflowManager) sqlReadQuery(ctx *sql.Context, query string, cb func(ctx *sql.Context, cvs ColumnValues) error) error {
	sch, rowIter, _, err := d.queryFunc(ctx, query)
	if err != nil {
		return err
	}

	rows, err := sql.RowIterToRows(ctx, rowIter)
	if err != nil {
		return err
	}

	size := len(sch)
	for _, row := range rows {

		cvs := make(ColumnValues, size)

		for i := range size {
			col := sch[i]
			val := row[i]
			cv, err := NewColumnValue(col, val)
			if err != nil {
				return err
			}
			cvs[i] = cv
		}

		err = cb(ctx, cvs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *doltWorkflowManager) getWorkflowSavedQueryExpectedRowColumnResultBySavedQueryStepId(ctx *sql.Context, sqsID WorkflowSavedQueryStepId) (*WorkflowSavedQueryExpectedRowColumnResult, error) {
	query := d.selectAllFromSavedQueryStepExpectedRowColumnResultsTableBySavedQueryStepIdQuery(string(sqsID))
	workflowSavedQueryExpectedResults, err := d.retrieveWorkflowSavedQueryExpectedRowColumnResults(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(workflowSavedQueryExpectedResults) < 1 {
		return nil, nil
	}
	if len(workflowSavedQueryExpectedResults) > 1 {
		return nil, errors.New(fmt.Sprintf("expected no more than one row column result for saved query step: %s", sqsID))
	}
	return workflowSavedQueryExpectedResults[0], nil
}

func (d *doltWorkflowManager) getWorkflowSavedQueryStepsByStepId(ctx *sql.Context, stepID WorkflowStepId) (*WorkflowSavedQueryStep, error) {
	query := d.selectAllFromSavedQueryStepsTableByWorkflowStepIdQuery(string(stepID))
	savedQuerySteps, err := d.retrieveWorkflowSavedQuerySteps(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(savedQuerySteps) < 1 {
		return nil, nil
	}
	if len(savedQuerySteps) > 1 {
		return nil, errors.New(fmt.Sprintf("expected no more than one saved query step for step: %s", stepID))
	}
	return savedQuerySteps[0], nil
}

func (d *doltWorkflowManager) listWorkflowStepsByJobId(ctx *sql.Context, jobID WorkflowJobId) ([]*WorkflowStep, error) {
	query := d.selectAllFromWorkflowStepsTableByWorkflowJobIdQuery(string(jobID))
	return d.retrieveWorkflowSteps(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowJobsByWorkflowName(ctx *sql.Context, workflowName string) ([]*WorkflowJob, error) {
	query := d.selectAllFromWorkflowJobsTableByWorkflowNameQuery(string(workflowName))
	return d.retrieveWorkflowJobs(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventTriggerActivitiesByEventTriggerId(ctx *sql.Context, triggerID WorkflowEventTriggerId) ([]*WorkflowEventTriggerActivity, error) {
	query := d.selectAllFromWorkflowEventTriggerActivitiesTableByEventTriggerIdQuery(string(triggerID))
	return d.retrieveWorkflowEventTriggerActivities(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventTriggersByEventId(ctx *sql.Context, eventID WorkflowEventId) ([]*WorkflowEventTrigger, error) {
	query := d.selectAllFromWorkflowEventTriggersTableByWorkflowEventIdQuery(string(eventID))
	return d.retrieveWorkflowEventTriggers(ctx, query)

}

func (d *doltWorkflowManager) listWorkflowEventsByWorkflowName(ctx *sql.Context, workflowName string) ([]*WorkflowEvent, error) {
	query := d.selectAllFromWorkflowEventsTableByWorkflowNameQuery(string(workflowName))
	return d.retrieveWorkflowEvent(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventTriggerBranchesByEventTriggerId(ctx *sql.Context, triggerID WorkflowEventTriggerId) ([]*WorkflowEventTriggerBranch, error) {
	query := d.selectAllFromWorkflowEventTriggerBranchesTableByEventTriggerIdQuery(string(triggerID))
	return d.retrieveWorkflowEventTriggerBranches(ctx, query)
}

func (d *doltWorkflowManager) retrieveWorkflowSavedQueryExpectedRowColumnResults(ctx *sql.Context, query string) ([]*WorkflowSavedQueryExpectedRowColumnResult, error) {
	workflowSavedQueryExpectedResults := make([]*WorkflowSavedQueryExpectedRowColumnResult, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
		er, rerr := d.newWorkflowSavedQueryStepExpectedRowColumnResult(cvs)
		if rerr != nil {
			return rerr
		}

		workflowSavedQueryExpectedResults = append(workflowSavedQueryExpectedResults, er)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowSavedQueryExpectedResults, nil
}

func (d *doltWorkflowManager) retrieveWorkflowSavedQuerySteps(ctx *sql.Context, query string) ([]*WorkflowSavedQueryStep, error) {
	workflowSavedQuerySteps := make([]*WorkflowSavedQueryStep, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
		sq, rerr := d.newWorkflowSavedQueryStep(cvs)
		if rerr != nil {
			return rerr
		}

		workflowSavedQuerySteps = append(workflowSavedQuerySteps, sq)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowSavedQuerySteps, nil
}

func (d *doltWorkflowManager) retrieveWorkflowSteps(ctx *sql.Context, query string) ([]*WorkflowStep, error) {
	workflowSteps := make([]*WorkflowStep, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
		s, rerr := d.newWorkflowStep(cvs)
		if rerr != nil {
			return rerr
		}

		workflowSteps = append(workflowSteps, s)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowSteps, nil
}

func (d *doltWorkflowManager) retrieveWorkflowJobs(ctx *sql.Context, query string) ([]*WorkflowJob, error) {
	workflowJobs := make([]*WorkflowJob, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
		j, rerr := d.newWorkflowJob(cvs)
		if rerr != nil {
			return rerr
		}
		workflowJobs = append(workflowJobs, j)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowJobs, nil
}

func (d *doltWorkflowManager) retrieveWorkflowEventTriggerActivities(ctx *sql.Context, query string) ([]*WorkflowEventTriggerActivity, error) {
	workflowEventTriggerActivities := make([]*WorkflowEventTriggerActivity, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
		a, rerr := d.newWorkflowEventTriggerActivity(cvs)
		if rerr != nil {
			return rerr
		}
		workflowEventTriggerActivities = append(workflowEventTriggerActivities, a)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowEventTriggerActivities, nil
}

func (d *doltWorkflowManager) retrieveWorkflowEventTriggerBranches(ctx *sql.Context, query string) ([]*WorkflowEventTriggerBranch, error) {
	workflowEventTriggerBranches := make([]*WorkflowEventTriggerBranch, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
		b, rerr := d.newWorkflowEventTriggerBranch(cvs)
		if rerr != nil {
			return rerr
		}

		workflowEventTriggerBranches = append(workflowEventTriggerBranches, b)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowEventTriggerBranches, nil
}

func (d *doltWorkflowManager) retrieveWorkflowEventTriggers(ctx *sql.Context, query string) ([]*WorkflowEventTrigger, error) {
	workflowEventTriggers := make([]*WorkflowEventTrigger, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
		wet, rerr := d.newWorkflowEventTrigger(cvs)
		if rerr != nil {
			return rerr
		}
		workflowEventTriggers = append(workflowEventTriggers, wet)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowEventTriggers, nil
}

func (d *doltWorkflowManager) retrieveWorkflowEvent(ctx *sql.Context, query string) ([]*WorkflowEvent, error) {
	workflowEvents := make([]*WorkflowEvent, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
		we, rerr := d.newWorkflowEvent(cvs)
		if rerr != nil {
			return rerr
		}

		workflowEvents = append(workflowEvents, we)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowEvents, nil
}

func (d *doltWorkflowManager) retrieveWorkflows(ctx *sql.Context, query string) ([]*Workflow, error) {
	workflows := make([]*Workflow, 0)
	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
		wf, rerr := d.newWorkflow(cvs)
		if rerr != nil {
			return rerr
		}
		workflows = append(workflows, wf)
		return nil
	}
	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}
	return workflows, nil
}

func (d *doltWorkflowManager) getWorkflow(ctx *sql.Context, workflowName string) (*Workflow, error) {
	query := d.selectOneFromWorkflowsTableQuery(string(workflowName))

	workflows, err := d.retrieveWorkflows(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(workflows) == 0 {
		return nil, ErrWorkflowNotFound
	}
	if len(workflows) > 1 {
		return nil, ErrMultipleWorkflowsFound
	}
	return workflows[0], nil
}

func (d *doltWorkflowManager) updateExistingWorkflow(ctx *sql.Context, config *WorkflowConfig) error {
	// todo: update events to match config
	// handle deletes
	if config.On.Push == nil {
		// todo: delete all push events for this workflow
	}
	if config.On.PullRequest == nil {
		// todo: delete all pull request events for this workflow
	}
	if config.On.WorkflowDispatch == nil {
		// todo: delete all workflow dispatch events for this workflow
	}

	// handle push
	if config.On.Push != nil {
		if len(config.On.Push.Branches) == 0 {
			// todo: delete all push events and triggers and trigger branches for this workflow
		} else {
			// todo: create a map of config branches
			// todo: list all trigger branches for this workflow push event
			// todo: iterate all branches
			// todo: for each branch, check config map
			// todo: if not in config map, delete branch in db
			// todo: if is in config map, delete from config map
			// todo: after loop, if any branches are left in the config map
			// todo: create those trigger branches
		}
	}
	
	if config.On.PullRequest != nil {
		if len(config.On.PullRequest.Branches) == 0 {
			// todo: delete all pull request events and triggers and branches for this workflow
		}
		if len(config.On.PullRequest.Activities) == 0 {
			// todo: delete all pull request events and triggers and activities for this workflow
		}
	}

	// todo: update triggers to match config
	// todo: update t branches to match config
	// todo: update t activities to match config

	// todo: update jobs to match config
	// todo: update steps to match config
	// todo: update sqs to match config
	// todo: update results to match config
	return nil
}

func (d *doltWorkflowManager) writeWorkflowRow(ctx *sql.Context, workflowName WorkflowName) (WorkflowName, error) {
	wn, query := d.insertIntoWorkflowsTableQuery(string(workflowName))
	err := d.sqlWriteQuery(ctx, query)
	if err != nil {
		return "", err
	}
	return WorkflowName(wn), nil
}

func (d *doltWorkflowManager) writeWorkflowEventRow(ctx *sql.Context, workflowName WorkflowName, eventType WorkflowEventType) (WorkflowEventId, error) {
	eventID, query := d.insertIntoWorkflowEventsTableQuery(string(workflowName), int(eventType))
	err := d.sqlWriteQuery(ctx, query)
	if err != nil {
		return "", err
	}
	return WorkflowEventId(eventID), nil
}

func (d *doltWorkflowManager) writeWorkflowEventTriggerRow(ctx *sql.Context, eventID WorkflowEventId, triggerType WorkflowEventTriggerType) (WorkflowEventTriggerId, error) {
	triggerID, query := d.insertIntoWorkflowEventTriggersTableQuery(string(eventID), int(triggerType))
	err := d.sqlWriteQuery(ctx, query)
	if err != nil {
		return "", err
	}
	return WorkflowEventTriggerId(triggerID), nil
}

func (d *doltWorkflowManager) writeWorkflowEventTriggerBranchesRow(ctx *sql.Context, triggerID WorkflowEventTriggerId, branch string) (WorkflowEventTriggerBranchId, error) {
	branchID, query := d.insertIntoWorkflowEventTriggerBranchesTableQuery(string(triggerID), branch)
	err := d.sqlWriteQuery(ctx, query)
	if err != nil {
		return "", err
	}
	return WorkflowEventTriggerBranchId(branchID), nil
}

func (d *doltWorkflowManager) writeWorkflowEventTriggerActivitiesRow(ctx *sql.Context, triggerID WorkflowEventTriggerId, activity string) (WorkflowEventTriggerActivityId, error) {
	activityID, query := d.insertIntoWorkflowEventTriggerBranchesTableQuery(string(triggerID), activity)
	err := d.sqlWriteQuery(ctx, query)
	if err != nil {
		return "", err
	}
	return WorkflowEventTriggerActivityId(activityID), nil
}

func (d *doltWorkflowManager) writeWorkflowJobRow(ctx *sql.Context, workflowName WorkflowName, jobName string) (WorkflowJobId, error) {
	jobID, query := d.insertIntoWorkflowJobsTableQuery(jobName, string(workflowName))
	err := d.sqlWriteQuery(ctx, query)
	if err != nil {
		return "", err
	}
	return WorkflowJobId(jobID), nil
}

func (d *doltWorkflowManager) writeWorkflowStepRow(ctx *sql.Context, jobID WorkflowJobId, stepName string, stepOrder int, stepType WorkflowStepType) (WorkflowStepId, error) {
	stepID, query := d.insertIntoWorkflowStepsTableQuery(stepName, string(jobID), stepOrder, int(stepType))
	err := d.sqlWriteQuery(ctx, query)
	if err != nil {
		return "", err
	}
	return WorkflowStepId(stepID), nil
}

func (d *doltWorkflowManager) writeWorkflowSavedQueryStepRow(ctx *sql.Context, stepID WorkflowStepId, savedQueryName string, expectedResultType WorkflowSavedQueryExpectedResultsType) (WorkflowSavedQueryStepId, error) {
	savedQueryStepID, query := d.insertIntoWorkflowSavedQueryStepsTableQuery(savedQueryName, string(stepID), int(expectedResultType))
	err := d.sqlWriteQuery(ctx, query)
	if err != nil {
		return "", err
	}
	return WorkflowSavedQueryStepId(savedQueryStepID), nil
}

func (d *doltWorkflowManager) writeWorkflowSavedQueryStepExpectedRowColumnResultRow(ctx *sql.Context, savedQueryStepID WorkflowSavedQueryStepId, expectedColumnComparisonType, expectedRowComparisonType WorkflowSavedQueryExpectedRowColumnComparisonType, expectedColumnCount, expectedRowCount int64) (WorkflowSavedQueryExpectedRowColumnResultId, error) {
	resultID, query := d.insertIntoWorkflowSavedQueryStepExpectedRowColumnResultsTableQuery(string(savedQueryStepID), int(expectedColumnComparisonType), int(expectedRowComparisonType), expectedColumnCount, expectedRowCount)
	err := d.sqlWriteQuery(ctx, query)
	if err != nil {
		return "", err
	}
	return WorkflowSavedQueryExpectedRowColumnResultId(resultID), nil
}

func (d *doltWorkflowManager) parseSavedQueryExpectedResultString(str string) (WorkflowSavedQueryExpectedRowColumnComparisonType, int64, error) {
	parts := strings.Split(strings.TrimSpace(str), " ")
	if len(parts) == 1 {
		i, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		return WorkflowSavedQueryExpectedRowColumnComparisonTypeEquals, i, nil
	}
	if len(parts) == 2 {
		i, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		switch strings.TrimSpace(parts[0]) {
		case "==":
			return WorkflowSavedQueryExpectedRowColumnComparisonTypeEquals, i, nil
		case "!=":
			return WorkflowSavedQueryExpectedRowColumnComparisonTypeNotEquals, i, nil
		case ">":
			return WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThan, i, nil
		case ">=":
			return WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThanOrEqual, i, nil
		case "<":
			return WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThan, i, nil
		case "<=":
			return WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThanOrEqual, i, nil
		}
	}
	return 0, 0, fmt.Errorf("unable to parse comparison string: %s", str)
}

func (d *doltWorkflowManager) createWorkflow(ctx *sql.Context, config *WorkflowConfig) error {
	workflowName, err := d.writeWorkflowRow(ctx, WorkflowName(config.Name))
	if err != nil {
		return err
	}

	// insert into events
	// handle on push
	var pushEventID WorkflowEventId
	if config.On.Push != nil {
		pushEventID, err = d.writeWorkflowEventRow(ctx, workflowName, WorkflowEventTypePush)
		if err != nil {
			return err
		}
	}

	// handle on pull request
	var pullRequestEventID WorkflowEventId
	if config.On.PullRequest != nil {
		pullRequestEventID, err = d.writeWorkflowEventRow(ctx, workflowName, WorkflowEventTypePullRequest)
		if err != nil {
			return err
		}
	}

	// handle on workflow dispatch
	var workflowDispatchEventID WorkflowEventId
	if config.On.WorkflowDispatch != nil {
		workflowDispatchEventID, err = d.writeWorkflowEventRow(ctx, workflowName, WorkflowEventTypeWorkflowDispatch)
		if err != nil {
			return err
		}
	}

	// insert into triggers
	// handle push
	var pushBranchesTriggerEventID WorkflowEventTriggerId
	if pushEventID != "" {
		if len(config.On.Push.Branches) == 0 {
			// dont insert trigger rows for generic push events
		} else {
			pushBranchesTriggerEventID, err = d.writeWorkflowEventTriggerRow(ctx, pushEventID, WorkflowEventTriggerTypeBranches)
			if err != nil {
				return err
			}
		}
	}

	// handle pull request
	var pullRequestBranchesTriggerEventID WorkflowEventTriggerId
	var pullRequestActivitiesTriggerEventID WorkflowEventTriggerId
	if pullRequestEventID != "" {
		if len(config.On.PullRequest.Branches) == 0 && len(config.On.PullRequest.Activities) == 0 {
			// dont insert trigger rows for generic pull request events
		} else {
			if len(config.On.PullRequest.Branches) > 0 {
				pullRequestBranchesTriggerEventID, err = d.writeWorkflowEventTriggerRow(ctx, pullRequestEventID, WorkflowEventTriggerTypeBranches)
				if err != nil {
					return err
				}
			}
			if len(config.On.PullRequest.Activities) > 0 {
				pullRequestActivitiesTriggerEventID, err = d.writeWorkflowEventTriggerRow(ctx, pullRequestEventID, WorkflowEventTriggerTypeActivities)
				if err != nil {
					return err
				}
			}
		}
	}

	// handle workflow dispatch
	if workflowDispatchEventID != "" {
		if config.On.WorkflowDispatch != nil {
			_, err = d.writeWorkflowEventTriggerRow(ctx, workflowDispatchEventID, WorkflowEventTriggerTypeWorkflowDispatch)
			if err != nil {
				return err
			}
		}
	}

	// insert into trigger branches
	// handle pushes
	if pushBranchesTriggerEventID != "" {
		for _, branch := range config.On.Push.Branches {
			_, err = d.writeWorkflowEventTriggerBranchesRow(ctx, pushBranchesTriggerEventID, branch)
			if err != nil {
				return err
			}
		}
	}

	// handle pull requests
	if pullRequestBranchesTriggerEventID != "" {
		for _, branch := range config.On.PullRequest.Branches {
			_, err = d.writeWorkflowEventTriggerBranchesRow(ctx, pullRequestBranchesTriggerEventID, branch)
			if err != nil {
				return err
			}
		}
	}

	// insert into trigger activities
	// handle pull requests
	if pullRequestActivitiesTriggerEventID != "" {
		for _, activity := range config.On.PullRequest.Activities {
			_, err = d.writeWorkflowEventTriggerActivitiesRow(ctx, pullRequestActivitiesTriggerEventID, activity)
			if err != nil {
				return err
			}
		}
	}

	// handle jobs
	for _, job := range config.Jobs {
		// insert into jobs
		jobID, err := d.writeWorkflowJobRow(ctx, workflowName, job.Name)
		if err != nil {
			return err
		}

		// handle steps
		for idx, step := range job.Steps {
			// insert into step
			order := idx + 1

			var stepType WorkflowStepType
			if step.SavedQueryName != "" {
				stepType = WorkflowStepTypeSavedQuery
			}

			stepID, err := d.writeWorkflowStepRow(ctx, jobID, step.Name, order, stepType)
			if err != nil {
				return err
			}

			// insert into saved query steps
			if stepType == WorkflowStepTypeSavedQuery {
				savedQueryStepID, err := d.writeWorkflowSavedQueryStepRow(ctx, stepID, step.SavedQueryName, WorkflowSavedQueryExpectedResultsTypeRowColumnCount)
				if err != nil {
					return err
				}

				// insert into expected results
				expectedColumnComparisonType, expectedColumnCount, err := d.parseSavedQueryExpectedResultString(step.ExpectedColumns)
				if err != nil {
					return err
				}

				expectedRowComparisonType, expectedRowCount, err := d.parseSavedQueryExpectedResultString(step.ExpectedRows)
				if err != nil {
					return err
				}

				_, err = d.writeWorkflowSavedQueryStepExpectedRowColumnResultRow(ctx, savedQueryStepID, expectedColumnComparisonType, expectedRowComparisonType, expectedColumnCount, expectedRowCount)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *doltWorkflowManager) storeFromConfig(ctx *sql.Context, config *WorkflowConfig) error {
	_, err := d.getWorkflow(ctx, config.Name)
	if err != nil {
		if err == ErrWorkflowNotFound {
			return d.createWorkflow(ctx, config)
		}
		return err
	}
	return d.updateExistingWorkflow(ctx, config)
}

func (d *doltWorkflowManager) StoreAndCommit(ctx *sql.Context, db sqle.Database, config *WorkflowConfig) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	err := d.storeFromConfig(ctx, config)
	if err != nil {
		return err
	}

	return d.commitWorkflow(ctx, config.Name)
}
