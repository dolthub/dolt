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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const (
	doltCITimeFormat = "2006-01-02 15:04:05"
)

var ErrWorkflowNotFound = errors.New("workflow not found")
var ErrMultipleWorkflowsFound = errors.New("multiple workflows found")

type WorkflowManager interface {
	// RemoveWorkflow deletes a workflow from the database and creates a Dolt commit
	RemoveWorkflow(ctx *sql.Context, db sqle.Database, workflowName string) error
	// ListWorkflows lists all workflows in the database.
	ListWorkflows(ctx *sql.Context, db sqle.Database) ([]string, error)
	// GetWorkflowConfig returns the WorkflowConfig for a workflow by name.
	GetWorkflowConfig(ctx *sql.Context, db sqle.Database, workflowName string) (*WorkflowConfig, error)
	// StoreAndCommit creates or updates a workflow and creates a Dolt commit
	StoreAndCommit(ctx *sql.Context, db sqle.Database, config *WorkflowConfig) error
}

type doltWorkflowManager struct {
	commiterName  string
	commiterEmail string
	queryFunc     queryFunc
}

var _ WorkflowManager = &doltWorkflowManager{}

func NewWorkflowManager(commiterName, commiterEmail string, queryFunc queryFunc) *doltWorkflowManager {
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

func (d *doltWorkflowManager) selectAllFromWorkflowEventsTableByWorkflowNameWhereEventTypeIsPullRequestQuery(workflowName string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s' and `%s` = %d;", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsWorkflowNameFkColName, workflowName, doltdb.WorkflowEventsEventTypeColName, WorkflowEventTypePullRequest)
}

func (d *doltWorkflowManager) selectAllFromWorkflowEventsTableByWorkflowNameWhereEventTypeIsPushQuery(workflowName string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s' and `%s` = %d;", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsWorkflowNameFkColName, workflowName, doltdb.WorkflowEventsEventTypeColName, WorkflowEventTypePush)
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

func (d *doltWorkflowManager) selectAllFromWorkflowEventTriggersTableByWorkflowEventIdWhereEventTriggerTypeIsBranchesQuery(eventID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s' and `%s` = %d;", doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName, eventID, doltdb.WorkflowEventTriggersEventTriggerTypeColName, WorkflowEventTriggerTypeBranches)
}

func (d *doltWorkflowManager) selectAllFromWorkflowEventTriggersTableByWorkflowEventIdWhereEventTriggerTypeIsActivityQuery(eventID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s' and `%s` in (%d, %d, %d, %d);", doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName, eventID, doltdb.WorkflowEventTriggersEventTriggerTypeColName, WorkflowEventTriggerTypeActivityOpened, WorkflowEventTriggerTypeActivityClosed, WorkflowEventTriggerTypeActivityReopened, WorkflowEventTriggerTypeActivitySynchronized)
}

func (d *doltWorkflowManager) selectAllFromWorkflowEventTriggerBranchesTableByEventTriggerIdQuery(triggerID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventTriggerBranchesTableName, doltdb.WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName, triggerID)
}

// inserts

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
	return savedQueryStepID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`, `%s`) values ('%s', '%s', '%s', %d);", doltdb.WorkflowSavedQueryStepsTableName, doltdb.WorkflowSavedQueryStepsIdPkColName, doltdb.WorkflowSavedQueryStepsWorkflowStepIdFkColName, doltdb.WorkflowSavedQueryStepsSavedQueryNameColName, doltdb.WorkflowSavedQueryStepsExpectedResultsTypeColName, savedQueryStepID, stepID, savedQueryName, expectedResultsType)
}

func (d *doltWorkflowManager) insertIntoWorkflowSavedQueryStepExpectedRowColumnResultsTableQuery(savedQueryStepID string, expectedColumnComparisonType, expectedRowComparisonType int, expectedColumnCount, expectedRowCount int64) (string, string) {
	expectedResultID := uuid.NewString()
	return expectedResultID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`,`%s`, `%s`, `%s`, `%s`, `%s`) values ('%s', '%s', %d, %d, %d, %d, now(), now());", doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsIdPkColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountComparisonTypeColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountComparisonTypeColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsCreatedAtColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsUpdatedAtColName, expectedResultID, savedQueryStepID, expectedColumnComparisonType, expectedRowComparisonType, expectedColumnCount, expectedRowCount)
}

// updates

func (d *doltWorkflowManager) updateWorkflowJobsTableQuery(jobID, jobName string) string {
	return fmt.Sprintf("update %s set `%s` = '%s', `%s` = now(), where `%s` = '%s';", doltdb.WorkflowJobsTableName, doltdb.WorkflowJobsNameColName, jobName, doltdb.WorkflowJobsUpdatedAtColName, doltdb.WorkflowJobsIdPkColName, jobID)
}

func (d *doltWorkflowManager) updateWorkflowStepsTableQuery(stepID string, stepOrder int) string {
	return fmt.Sprintf("update %s set `%s` = %d, `%s` = now(), where `%s` = '%s';", doltdb.WorkflowStepsTableName, doltdb.WorkflowStepsStepOrderColName, stepOrder, doltdb.WorkflowStepsUpdatedAtColName, doltdb.WorkflowStepsIdPkColName, stepID)
}

func (d *doltWorkflowManager) updateWorkflowSavedQueryStepsTableQuery(savedQueryStepID, savedQueryName string, expectedResultsType int) string {
	return fmt.Sprintf("update %s set `%s` = '%s', `%s` = %d where `%s` = '%s';", doltdb.WorkflowSavedQueryStepsTableName, doltdb.WorkflowSavedQueryStepsSavedQueryNameColName, savedQueryName, doltdb.WorkflowSavedQueryStepsExpectedResultsTypeColName, expectedResultsType, doltdb.WorkflowSavedQueryStepsIdPkColName, savedQueryStepID)
}

func (d *doltWorkflowManager) updateWorkflowSavedQueryStepsExpectedRowColumnResultsTableQuery(expectedResultID string, expectedColumnComparisonType, expectedRowComparisonType int, expectedColumnCount, expectedRowCount int64) string {
	return fmt.Sprintf("update %s set `%s` = %d, `%s` = %d, `%s` = %d, `%s` = %d, `%s` = now() where `%s` = '%s';", doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountComparisonTypeColName, expectedColumnComparisonType, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountComparisonTypeColName, expectedRowComparisonType, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountColName, expectedColumnCount, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountColName, expectedRowCount, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsUpdatedAtColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsIdPkColName, expectedResultID)
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

func (d *doltWorkflowManager) deleteFromWorkflowEventsTableByWorkflowNameQueryWhereWorkflowEventTypeIsPush(workflowName string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s' and `%s` = %d;", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsWorkflowNameFkColName, workflowName, doltdb.WorkflowEventsEventTypeColName, WorkflowEventTypePush)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventsTableByWorkflowNameQueryWhereWorkflowEventTypeIsPullRequest(workflowName string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s' and `%s` = %d;", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsWorkflowNameFkColName, workflowName, doltdb.WorkflowEventsEventTypeColName, WorkflowEventTypePullRequest)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventsTableByWorkflowNameQueryWhereWorkflowEventTypeIsWorkflowDispatch(workflowName string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s' and `%s` = %d;", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsWorkflowNameFkColName, workflowName, doltdb.WorkflowEventsEventTypeColName, WorkflowEventTypeWorkflowDispatch)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventTriggersTableByWorkflowEventTriggerIdQuery(triggerID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersIdPkColName, triggerID)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventTriggerBranchesTableByEventTriggerBranchIdQuery(branchID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowEventTriggerBranchesTableName, doltdb.WorkflowEventTriggerBranchesIdPkColName, branchID)
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

func (d *doltWorkflowManager) newWorkflow(cvs columnValues) (*Workflow, error) {
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

func (d *doltWorkflowManager) newWorkflowEvent(cvs columnValues) (*WorkflowEvent, error) {
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

func (d *doltWorkflowManager) newWorkflowJob(cvs columnValues) (*WorkflowJob, error) {
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

func (d *doltWorkflowManager) newWorkflowSavedQueryStepExpectedRowColumnResult(cvs columnValues) (*WorkflowSavedQueryExpectedRowColumnResult, error) {
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

func (d *doltWorkflowManager) newWorkflowSavedQueryStep(cvs columnValues) (*WorkflowSavedQueryStep, error) {
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

func (d *doltWorkflowManager) newWorkflowStep(cvs columnValues) (*WorkflowStep, error) {
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

func (d *doltWorkflowManager) newWorkflowEventTrigger(cvs columnValues) (*WorkflowEventTrigger, error) {
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

func (d *doltWorkflowManager) newWorkflowEventTriggerBranch(cvs columnValues) (*WorkflowEventTriggerBranch, error) {
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

	for _, wrapt := range ExpectedDoltCITablesOrdered {
		if !wrapt.Deprecated {
			_, ok := tableMap[wrapt.TableName.Name]
			if !ok {
				return errors.New(fmt.Sprintf("expected workflow table not found: %s", wrapt.TableName.Name))
			}
		}
	}

	return nil
}

func (d *doltWorkflowManager) commitWorkflow(ctx *sql.Context, workflowName string) error {
	// stage table in reverse order so child tables
	// are staged before parent tables
	for i := len(ExpectedDoltCITablesOrdered) - 1; i >= 0; i-- {
		wrapt := ExpectedDoltCITablesOrdered[i]
		if !wrapt.Deprecated {
			err := d.sqlWriteQuery(ctx, fmt.Sprintf("CALL DOLT_ADD('%s');", wrapt.TableName.Name))
			if err != nil {
				return err
			}
		}
	}
	return d.sqlWriteQuery(ctx, fmt.Sprintf("CALL DOLT_COMMIT('-m' 'Successfully stored workflow: %s', '--author', '%s <%s>');", workflowName, d.commiterName, d.commiterEmail))
}

func (d *doltWorkflowManager) commitRemoveWorkflow(ctx *sql.Context, workflowName string) error {
	// stage table in reverse order so child tables
	// are staged before parent tables
	for i := len(ExpectedDoltCITablesOrdered) - 1; i >= 0; i-- {
		wrapt := ExpectedDoltCITablesOrdered[i]
		if !wrapt.Deprecated {
			err := d.sqlWriteQuery(ctx, fmt.Sprintf("CALL DOLT_ADD('%s');", wrapt.TableName.Name))
			if err != nil {
				return err
			}
		}
	}
	return d.sqlWriteQuery(ctx, fmt.Sprintf("CALL DOLT_COMMIT('-m' 'Successfully removed workflow: %s', '--author', '%s <%s>');", workflowName, d.commiterName, d.commiterEmail))
}

func (d *doltWorkflowManager) sqlWriteQuery(ctx *sql.Context, query string) error {
	return sqlWriteQuery(ctx, d.queryFunc, query)
}

func (d *doltWorkflowManager) sqlReadQuery(ctx *sql.Context, query string, cb func(ctx *sql.Context, cvs columnValues) error) error {
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

		cvs := make(columnValues, size)

		for i := range size {
			col := sch[i]
			val := row[i]
			cv, err := newColumnValue(col, val)
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

func (d *doltWorkflowManager) getWorkflowSavedQueryStepByStepId(ctx *sql.Context, stepID WorkflowStepId) (*WorkflowSavedQueryStep, error) {
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

func (d *doltWorkflowManager) listWorkflowJobsByWorkflowName(ctx *sql.Context, workflowName WorkflowName) ([]*WorkflowJob, error) {
	query := d.selectAllFromWorkflowJobsTableByWorkflowNameQuery(string(workflowName))
	return d.retrieveWorkflowJobs(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventTriggersByEventId(ctx *sql.Context, eventID WorkflowEventId) ([]*WorkflowEventTrigger, error) {
	query := d.selectAllFromWorkflowEventTriggersTableByWorkflowEventIdQuery(string(eventID))
	return d.retrieveWorkflowEventTriggers(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventTriggersByEventIdWhereEventTriggerTypeIsBranches(ctx *sql.Context, eventID WorkflowEventId) ([]*WorkflowEventTrigger, error) {
	query := d.selectAllFromWorkflowEventTriggersTableByWorkflowEventIdWhereEventTriggerTypeIsBranchesQuery(string(eventID))
	return d.retrieveWorkflowEventTriggers(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventTriggersByEventIdWhereEventTriggerTypeIsActivity(ctx *sql.Context, eventID WorkflowEventId) ([]*WorkflowEventTrigger, error) {
	query := d.selectAllFromWorkflowEventTriggersTableByWorkflowEventIdWhereEventTriggerTypeIsActivityQuery(string(eventID))
	return d.retrieveWorkflowEventTriggers(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventsByWorkflowName(ctx *sql.Context, workflowName WorkflowName) ([]*WorkflowEvent, error) {
	query := d.selectAllFromWorkflowEventsTableByWorkflowNameQuery(string(workflowName))
	return d.retrieveWorkflowEvents(ctx, query)
}

func (d *doltWorkflowManager) listWorkflows(ctx *sql.Context) ([]*Workflow, error) {
	query := d.selectAllFromWorkflowsTableQuery()
	return d.retrieveWorkflows(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventsByWorkflowNameWhereEventTypeIsPush(ctx *sql.Context, workflowName WorkflowName) ([]*WorkflowEvent, error) {
	query := d.selectAllFromWorkflowEventsTableByWorkflowNameWhereEventTypeIsPushQuery(string(workflowName))
	return d.retrieveWorkflowEvents(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventsByWorkflowNameWhereEventTypeIsPullRequest(ctx *sql.Context, workflowName WorkflowName) ([]*WorkflowEvent, error) {
	query := d.selectAllFromWorkflowEventsTableByWorkflowNameWhereEventTypeIsPullRequestQuery(string(workflowName))
	return d.retrieveWorkflowEvents(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventTriggerBranchesByEventTriggerId(ctx *sql.Context, triggerID WorkflowEventTriggerId) ([]*WorkflowEventTriggerBranch, error) {
	query := d.selectAllFromWorkflowEventTriggerBranchesTableByEventTriggerIdQuery(string(triggerID))
	return d.retrieveWorkflowEventTriggerBranches(ctx, query)
}

func (d *doltWorkflowManager) retrieveWorkflowSavedQueryExpectedRowColumnResults(ctx *sql.Context, query string) ([]*WorkflowSavedQueryExpectedRowColumnResult, error) {
	workflowSavedQueryExpectedResults := make([]*WorkflowSavedQueryExpectedRowColumnResult, 0)

	cb := func(cbCtx *sql.Context, cvs columnValues) error {
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

	cb := func(cbCtx *sql.Context, cvs columnValues) error {
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

	cb := func(cbCtx *sql.Context, cvs columnValues) error {
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

	cb := func(cbCtx *sql.Context, cvs columnValues) error {
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

func (d *doltWorkflowManager) retrieveWorkflowEventTriggerBranches(ctx *sql.Context, query string) ([]*WorkflowEventTriggerBranch, error) {
	workflowEventTriggerBranches := make([]*WorkflowEventTriggerBranch, 0)

	cb := func(cbCtx *sql.Context, cvs columnValues) error {
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

	cb := func(cbCtx *sql.Context, cvs columnValues) error {
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

func (d *doltWorkflowManager) retrieveWorkflowEvents(ctx *sql.Context, query string) ([]*WorkflowEvent, error) {
	workflowEvents := make([]*WorkflowEvent, 0)

	cb := func(cbCtx *sql.Context, cvs columnValues) error {
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
	cb := func(cbCtx *sql.Context, cvs columnValues) error {
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
	if config.On.Push == nil {
		err := d.deletePushWorkflowEvents(ctx, WorkflowName(config.Name.Value))
		if err != nil {
			return err
		}
	}

	if config.On.PullRequest == nil {
		err := d.deletePullRequestWorkflowEvents(ctx, WorkflowName(config.Name.Value))
		if err != nil {
			return err
		}
	}

	if config.On.WorkflowDispatch == nil {
		err := d.deleteWorkflowDispatchWorkflowEvents(ctx, WorkflowName(config.Name.Value))
		if err != nil {
			return err
		}
	}

	// handle on push
	if config.On.Push != nil {
		if len(config.On.Push.Branches) == 0 {
			events, err := d.listWorkflowEventsByWorkflowNameWhereEventTypeIsPush(ctx, WorkflowName(config.Name.Value))
			if err != nil {
				return err
			}

			if len(events) == 0 {
				_, err = d.writeWorkflowEventRow(ctx, WorkflowName(config.Name.Value), WorkflowEventTypePush)
				if err != nil {
					return err
				}
			} else {
				for _, event := range events {
					triggers, err := d.listWorkflowEventTriggersByEventIdWhereEventTriggerTypeIsBranches(ctx, *event.Id)
					if err != nil {
						return err
					}
					for _, trigger := range triggers {
						err = d.deleteWorkflowEventTrigger(ctx, *trigger.Id)
						if err != nil {
							return err
						}
					}
				}
			}

		} else {
			configBranches := make(map[string]string)
			for _, branch := range config.On.Push.Branches {
				configBranches[branch.Value] = branch.Value
			}

			pushEvents, err := d.listWorkflowEventsByWorkflowNameWhereEventTypeIsPush(ctx, WorkflowName(config.Name.Value))
			if err != nil {
				return err
			}

			for _, event := range pushEvents {
				triggers, err := d.listWorkflowEventTriggersByEventIdWhereEventTriggerTypeIsBranches(ctx, *event.Id)
				if err != nil {
					return err
				}

				for _, trigger := range triggers {
					branches, err := d.listWorkflowEventTriggerBranchesByEventTriggerId(ctx, *trigger.Id)
					if err != nil {
						return err
					}
					for _, branch := range branches {
						_, ok := configBranches[branch.Branch]
						if !ok {
							err = d.deleteWorkflowEventTriggerBranch(ctx, *branch.Id)
							if err != nil {
								return err
							}
						} else {
							delete(configBranches, branch.Branch)
						}
					}

					for branch := range configBranches {
						_, err = d.writeWorkflowEventTriggerBranchesRow(ctx, *trigger.Id, branch)
						if err != nil {
							return err
						}
						delete(configBranches, branch)
					}
				}

				// handle case where there's a defined push event, but no triggers yet
				if len(triggers) == 0 {
					triggerID, err := d.writeWorkflowEventTriggerRow(ctx, *event.Id, WorkflowEventTriggerTypeBranches)
					if err != nil {
						return err
					}
					for branch := range configBranches {
						_, err = d.writeWorkflowEventTriggerBranchesRow(ctx, triggerID, branch)
						if err != nil {
							return err
						}
						delete(configBranches, branch)
					}
				}
			}

			// handle case where there's no defined push event
			if len(pushEvents) == 0 {
				eventID, err := d.writeWorkflowEventRow(ctx, WorkflowName(config.Name.Value), WorkflowEventTypePush)
				if err != nil {
					return err
				}
				triggerID, err := d.writeWorkflowEventTriggerRow(ctx, eventID, WorkflowEventTriggerTypeBranches)
				if err != nil {
					return err
				}
				for branch := range configBranches {
					_, err = d.writeWorkflowEventTriggerBranchesRow(ctx, triggerID, branch)
					if err != nil {
						return err
					}
					delete(configBranches, branch)
				}
			}
		}
	}

	if config.On.PullRequest != nil {
		if len(config.On.PullRequest.Branches) == 0 {
			events, err := d.listWorkflowEventsByWorkflowNameWhereEventTypeIsPullRequest(ctx, WorkflowName(config.Name.Value))
			if err != nil {
				return err
			}

			for _, event := range events {
				triggers, err := d.listWorkflowEventTriggersByEventIdWhereEventTriggerTypeIsBranches(ctx, *event.Id)
				if err != nil {
					return err
				}

				for _, trigger := range triggers {
					err = d.deleteWorkflowEventTrigger(ctx, *trigger.Id)
					if err != nil {
						return err
					}
				}
			}
		} else {
			configBranches := make(map[string]string)
			for _, branch := range config.On.PullRequest.Branches {
				lowered := strings.ToLower(branch.Value)
				configBranches[lowered] = lowered
			}

			prEvents, err := d.listWorkflowEventsByWorkflowNameWhereEventTypeIsPullRequest(ctx, WorkflowName(config.Name.Value))
			if err != nil {
				return err
			}

			for _, event := range prEvents {
				triggers, err := d.listWorkflowEventTriggersByEventIdWhereEventTriggerTypeIsBranches(ctx, *event.Id)
				if err != nil {
					return err
				}

				for _, trigger := range triggers {
					branches, err := d.listWorkflowEventTriggerBranchesByEventTriggerId(ctx, *trigger.Id)
					if err != nil {
						return err
					}
					for _, branch := range branches {
						lowered := branch.Branch
						_, ok := configBranches[lowered]
						if !ok {
							err = d.deleteWorkflowEventTriggerBranch(ctx, *branch.Id)
							if err != nil {
								return err
							}
						} else {
							delete(configBranches, lowered)
						}
					}

					for branch := range configBranches {
						_, err = d.writeWorkflowEventTriggerBranchesRow(ctx, *trigger.Id, branch)
						if err != nil {
							return err
						}
						delete(configBranches, branch)
					}
				}

				// handle case where there's a defined pull request event, but no triggers yet
				if len(triggers) == 0 {
					triggerID, err := d.writeWorkflowEventTriggerRow(ctx, *event.Id, WorkflowEventTriggerTypeBranches)
					if err != nil {
						return err
					}
					for branch := range configBranches {
						_, err = d.writeWorkflowEventTriggerBranchesRow(ctx, triggerID, branch)
						if err != nil {
							return err
						}
						delete(configBranches, branch)
					}
				}
			}

			// handle case where there's no defined pull request event
			if len(prEvents) == 0 {
				eventID, err := d.writeWorkflowEventRow(ctx, WorkflowName(config.Name.Value), WorkflowEventTypePullRequest)
				if err != nil {
					return err
				}
				triggerID, err := d.writeWorkflowEventTriggerRow(ctx, eventID, WorkflowEventTriggerTypeBranches)
				if err != nil {
					return err
				}
				for branch := range configBranches {
					_, err = d.writeWorkflowEventTriggerBranchesRow(ctx, triggerID, branch)
					if err != nil {
						return err
					}
					delete(configBranches, branch)
				}
			}
		}

		if len(config.On.PullRequest.Activities) == 0 {
			events, err := d.listWorkflowEventsByWorkflowNameWhereEventTypeIsPullRequest(ctx, WorkflowName(config.Name.Value))
			if err != nil {
				return err
			}

			for _, event := range events {
				triggers, err := d.listWorkflowEventTriggersByEventIdWhereEventTriggerTypeIsActivity(ctx, *event.Id)
				if err != nil {
					return err
				}

				for _, trigger := range triggers {
					err = d.deleteWorkflowEventTrigger(ctx, *trigger.Id)
					if err != nil {
						return err
					}
				}
			}
		} else {
			configActivities := make(map[string]string)
			for _, activity := range config.On.PullRequest.Activities {
				lowered := strings.ToLower(activity.Value)
				configActivities[lowered] = lowered
			}

			prEvents, err := d.listWorkflowEventsByWorkflowNameWhereEventTypeIsPullRequest(ctx, WorkflowName(config.Name.Value))
			if err != nil {
				return err
			}

			for _, event := range prEvents {
				triggers, err := d.listWorkflowEventTriggersByEventIdWhereEventTriggerTypeIsActivity(ctx, *event.Id)
				if err != nil {
					return err
				}

				for _, trigger := range triggers {
					activity, err := WorkflowEventTriggerActivityTypeToString(trigger.EventTriggerType)
					if err != nil {
						return err
					}
					_, ok := configActivities[activity]
					if !ok {
						err = d.deleteWorkflowEventTrigger(ctx, *trigger.Id)
						if err != nil {
							return err
						}
					} else {
						delete(configActivities, activity)
					}
				}

				for activity := range configActivities {
					at, err := ToWorkflowEventTriggerActivityType(activity)
					if err != nil {
						return err
					}
					_, err = d.writeWorkflowEventTriggerRow(ctx, *event.Id, at)
					if err != nil {
						return err
					}
					delete(configActivities, activity)
				}

				// handle case where there's a defined pull request event, but no triggers yet
				if len(triggers) == 0 {
					for activity := range configActivities {
						at, err := ToWorkflowEventTriggerActivityType(activity)
						if err != nil {
							return err
						}
						_, err = d.writeWorkflowEventTriggerRow(ctx, *event.Id, at)
						if err != nil {
							return err
						}
						delete(configActivities, activity)
					}
				}
			}

			// handle case where there's no defined pull request event
			if len(prEvents) == 0 {
				eventID, err := d.writeWorkflowEventRow(ctx, WorkflowName(config.Name.Value), WorkflowEventTypePullRequest)
				if err != nil {
					return err
				}
				for activity := range configActivities {
					at, err := ToWorkflowEventTriggerActivityType(activity)
					if err != nil {
						return err
					}
					_, err = d.writeWorkflowEventTriggerRow(ctx, eventID, at)
					if err != nil {
						return err
					}
					delete(configActivities, activity)
				}
			}
		}

		if len(config.On.PullRequest.Branches) == 0 && len(config.On.PullRequest.Activities) == 0 {
			_, err := d.writeWorkflowEventRow(ctx, WorkflowName(config.Name.Value), WorkflowEventTypePullRequest)
			if err != nil {
				return err
			}
		}
	}

	if config.On.WorkflowDispatch != nil {
		_, err := d.writeWorkflowEventRow(ctx, WorkflowName(config.Name.Value), WorkflowEventTypeWorkflowDispatch)
		if err != nil {
			return err
		}
	}

	configJobs := make(map[string]Job)
	for _, job := range config.Jobs {
		configJobs[job.Name.Value] = job
	}

	jobs, err := d.listWorkflowJobsByWorkflowName(ctx, WorkflowName(config.Name.Value))
	if err != nil {
		return err
	}

	for _, job := range jobs {
		_, ok := configJobs[job.Name]
		if !ok {
			err = d.deleteWorkflowJob(ctx, *job.Id)
			if err != nil {
				return err
			}
		} else {
			configSteps := make(map[string]Step)
			orderedSteps := make(map[string]int)

			for _, configJob := range config.Jobs {
				if configJob.Name.Value == job.Name {
					for idx, step := range configJob.Steps {
						orderedSteps[step.Name.Value] = idx
						configSteps[step.Name.Value] = step
					}
					break
				}
			}

			steps, err := d.listWorkflowStepsByJobId(ctx, *job.Id)
			if err != nil {
				return err
			}

			for _, step := range steps {
				configStep, ok := configSteps[step.Name]
				if !ok {
					err = d.deleteWorkflowStep(ctx, *step.Id)
					if err != nil {
						return err
					}
				} else {
					orderIdx, ok := orderedSteps[step.Name]
					if !ok {
						return errors.New("failed to get step order")
					}

					stepOrder := orderIdx + 1
					if step.StepOrder != stepOrder {
						err = d.updateWorkflowStepRow(ctx, *step.Id, stepOrder)
						if err != nil {
							return err
						}
					}

					if configStep.SavedQueryName.Value != "" {
						savedQueryStep, err := d.getWorkflowSavedQueryStepByStepId(ctx, *step.Id)
						if err != nil {
							return err
						}

						if configStep.ExpectedRows.Value == "" && configStep.ExpectedColumns.Value == "" {
							err = d.deleteWorkflowSavedQueryStepExpectedRowColumnResults(ctx, *savedQueryStep.Id)
							if err != nil {
								return err
							}

							err = d.updateWorkflowSavedQueryStepRow(ctx, *savedQueryStep.Id, configStep.SavedQueryName.Value, WorkflowSavedQueryExpectedResultsTypeUnspecified)
							if err != nil {
								return err
							}
						} else {
							if savedQueryStep.SavedQueryExpectedResultsType == WorkflowSavedQueryExpectedResultsTypeRowColumnCount {
								if configStep.SavedQueryName.Value != savedQueryStep.SavedQueryName {
									err = d.updateWorkflowSavedQueryStepRow(ctx, *savedQueryStep.Id, configStep.SavedQueryName.Value, WorkflowSavedQueryExpectedResultsTypeRowColumnCount)
									if err != nil {
										return err
									}
								}

								result, err := d.getWorkflowSavedQueryExpectedRowColumnResultBySavedQueryStepId(ctx, *savedQueryStep.Id)
								if err != nil {
									return err
								}

								newExpectedColumnComparisonType := WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified
								var newExpectedColumnCount int64
								if configStep.ExpectedColumns.Value != "" {
									newExpectedColumnComparisonType, newExpectedColumnCount, err = d.parseSavedQueryExpectedResultString(configStep.ExpectedColumns.Value)
									if err != nil {
										return err
									}
								}

								newExpectedRowComparisonType := WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified
								var newExpectedRowCount int64
								if configStep.ExpectedRows.Value != "" {
									newExpectedRowComparisonType, newExpectedRowCount, err = d.parseSavedQueryExpectedResultString(configStep.ExpectedRows.Value)
									if err != nil {
										return err
									}
								}

								if (configStep.ExpectedColumns.Value != "" && newExpectedRowComparisonType != result.ExpectedRowCountComparisonType) ||
									(configStep.ExpectedRows.Value != "" && newExpectedColumnComparisonType != result.ExpectedColumnCountComparisonType) ||
									(configStep.ExpectedRows.Value != "" && newExpectedRowCount != result.ExpectedRowCount) ||
									(configStep.ExpectedColumns.Value != "" && newExpectedColumnCount != result.ExpectedColumnCount) {
									err = d.updateWorkflowSavedQueryStepsExpectedRowColumnResultsRow(ctx, *result.Id, newExpectedColumnComparisonType, newExpectedRowComparisonType, newExpectedColumnCount, newExpectedRowCount)
									if err != nil {
										return err
									}
								}
							}
						}
					}

					delete(configSteps, step.Name)
					delete(orderedSteps, step.Name)
				}
			}

			for _, step := range configSteps {
				orderIdx, ok := orderedSteps[step.Name.Value]
				if !ok {
					return errors.New("failed to get step order")
				}

				stepOrder := orderIdx + 1
				stepID, err := d.writeWorkflowStepRow(ctx, *job.Id, step.Name.Value, stepOrder, WorkflowStepTypeSavedQuery)
				if err != nil {
					return err
				}

				savedQueryStepID, err := d.writeWorkflowSavedQueryStepRow(ctx, stepID, step.SavedQueryName.Value, WorkflowSavedQueryExpectedResultsTypeRowColumnCount)
				if err != nil {
					return err
				}

				expectedColumnComparisonType, expectedColumnCount, err := d.parseSavedQueryExpectedResultString(step.ExpectedColumns.Value)
				if err != nil {
					return err
				}

				expectedRowComparisonType, expectedRowCount, err := d.parseSavedQueryExpectedResultString(step.ExpectedRows.Value)
				if err != nil {
					return err
				}

				_, err = d.writeWorkflowSavedQueryStepExpectedRowColumnResultRow(ctx, savedQueryStepID, expectedColumnComparisonType, expectedRowComparisonType, expectedColumnCount, expectedRowCount)
				if err != nil {
					return err
				}

				delete(configSteps, step.Name.Value)
				delete(orderedSteps, step.Name.Value)
			}

			delete(configJobs, job.Name)
		}
	}

	// create all jobs that do not yet exist
	for _, job := range configJobs {
		jobID, err := d.writeWorkflowJobRow(ctx, WorkflowName(config.Name.Value), job.Name.Value)
		if err != nil {
			return err
		}
		for idx, step := range job.Steps {
			stepID, err := d.writeWorkflowStepRow(ctx, jobID, step.Name.Value, idx+1, WorkflowStepTypeSavedQuery)
			if err != nil {
				return err
			}

			savedQueryStepID, err := d.writeWorkflowSavedQueryStepRow(ctx, stepID, step.SavedQueryName.Value, WorkflowSavedQueryExpectedResultsTypeRowColumnCount)
			if err != nil {
				return err
			}

			expectedColumnComparisonType, expectedColumnCount, err := d.parseSavedQueryExpectedResultString(step.ExpectedColumns.Value)
			if err != nil {
				return err
			}

			expectedRowComparisonType, expectedRowCount, err := d.parseSavedQueryExpectedResultString(step.ExpectedRows.Value)
			if err != nil {
				return err
			}

			_, err = d.writeWorkflowSavedQueryStepExpectedRowColumnResultRow(ctx, savedQueryStepID, expectedColumnComparisonType, expectedRowComparisonType, expectedColumnCount, expectedRowCount)
			if err != nil {
				return err
			}
		}

		delete(configJobs, job.Name.Value)
	}

	return nil
}

func (d *doltWorkflowManager) deleteWorkflow(ctx *sql.Context, workflowName WorkflowName) error {
	query := d.deleteFromWorkflowsTableByWorkflowNameQuery(string(workflowName))
	return d.sqlWriteQuery(ctx, query)
}

func (d *doltWorkflowManager) deletePushWorkflowEvents(ctx *sql.Context, workflowName WorkflowName) error {
	query := d.deleteFromWorkflowEventsTableByWorkflowNameQueryWhereWorkflowEventTypeIsPush(string(workflowName))
	return d.sqlWriteQuery(ctx, query)
}

func (d *doltWorkflowManager) deletePullRequestWorkflowEvents(ctx *sql.Context, workflowName WorkflowName) error {
	query := d.deleteFromWorkflowEventsTableByWorkflowNameQueryWhereWorkflowEventTypeIsPullRequest(string(workflowName))
	return d.sqlWriteQuery(ctx, query)
}

func (d *doltWorkflowManager) deleteWorkflowEventTriggerBranch(ctx *sql.Context, branchID WorkflowEventTriggerBranchId) error {
	query := d.deleteFromWorkflowEventTriggerBranchesTableByEventTriggerBranchIdQuery(string(branchID))
	return d.sqlWriteQuery(ctx, query)
}

func (d *doltWorkflowManager) deleteWorkflowEventTrigger(ctx *sql.Context, triggerID WorkflowEventTriggerId) error {
	query := d.deleteFromWorkflowEventTriggersTableByWorkflowEventTriggerIdQuery(string(triggerID))
	return d.sqlWriteQuery(ctx, query)
}

func (d *doltWorkflowManager) deleteWorkflowJob(ctx *sql.Context, jobID WorkflowJobId) error {
	query := d.deleteFromWorkflowJobsTableByWorkflowJobIdQuery(string(jobID))
	return d.sqlWriteQuery(ctx, query)
}

func (d *doltWorkflowManager) deleteWorkflowStep(ctx *sql.Context, stepID WorkflowStepId) error {
	query := d.deleteFromWorkflowStepsTableByWorkflowStepIdQuery(string(stepID))
	return d.sqlWriteQuery(ctx, query)
}

func (d *doltWorkflowManager) deleteWorkflowDispatchWorkflowEvents(ctx *sql.Context, workflowName WorkflowName) error {
	query := d.deleteFromWorkflowEventsTableByWorkflowNameQueryWhereWorkflowEventTypeIsWorkflowDispatch(string(workflowName))
	return d.sqlWriteQuery(ctx, query)
}

func (d *doltWorkflowManager) deleteWorkflowSavedQueryStepExpectedRowColumnResults(ctx *sql.Context, savedQueryStepID WorkflowSavedQueryStepId) error {
	query := d.deleteFromSavedQueryStepExpectedRowColumnResultsTableBySavedQueryStepIdQuery(string(savedQueryStepID))
	return d.sqlWriteQuery(ctx, query)
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

func (d *doltWorkflowManager) updateWorkflowStepRow(ctx *sql.Context, stepID WorkflowStepId, stepOrder int) error {
	query := d.updateWorkflowStepsTableQuery(string(stepID), stepOrder)
	return d.sqlWriteQuery(ctx, query)
}

func (d *doltWorkflowManager) updateWorkflowSavedQueryStepRow(ctx *sql.Context, savedQueryStepID WorkflowSavedQueryStepId, savedQueryName string, expectedResultsType WorkflowSavedQueryExpectedResultsType) error {
	query := d.updateWorkflowSavedQueryStepsTableQuery(string(savedQueryStepID), savedQueryName, int(expectedResultsType))
	return d.sqlWriteQuery(ctx, query)
}

func (d *doltWorkflowManager) updateWorkflowSavedQueryStepsExpectedRowColumnResultsRow(ctx *sql.Context, resultID WorkflowSavedQueryExpectedRowColumnResultId, expectedColumnComparisonType, expectedRowComparisonType WorkflowSavedQueryExpectedRowColumnComparisonType, expectedColumnCount, expectedRowCount int64) error {
	query := d.updateWorkflowSavedQueryStepsExpectedRowColumnResultsTableQuery(string(resultID), int(expectedColumnComparisonType), int(expectedRowComparisonType), expectedColumnCount, expectedRowCount)
	return d.sqlWriteQuery(ctx, query)
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
	if str == "" {
		return WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified, 0, nil
	}

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
		default:
			return 0, 0, errors.New("unknown comparison type")
		}
	}
	return 0, 0, fmt.Errorf("unable to parse comparison string: %s", str)
}

func (d *doltWorkflowManager) toSavedQueryExpectedResultString(comparisonType WorkflowSavedQueryExpectedRowColumnComparisonType, count int64) (string, error) {
	var compareStr string
	switch comparisonType {
	case WorkflowSavedQueryExpectedRowColumnComparisonTypeEquals:
		compareStr = "=="
	case WorkflowSavedQueryExpectedRowColumnComparisonTypeNotEquals:
		compareStr = "!="
	case WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThan:
		compareStr = ">"
	case WorkflowSavedQueryExpectedRowColumnComparisonTypeGreaterThanOrEqual:
		compareStr = ">="
	case WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThan:
		compareStr = "<"
	case WorkflowSavedQueryExpectedRowColumnComparisonTypeLessThanOrEqual:
		compareStr = "<="
	default:
		return "", errors.New("unknown comparison type")
	}
	return fmt.Sprintf("%s %d", compareStr, count), nil
}

func (d *doltWorkflowManager) createWorkflow(ctx *sql.Context, config *WorkflowConfig) error {
	workflowName, err := d.writeWorkflowRow(ctx, WorkflowName(config.Name.Value))
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
				for _, activity := range config.On.PullRequest.Activities {
					lowered := strings.ToLower(activity.Value)
					at, err := ToWorkflowEventTriggerActivityType(lowered)
					if err != nil {
						return err
					}
					_, err = d.writeWorkflowEventTriggerRow(ctx, pullRequestEventID, at)
					if err != nil {
						return err
					}
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
			lowered := strings.ToLower(branch.Value)
			_, err = d.writeWorkflowEventTriggerBranchesRow(ctx, pushBranchesTriggerEventID, lowered)
			if err != nil {
				return err
			}
		}
	}

	// handle pull requests
	if pullRequestBranchesTriggerEventID != "" {
		for _, branch := range config.On.PullRequest.Branches {
			lowered := strings.ToLower(branch.Value)
			_, err = d.writeWorkflowEventTriggerBranchesRow(ctx, pullRequestBranchesTriggerEventID, lowered)
			if err != nil {
				return err
			}
		}
	}

	// handle jobs
	for _, job := range config.Jobs {
		// insert into jobs
		jobID, err := d.writeWorkflowJobRow(ctx, workflowName, job.Name.Value)
		if err != nil {
			return err
		}

		// handle steps
		for idx, step := range job.Steps {
			// insert into step
			order := idx + 1

			var stepType WorkflowStepType
			if step.SavedQueryName.Value != "" {
				stepType = WorkflowStepTypeSavedQuery
			}

			stepID, err := d.writeWorkflowStepRow(ctx, jobID, step.Name.Value, order, stepType)
			if err != nil {
				return err
			}

			// insert into saved query steps
			if stepType == WorkflowStepTypeSavedQuery {
				resultType := WorkflowSavedQueryExpectedResultsTypeUnspecified
				if step.ExpectedColumns.Value != "" || step.ExpectedRows.Value != "" {
					resultType = WorkflowSavedQueryExpectedResultsTypeRowColumnCount
				}

				savedQueryStepID, err := d.writeWorkflowSavedQueryStepRow(ctx, stepID, step.SavedQueryName.Value, resultType)
				if err != nil {
					return err
				}

				if resultType == WorkflowSavedQueryExpectedResultsTypeRowColumnCount {
					// insert into expected results
					expectedColumnComparisonType, expectedColumnCount, err := d.parseSavedQueryExpectedResultString(step.ExpectedColumns.Value)
					if err != nil {
						return err
					}

					expectedRowComparisonType, expectedRowCount, err := d.parseSavedQueryExpectedResultString(step.ExpectedRows.Value)
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
	}
	return nil
}

func (d *doltWorkflowManager) getWorkflowConfig(ctx *sql.Context, workflowName string) (*WorkflowConfig, error) {
	config := &WorkflowConfig{}

	workflow, err := d.getWorkflow(ctx, workflowName)
	if err != nil {
		return nil, err
	}

	config.Name = newScalarDoubleQuotedYamlNode(string(*workflow.Name))

	events, err := d.listWorkflowEventsByWorkflowName(ctx, *workflow.Name)
	if err != nil {
		return nil, err
	}

	on := On{}

	for _, event := range events {
		triggers, err := d.listWorkflowEventTriggersByEventId(ctx, *event.Id)
		if err != nil {
			return nil, err
		}

		activities := make([]yaml.Node, 0)
		branches := make([]yaml.Node, 0)

		for _, trigger := range triggers {
			switch trigger.EventTriggerType {
			case WorkflowEventTriggerTypeBranches:
				brns, err := d.listWorkflowEventTriggerBranchesByEventTriggerId(ctx, *trigger.Id)
				if err != nil {
					return nil, err
				}
				for _, brn := range brns {
					branches = append(branches, newScalarDoubleQuotedYamlNode(brn.Branch))
				}
			case WorkflowEventTriggerTypeActivityOpened,
				WorkflowEventTriggerTypeActivityClosed,
				WorkflowEventTriggerTypeActivityReopened,
				WorkflowEventTriggerTypeActivitySynchronized,
				WorkflowEventTriggerTypeWorkflowDispatch:
				activity, err := WorkflowEventTriggerActivityTypeToString(trigger.EventTriggerType)
				if err != nil {
					return nil, err
				}
				activities = append(activities, newScalarDoubleQuotedYamlNode(activity))
			case WorkflowEventTriggerTypeUnspecified:
			default:
				return nil, fmt.Errorf("unknown trigger type: %d", trigger.EventTriggerType)
			}
		}

		if event.EventType == WorkflowEventTypePush {
			on.Push = &Push{}
			if len(branches) > 0 {
				on.Push.Branches = branches
			}
		} else if event.EventType == WorkflowEventTypePullRequest {
			on.PullRequest = &PullRequest{}
			if len(branches) > 0 {
				on.PullRequest.Branches = branches
			}
			if len(activities) > 0 {
				on.PullRequest.Activities = activities
			}
		} else if event.EventType == WorkflowEventTypeWorkflowDispatch {
			on.WorkflowDispatch = &WorkflowDispatch{}
		}
	}

	config.On = on

	jobs := make([]Job, 0)

	jbs, err := d.listWorkflowJobsByWorkflowName(ctx, *workflow.Name)
	if err != nil {
		return nil, err
	}

	for _, jb := range jbs {
		steps := make([]Step, 0)

		stps, err := d.listWorkflowStepsByJobId(ctx, *jb.Id)
		if err != nil {
			return nil, err
		}

		sort.Slice(stps, func(i, j int) bool {
			return stps[i].StepOrder < stps[j].StepOrder
		})

		for _, stp := range stps {
			if stp.StepType == WorkflowStepTypeSavedQuery {
				savedQueryStep, err := d.getWorkflowSavedQueryStepByStepId(ctx, *stp.Id)
				if err != nil {
					return nil, err
				}

				step := Step{
					Name:           newScalarDoubleQuotedYamlNode(stp.Name),
					SavedQueryName: newScalarDoubleQuotedYamlNode(savedQueryStep.SavedQueryName),
				}

				if savedQueryStep.SavedQueryExpectedResultsType == WorkflowSavedQueryExpectedResultsTypeRowColumnCount {
					expectedResult, err := d.getWorkflowSavedQueryExpectedRowColumnResultBySavedQueryStepId(ctx, *savedQueryStep.Id)
					if err != nil {
						return nil, err
					}

					if expectedResult.ExpectedColumnCountComparisonType != WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified {
						expectedColumnsStr, err := d.toSavedQueryExpectedResultString(expectedResult.ExpectedColumnCountComparisonType, expectedResult.ExpectedColumnCount)
						if err != nil {
							return nil, err
						}
						step.ExpectedColumns = newScalarDoubleQuotedYamlNode(expectedColumnsStr)
					}

					if expectedResult.ExpectedRowCountComparisonType != WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified {
						expectedRowsStr, err := d.toSavedQueryExpectedResultString(expectedResult.ExpectedRowCountComparisonType, expectedResult.ExpectedRowCount)
						if err != nil {
							return nil, err
						}
						step.ExpectedRows = newScalarDoubleQuotedYamlNode(expectedRowsStr)
					}
				}

				steps = append(steps, step)
			}
		}

		job := Job{
			Name:  newScalarDoubleQuotedYamlNode(jb.Name),
			Steps: steps,
		}

		jobs = append(jobs, job)
	}

	config.Jobs = jobs
	return config, nil
}

func (d *doltWorkflowManager) storeFromConfig(ctx *sql.Context, config *WorkflowConfig) error {
	_, err := d.getWorkflow(ctx, config.Name.Value)
	if err != nil {
		if err == ErrWorkflowNotFound {
			return d.createWorkflow(ctx, config)
		}
		return err
	}
	return d.updateExistingWorkflow(ctx, config)
}

func (d *doltWorkflowManager) GetWorkflowConfig(ctx *sql.Context, db sqle.Database, workflowName string) (*WorkflowConfig, error) {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Read); err != nil {
		return nil, err
	}
	return d.getWorkflowConfig(ctx, workflowName)
}

func (d *doltWorkflowManager) ListWorkflows(ctx *sql.Context, db sqle.Database) ([]string, error) {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Read); err != nil {
		return nil, err
	}
	names := make([]string, 0)
	workflows, err := d.listWorkflows(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(workflows, func(i, j int) bool {
		return *workflows[i].Name < *workflows[j].Name
	})
	for _, w := range workflows {
		names = append(names, string(*w.Name))
	}
	return names, nil
}

func (d *doltWorkflowManager) RemoveWorkflow(ctx *sql.Context, db sqle.Database, workflowName string) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}
	_, err := d.getWorkflow(ctx, workflowName)
	if err != nil {
		return err
	}

	err = d.deleteWorkflow(ctx, WorkflowName(workflowName))
	if err != nil {
		return err
	}
	return d.commitRemoveWorkflow(ctx, workflowName)
}

func (d *doltWorkflowManager) StoreAndCommit(ctx *sql.Context, db sqle.Database, config *WorkflowConfig) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	err := d.storeFromConfig(ctx, config)
	if err != nil {
		return err
	}

	return d.commitWorkflow(ctx, config.Name.Value)
}

func newScalarDoubleQuotedYamlNode(value string) yaml.Node {
	return yaml.Node{
		Kind:  yaml.ScalarNode,
		Style: yaml.DoubleQuotedStyle,
		Value: value,
	}
}
