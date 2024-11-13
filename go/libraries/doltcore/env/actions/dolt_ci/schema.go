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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// ExpectedDoltCITablesOrdered contains the tables names for the dolt ci workflow tables, in parent to child table order.
// This is exported for use in DoltHub/DoltLab.
var ExpectedDoltCITablesOrdered = []doltdb.TableName{
	{Name: doltdb.WorkflowsTableName},
	{Name: doltdb.WorkflowEventsTableName},
	{Name: doltdb.WorkflowEventTriggersTableName},
	{Name: doltdb.WorkflowEventTriggerBranchesTableName},
	{Name: doltdb.WorkflowEventTriggerActivitiesTableName},
	{Name: doltdb.WorkflowJobsTableName},
	{Name: doltdb.WorkflowStepsTableName},
	{Name: doltdb.WorkflowSavedQueryStepsTableName},
	{Name: doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName},
}

type queryFunc func(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, *sql.QueryFlags, error)

func HasDoltCITables(ctx *sql.Context) (bool, error) {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)
	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return false, err
	}

	root := ws.WorkingRoot()

	exists := 0
	var hasSome bool
	var hasAll bool
	for _, tableName := range ExpectedDoltCITablesOrdered {
		found, err := root.HasTable(ctx, tableName)
		if err != nil {
			return false, err
		}
		if found {
			exists++
		}
	}

	hasSome = exists > 0 && exists < len(ExpectedDoltCITablesOrdered)
	hasAll = exists == len(ExpectedDoltCITablesOrdered)
	if !hasSome && !hasAll {
		return false, nil
	}
	if hasSome && !hasAll {
		return true, fmt.Errorf("found some but not all of required dolt ci tables")
	}
	return true, nil
}

func getExistingDoltCITables(ctx *sql.Context) ([]doltdb.TableName, error) {
	existing := make([]doltdb.TableName, 0)
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)
	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return nil, err
	}

	root := ws.WorkingRoot()

	for _, tableName := range ExpectedDoltCITablesOrdered {
		found, err := root.HasTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if found {
			existing = append(existing, tableName)
		}
	}

	return existing, nil
}

func sqlWriteQuery(ctx *sql.Context, queryFunc queryFunc, query string) error {
	_, rowIter, _, err := queryFunc(ctx, query)
	if err != nil {
		return err
	}
	_, err = sql.RowIterToRows(ctx, rowIter)
	return err
}

func commitCIDestroy(ctx *sql.Context, queryFunc queryFunc, commiterName, commiterEmail string) error {
	// stage table in reverse order so child tables
	// are staged before parent tables
	for i := len(ExpectedDoltCITablesOrdered) - 1; i >= 0; i-- {
		tableName := ExpectedDoltCITablesOrdered[i]
		err := sqlWriteQuery(ctx, queryFunc, fmt.Sprintf("CALL DOLT_ADD('%s');", tableName))
		if err != nil {
			return err
		}
	}
	return sqlWriteQuery(ctx, queryFunc, fmt.Sprintf("CALL DOLT_COMMIT('-m' 'Successfully destroyed Dolt CI', '--author', '%s <%s>');", commiterName, commiterEmail))
}

func commitCIInit(ctx *sql.Context, queryFunc queryFunc, commiterName, commiterEmail string) error {
	// stage table in reverse order so child tables
	// are staged before parent tables
	for i := len(ExpectedDoltCITablesOrdered) - 1; i >= 0; i-- {
		tableName := ExpectedDoltCITablesOrdered[i]
		err := sqlWriteQuery(ctx, queryFunc, fmt.Sprintf("CALL DOLT_ADD('%s');", tableName))
		if err != nil {
			return err
		}
	}
	return sqlWriteQuery(ctx, queryFunc, fmt.Sprintf("CALL DOLT_COMMIT('-m' 'Successfully initialized Dolt CI', '--author', '%s <%s>');", commiterName, commiterEmail))
}

func DestroyDoltCITables(ctx *sql.Context, db sqle.Database, queryFunc queryFunc, commiterName, commiterEmail string) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	// disable foreign key checks
	err := SqlWriteQuery(ctx, queryFunc, "SET FOREIGN_KEY_CHECKS=0;")
	if err != nil {
		return err
	}

	existing, err := getExistingDoltCITables(ctx)
	if err != nil {
		return err
	}

	for _, tableName := range existing {
		err = SqlWriteQuery(ctx, queryFunc, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName.Name))
		if err != nil {
			return err
		}
	}

	// enable foreign keys again
	err = SqlWriteQuery(ctx, queryFunc, "SET FOREIGN_KEY_CHECKS=1;")
	if err != nil {
		return err
	}

	return commitCIDestroy(ctx, queryFunc, commiterName, commiterEmail)
}

func CreateDoltCITables(ctx *sql.Context, db sqle.Database, queryFunc queryFunc, commiterName, commiterEmail string) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	orderedCreateTableQueries := []string{
		createWorkflowsTableQuery(),
		createWorkflowEventsTableQuery(),
		createWorkflowEventTriggersTableQuery(),
		createWorkflowEventTriggerBranchesTableQuery(),
		createWorkflowEventTriggerActivitiesTableQuery(),
		createWorkflowJobsTableQuery(),
		createWorkflowStepsTableQuery(),
		createWorkflowSavedQueryStepsTableQuery(),
		createWorkflowSavedQueryStepExpectedRowColumnResultsTableQuery(),
		deleteAllFromWorkflowsTableQuery(), // as last step run delete to create resolve all indexes/fks
	}

	newCtx := doltdb.ContextWithDoltCICreateBypassKey(ctx)

	for _, query := range orderedCreateTableQueries {
		err := SqlWriteQuery(newCtx, queryFunc, query)
		if err != nil {
			return err
		}
	}

	return commitCIInit(newCtx, queryFunc, commiterName, commiterEmail)
}

func createWorkflowsTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(2048) collate utf8mb4_0900_ai_ci primary key, `%s` datetime(6) not null, `%s` datetime(6) not null);", doltdb.WorkflowsTableName, doltdb.WorkflowsNameColName, doltdb.WorkflowsCreatedAtColName, doltdb.WorkflowsUpdatedAtColName)
}

func createWorkflowEventsTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(36) primary key, `%s` int not null, `%s` varchar(2048) not null, foreign key (`%s`) references %s (`%s`) on delete cascade);", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsIdPkColName, doltdb.WorkflowEventsEventTypeColName, doltdb.WorkflowEventsWorkflowNameFkColName, doltdb.WorkflowEventsWorkflowNameFkColName, doltdb.WorkflowsTableName, doltdb.WorkflowsNameColName)
}

func createWorkflowEventTriggersTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(36) primary key, `%s` int not null, `%s` varchar(36) not null, foreign key (`%s`) references %s (`%s`) on delete cascade);", doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersIdPkColName, doltdb.WorkflowEventTriggersEventTriggerTypeColName, doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName, doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName, doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsIdPkColName)
}

func createWorkflowEventTriggerBranchesTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(36) primary key, `%s` varchar(1024) collate utf8mb4_0900_ai_ci not null, `%s` varchar(36) not null, foreign key (`%s`) references %s (`%s`) on delete cascade);", doltdb.WorkflowEventTriggerBranchesTableName, doltdb.WorkflowEventTriggerBranchesIdPkColName, doltdb.WorkflowEventTriggerBranchesBranchColName, doltdb.WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName, doltdb.WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName, doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersIdPkColName)
}

func createWorkflowEventTriggerActivitiesTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(36) primary key, `%s` varchar(1024) collate utf8mb4_0900_ai_ci not null, `%s` varchar(36) not null, foreign key (`%s`) references %s (`%s`) on delete cascade);", doltdb.WorkflowEventTriggerActivitiesTableName, doltdb.WorkflowEventTriggerActivitiesIdPkColName, doltdb.WorkflowEventTriggerActivitiesActivityColName, doltdb.WorkflowEventTriggerActivitiesWorkflowEventTriggersIdFkColName, doltdb.WorkflowEventTriggerActivitiesWorkflowEventTriggersIdFkColName, doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersIdPkColName)
}

func createWorkflowJobsTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(36) primary key, `%s` varchar(1024) collate utf8mb4_0900_ai_ci not null, `%s` datetime(6) not null, `%s` datetime(6) not null, `%s` varchar(2048) not null, foreign key (`%s`) references %s (`%s`) on delete cascade);", doltdb.WorkflowJobsTableName, doltdb.WorkflowJobsIdPkColName, doltdb.WorkflowJobsNameColName, doltdb.WorkflowJobsCreatedAtColName, doltdb.WorkflowJobsUpdatedAtColName, doltdb.WorkflowJobsWorkflowNameFkColName, doltdb.WorkflowJobsWorkflowNameFkColName, doltdb.WorkflowsTableName, doltdb.WorkflowsNameColName)
}

func createWorkflowStepsTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(36) primary key, `%s` varchar(1024) collate utf8mb4_0900_ai_ci not null, `%s` int not null, `%s` int not null, `%s` datetime(6) not null, `%s` datetime(6) not null,`%s` varchar(36) not null, foreign key (`%s`) references %s (`%s`) on delete cascade);", doltdb.WorkflowStepsTableName, doltdb.WorkflowStepsIdPkColName, doltdb.WorkflowStepsNameColName, doltdb.WorkflowStepsStepOrderColName, doltdb.WorkflowStepsStepTypeColName, doltdb.WorkflowStepsCreatedAtColName, doltdb.WorkflowStepsUpdatedAtColName, doltdb.WorkflowStepsWorkflowJobIdFkColName, doltdb.WorkflowStepsWorkflowJobIdFkColName, doltdb.WorkflowJobsTableName, doltdb.WorkflowJobsIdPkColName)
}

func createWorkflowSavedQueryStepsTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(36) primary key, `%s` varchar(2048) collate utf8mb4_0900_ai_ci not null, `%s` int not null, `%s` varchar(36) not null, foreign key (`%s`) references %s (`%s`) on delete cascade);", doltdb.WorkflowSavedQueryStepsTableName, doltdb.WorkflowSavedQueryStepsIdPkColName, doltdb.WorkflowSavedQueryStepsSavedQueryNameColName, doltdb.WorkflowSavedQueryStepsExpectedResultsTypeColName, doltdb.WorkflowSavedQueryStepsWorkflowStepIdFkColName, doltdb.WorkflowSavedQueryStepsWorkflowStepIdFkColName, doltdb.WorkflowStepsTableName, doltdb.WorkflowStepsIdPkColName)
}

func createWorkflowSavedQueryStepExpectedRowColumnResultsTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(36) primary key,`%s` int not null, `%s` int not null,`%s` bigint not null,`%s` bigint not null,`%s` datetime(6) not null,`%s` datetime(6) not null,`%s` varchar(36) not null, foreign key (`%s`) references %s (`%s`) on delete cascade);", doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsIdPkColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountComparisonTypeColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountComparisonTypeColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsCreatedAtColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsUpdatedAtColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName, doltdb.WorkflowSavedQueryStepsTableName, doltdb.WorkflowSavedQueryStepsIdPkColName)
}

func deleteAllFromWorkflowsTableQuery() string {
	return fmt.Sprintf("delete from %s;", doltdb.WorkflowsTableName)
}
