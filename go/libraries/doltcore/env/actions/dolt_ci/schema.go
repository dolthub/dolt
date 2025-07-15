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

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// WrappedTableName is a struct that wraps a doltdb.TableName
// and specifies whether the tables should still be created.
// Deprecated tables will have Deprecated: true
type WrappedTableName struct {
	TableName  doltdb.TableName
	Deprecated bool
}

type WrappedTableNameSlice []WrappedTableName

func (w WrappedTableNameSlice) ActiveTableNames() []doltdb.TableName {
	tableNames := make([]doltdb.TableName, 0)
	for _, wrapt := range w {
		if !wrapt.Deprecated {
			tableNames = append(tableNames, wrapt.TableName)
		}
	}
	return tableNames
}

// ExpectedDoltCITablesOrdered contains the tables names for the dolt ci workflow tables, in parent to child table order.
// This is exported for use in DoltHub/DoltLab.
var ExpectedDoltCITablesOrdered = WrappedTableNameSlice{
	{TableName: doltdb.TableName{Name: doltdb.WorkflowsTableName}},
	{TableName: doltdb.TableName{Name: doltdb.WorkflowEventsTableName}},
	{TableName: doltdb.TableName{Name: doltdb.WorkflowEventTriggersTableName}},
	{TableName: doltdb.TableName{Name: doltdb.WorkflowEventTriggerBranchesTableName}},
	{TableName: doltdb.TableName{Name: doltdb.WorkflowEventTriggerActivitiesTableName}, Deprecated: true},
	{TableName: doltdb.TableName{Name: doltdb.WorkflowJobsTableName}},
	{TableName: doltdb.TableName{Name: doltdb.WorkflowStepsTableName}},
	{TableName: doltdb.TableName{Name: doltdb.WorkflowSavedQueryStepsTableName}},
	{TableName: doltdb.TableName{Name: doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName}},
}

type queryFunc func(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, *sql.QueryFlags, error)

// HasDoltCITables reports whether a database has all expected dolt_ci tables which store continuous integration config.
// If the database has only some of the expected tables, an error is returned.
func HasDoltCITables(queryist cli.Queryist, sqlCtx *sql.Context) (bool, error) {
	exists := 0
	var hasSome bool
	var hasAll bool
	activeOnly := ExpectedDoltCITablesOrdered.ActiveTableNames()

	for _, tableName := range activeOnly {
		query := fmt.Sprintf("SHOW TABLES LIKE '%s';", tableName.Name)

		_, _, _, err := queryist.Query(sqlCtx, "set @@dolt_show_system_tables = 1")
		if err != nil {
			return false, err
		}
		rows, err := commands.GetRowsForSql(queryist, sqlCtx, query)
		if err != nil {
			return false, err
		}
		_, _, _, err = queryist.Query(sqlCtx, "set @@dolt_show_system_tables = 0")
		if err != nil {
			return false, err
		}

		if len(rows) > 0 {
			exists++
		}
	}

	hasSome = exists > 0 && exists < len(activeOnly)
	hasAll = exists == len(activeOnly)
	if !hasSome && !hasAll {
		return false, nil
	}
	if hasSome && !hasAll {
		return true, fmt.Errorf("found some but not all of required dolt ci tables")
	}
	return true, nil
}

func sqlWriteQuery(ctx *sql.Context, queryFunc queryFunc, query string) error {
	_, rowIter, _, err := queryFunc(ctx, query)
	if err != nil {
		return err
	}
	_, err = sql.RowIterToRows(ctx, rowIter)
	return err
}

func commitCIDestroy(queryist cli.Queryist, ctx *sql.Context, tableNames []doltdb.TableName, name, email string) error {
	// stage table in reverse order so child tables
	// are staged before parent tables
	for i := len(tableNames) - 1; i >= 0; i-- {
		tn := tableNames[i]
		_, err := commands.GetRowsForSql(queryist, ctx, fmt.Sprintf("CALL DOLT_ADD('%s');", tn.Name))
		if err != nil {
			return err
		}
	}

	query := fmt.Sprintf("CALL DOLT_COMMIT('-m', 'Successfully destroyed Dolt CI', '--author', '%s <%s>');", name, email)
	_, err := commands.GetRowsForSql(queryist, ctx, query)
	return err
}

func commitCIInit(ctx *sql.Context, queryist cli.Queryist, tableNames []doltdb.TableName, name, email string) error {
	// stage table in reverse order so child tables
	// are staged before parent tables
	for i := len(tableNames) - 1; i >= 0; i-- {
		tn := tableNames[i]
		query := fmt.Sprintf("CALL DOLT_ADD('%s');", tn.Name)
		_, err := commands.GetRowsForSql(queryist, ctx, query)
		if err != nil {
			return err
		}
	}

	query := fmt.Sprintf("CALL DOLT_COMMIT('-m', 'Successfully initialized Dolt CI', '--author', '%s <%s>');", name, email)
	_, err := commands.GetRowsForSql(queryist, ctx, query)
	return err
}

// DestroyDoltCITables drops all dolt_ci tables and creates a new Dolt commit.
func DestroyDoltCITables(queryist cli.Queryist, ctx *sql.Context, name, email string) error {
	//TODO DO I NEED TO CHECK PERMISSIONS

	//disable foreign key checks
	_, _, _, err := queryist.Query(ctx, "SET FOREIGN_KEY_CHECKS=0;")
	if err != nil {
		return err
	}

	ciTables := ExpectedDoltCITablesOrdered.ActiveTableNames()
	for _, tableName := range ciTables {
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName.Name)
		_, err := commands.GetRowsForSql(queryist, ctx, query)
		if err != nil {
			return err
		}
	}

	_, _, _, err = queryist.Query(ctx, "SET FOREIGN_KEY_CHECKS=1;")
	if err != nil {
		return err
	}
	return commitCIDestroy(queryist, ctx, ciTables, name, email)
}

func CreateDoltCITables(queryist cli.Queryist, sqlCtx *sql.Context, name, email string) error {
	//TODO DO I NEED TO CHECK PERMISSIONS

	orderedCreateTableQueries := []string{
		createWorkflowsTableQuery(),
		createWorkflowEventsTableQuery(),
		createWorkflowEventTriggersTableQuery(),
		createWorkflowEventTriggerBranchesTableQuery(),
		createWorkflowJobsTableQuery(),
		createWorkflowStepsTableQuery(),
		createWorkflowSavedQueryStepsTableQuery(),
		createWorkflowSavedQueryStepExpectedRowColumnResultsTableQuery(),
		deleteAllFromWorkflowsTableQuery(), // as last step run delete to create resolve all indexes/fks
	}

	_, _, _, err := queryist.Query(sqlCtx, "set @@dolt_allow_ci_creation = 1")
	if err != nil {
		return err
	}

	for _, query := range orderedCreateTableQueries {
		_, err = commands.GetRowsForSql(queryist, sqlCtx, query)
		if err != nil {
			return err
		}
	}

	_, _, _, err = queryist.Query(sqlCtx, "set @@dolt_allow_ci_creation = 0")
	if err != nil {
		return err
	}

	return commitCIInit(sqlCtx, queryist, ExpectedDoltCITablesOrdered.ActiveTableNames(), name, email)
}

func createWorkflowsTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(2048) collate utf8mb4_0900_ai_ci primary key, `%s` datetime(6) not null, `%s` datetime(6) not null);", doltdb.WorkflowsTableName, doltdb.WorkflowsNameColName, doltdb.WorkflowsCreatedAtColName, doltdb.WorkflowsUpdatedAtColName)
}

func createWorkflowEventsTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(36) primary key, `%s` int not null, `%s` varchar(2048) collate utf8mb4_0900_ai_ci not null, foreign key (`%s`) references %s (`%s`) on delete cascade);", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsIdPkColName, doltdb.WorkflowEventsEventTypeColName, doltdb.WorkflowEventsWorkflowNameFkColName, doltdb.WorkflowEventsWorkflowNameFkColName, doltdb.WorkflowsTableName, doltdb.WorkflowsNameColName)
}

func createWorkflowEventTriggersTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(36) primary key, `%s` int not null, `%s` varchar(36) not null, foreign key (`%s`) references %s (`%s`) on delete cascade);", doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersIdPkColName, doltdb.WorkflowEventTriggersEventTriggerTypeColName, doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName, doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName, doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsIdPkColName)
}

func createWorkflowEventTriggerBranchesTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(36) primary key, `%s` varchar(1024) collate utf8mb4_0900_ai_ci not null, `%s` varchar(36) not null, foreign key (`%s`) references %s (`%s`) on delete cascade);", doltdb.WorkflowEventTriggerBranchesTableName, doltdb.WorkflowEventTriggerBranchesIdPkColName, doltdb.WorkflowEventTriggerBranchesBranchColName, doltdb.WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName, doltdb.WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName, doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersIdPkColName)
}

func createWorkflowJobsTableQuery() string {
	return fmt.Sprintf("create table %s (`%s` varchar(36) primary key, `%s` varchar(1024) collate utf8mb4_0900_ai_ci not null, `%s` datetime(6) not null, `%s` datetime(6) not null, `%s` varchar(2048) collate utf8mb4_0900_ai_ci not null, foreign key (`%s`) references %s (`%s`) on delete cascade);", doltdb.WorkflowJobsTableName, doltdb.WorkflowJobsIdPkColName, doltdb.WorkflowJobsNameColName, doltdb.WorkflowJobsCreatedAtColName, doltdb.WorkflowJobsUpdatedAtColName, doltdb.WorkflowJobsWorkflowNameFkColName, doltdb.WorkflowJobsWorkflowNameFkColName, doltdb.WorkflowsTableName, doltdb.WorkflowsNameColName)
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
