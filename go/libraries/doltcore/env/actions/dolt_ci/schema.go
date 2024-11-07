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
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
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

func DestroyDoltCITables(ctx *sql.Context, db sqle.Database, queryFunc queryFunc, commiterName, commiterEmail string) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	// disable foreign key checks
	err := sqlWriteQuery(ctx, queryFunc, "SET FOREIGN_KEY_CHECKS=0;")
	if err != nil {
		return err
	}

	existing, err := getExistingDoltCITables(ctx)
	if err != nil {
		return err
	}

	for _, tableName := range existing {
		err = sqlWriteQuery(ctx, queryFunc, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName.Name))
		if err != nil {
			return err
		}
	}

	// enable foreign keys again
	err = sqlWriteQuery(ctx, queryFunc, "SET FOREIGN_KEY_CHECKS=1;")
	if err != nil {
		return err
	}

	return commitCIDestroy(ctx, queryFunc, commiterName, commiterEmail)
}

func CreateDoltCITables(ctx *sql.Context, db sqle.Database, commiterName, commiterEmail string) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	err := createDoltCITables(ctx)
	if err != nil {
		return err
	}

	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	ddb, exists := dSess.GetDoltDB(ctx, dbName)
	if !exists {
		return fmt.Errorf("database not found in database %s", dbName)
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return fmt.Errorf("roots not found in database %s", dbName)
	}

	roots, err = actions.StageTables(ctx, roots, ExpectedDoltCITablesOrdered, true)
	if err != nil {
		return err
	}

	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return err
	}

	ws = ws.WithWorkingRoot(roots.Working)
	ws = ws.WithStagedRoot(roots.Staged)

	wsHash, err := ws.HashOf()
	if err != nil {
		return err
	}

	wRef := ws.Ref()
	pRef, err := wRef.ToHeadRef()
	if err != nil {
		return err
	}

	parent, err := ddb.ResolveCommitRef(ctx, pRef)
	if err != nil {
		return err
	}

	parents := []*doltdb.Commit{parent}

	meta, err := datas.NewCommitMeta(commiterName, commiterEmail, "Successfully initialized Dolt CI")
	if err != nil {
		return err
	}

	pcm, err := ddb.NewPendingCommit(ctx, roots, parents, meta)
	if err != nil {
		return err
	}

	wsMeta := &datas.WorkingSetMeta{
		Name:      commiterName,
		Email:     commiterEmail,
		Timestamp: uint64(time.Now().Unix()),
	}
	_, err = ddb.CommitWithWorkingSet(ctx, pRef, wRef, pcm, ws, wsHash, wsMeta, nil)
	return err
}

func createDoltCITables(ctx *sql.Context) error {
	// creation order matters here
	// for foreign key creation
	err := createWorkflowsTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowEventsTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowEventTriggersTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowEventTriggerBranchesTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowEventTriggerActivitiesTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowJobsTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowStepsTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowSavedQueryStepsTable(ctx)
	if err != nil {
		return err
	}
	return createWorkflowSavedQueryStepExpectedRowColumnResultsTable(ctx)
}
