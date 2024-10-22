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

package sqle

import (
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"time"
)

var ExpectedDoltCITables = []string{
	doltdb.WorkflowsTableName,
	doltdb.WorkflowEventsTableName,
}

type DoltCITablesCreator interface {
	// HasTables is used to check whether the database
	// already contains dolt ci tables. If any expected tables are missing,
	// an error is returned
	HasTables(ctx *sql.Context) (bool, error)

	// CreateTables creates all tables required for dolt ci
	CreateTables(ctx *sql.Context) error
}

type doltCITablesCreator struct {
	ctx              *sql.Context
	db               Database
	commiterName     string
	commiterEmail    string
	workflowsTC      DoltCITableCreator
	workflowEventsTC DoltCITableCreator
}

func NewDoltCITablesCreator(ctx *sql.Context, db Database, committerName, commiterEmail string) *doltCITablesCreator {
	return &doltCITablesCreator{
		ctx:              ctx,
		db:               db,
		commiterName:     committerName,
		commiterEmail:    commiterEmail,
		workflowsTC:      NewDoltCIWorkflowsTableCreator(),
		workflowEventsTC: NewDoltCIWorkflowEventsTableCreator(),
	}
}

func (d *doltCITablesCreator) createTables(ctx *sql.Context) error {
	// TOD0: maybe CreateTable(...) should take in old RootVal and return new RootVal?
	err := d.workflowsTC.CreateTable(ctx)
	if err != nil {
		return err
	}
	return d.workflowEventsTC.CreateTable(ctx)
}

func (d *doltCITablesCreator) HasTables(ctx *sql.Context) (bool, error) {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)
	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return false, err
	}

	root := ws.WorkingRoot()

	for _, tableName := range ExpectedDoltCITables {
		found, err := root.HasTable(ctx, doltdb.TableName{Name: tableName})
		if err != nil {
			return false, err
		}
		if !found {
			return false, fmt.Errorf("required dolt ci table `%s` not found", doltdb.WorkflowsTableName)
		}
	}

	return true, nil
}

func (d *doltCITablesCreator) CreateTables(ctx *sql.Context) error {
	if err := dsess.CheckAccessForDb(d.ctx, d.db, branch_control.Permissions_Write); err != nil {
		return err
	}

	err := d.createTables(ctx)
	if err != nil {
		return err
	}

	// update doltdb workingset exactly once
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)
	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return err
	}
	wsHash, err := ws.HashOf() // TODO: setting the WorkingRoot of WorkingSet has no impact on hash
	if err != nil {
		return err
	}
	ddb, exists := dSess.GetDoltDB(ctx, dbName)
	if !exists {
		return fmt.Errorf("database not found in database %s", dbName)
	}
	err = ddb.UpdateWorkingSet(ctx, ws.Ref(), ws, wsHash, doltdb.TodoWorkingSetMeta(), nil)
	if err != nil {
		return err
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return fmt.Errorf("failed to get roots from session in database: %s", dbName)
	}

	t := time.Now()
	pendingCommit, err := dSess.NewPendingCommit(ctx, dbName, roots, actions.CommitStagedProps{
		Message: "Initialize Dolt CI",
		Date:    t,
		Name:    d.commiterName,
		Email:   d.commiterEmail,
	})
	if err != nil {
		return err
	}

	_, err = dSess.DoltCommit(ctx, dbName, dSess.GetTransaction(), pendingCommit)
	if err != nil {
		return err
	}

	return nil
}

var _ DoltCITablesCreator = &doltCITablesCreator{}
