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
"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
)

type DoltCITablesCreator interface {
	CreateTables(ctx *sql.Context) error
}

type doltCITablesCreator struct {
	ctx              *sql.Context
	db               Database
	workflowsTC      DoltCITableCreator
	workflowEventsTC DoltCITableCreator
}

func NewDoltCITablesCreator(ctx *sql.Context, db Database) *doltCITablesCreator {
	return &doltCITablesCreator{
		ctx:              ctx,
		db:               db,
		workflowsTC:      NewDoltCIWorkflowsTableCreator(),
		workflowEventsTC: NewDoltCIWorkflowEventsTableCreator(),
	}
}

func (d doltCITablesCreator) CreateTables(ctx *sql.Context) error {
	if err := dsess.CheckAccessForDb(d.ctx, d.db, branch_control.Permissions_Write); err != nil {
		return err
	}

	// TOD0: maybe CreateTable(...) should take in old RootVal and return new RootVal?
	err := d.workflowsTC.CreateTable(ctx)
	if err != nil {
		return err
	}

	err = d.workflowEventsTC.CreateTable(ctx)
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

	return nil
}

var _ DoltCITablesCreator = &doltCITablesCreator{}
