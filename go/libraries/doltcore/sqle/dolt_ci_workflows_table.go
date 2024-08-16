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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	stypes "github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
)

type doltCIWorkflowsTableCreator struct{}

var _ DoltCITableCreator = (*doltCIWorkflowsTableCreator)(nil)

func NewDoltCIWorkflowsTableCreator() *doltCIWorkflowsTableCreator {
	return &doltCIWorkflowsTableCreator{}
}

func (d *doltCIWorkflowsTableCreator) CreateTable(ctx *sql.Context) error {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	dbState, ok, err := dSess.LookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("no root value found in session")
	}

	roots, _ := dSess.GetRoots(ctx, dbName)

	found, err := roots.Working.HasTable(ctx, doltdb.TableName{Name: doltdb.WorkflowsTableName})
	if err != nil {
		return err
	}

	if found {
		return nil
	}

	colCollection := schema.NewColCollection(
		schema.Column{
			Name:          doltdb.WorkflowsNameColName,
			Tag:           schema.WorkflowsNameTag,
			Kind:          stypes.StringKind,
			IsPartOfPK:    true,
			TypeInfo:      typeinfo.FromKind(stypes.StringKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
		schema.Column{
			Name:          doltdb.WorkflowsCreatedAtColName,
			Tag:           schema.WorkflowsCreatedAtTag,
			Kind:          stypes.TimestampKind,
			IsPartOfPK:    false,
			TypeInfo:      typeinfo.FromKind(stypes.TimestampKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
		schema.Column{
			Name:          doltdb.WorkflowsUpdatedAtColName,
			Tag:           schema.WorkflowsUpdatedAtTag,
			Kind:          stypes.TimestampKind,
			IsPartOfPK:    false,
			TypeInfo:      typeinfo.FromKind(stypes.TimestampKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
	)

	newSchema, err := schema.NewSchema(colCollection, nil, schema.Collation_Default, nil, nil)
	if err != nil {
		return err
	}

	// underlying table doesn't exist. Record this, then create the table.
	nrv, err := doltdb.CreateEmptyTable(ctx, roots.Working, doltdb.TableName{Name: doltdb.WorkflowsTableName}, newSchema)
	if err != nil {
		return err
	}

	if ws := dbState.WriteSession(); ws != nil {
		err = ws.SetWorkingSet(ctx, dbState.WorkingSet().WithWorkingRoot(nrv))
		if err != nil {
			return err
		}
	}

	err = dSess.SetWorkingRoot(ctx, dbName, nrv)
	if err != nil {
		return err
	}

	// this doesnt work
	if ws := dbState.WriteSession(); ws != nil {
		tableWriter, err := ws.GetTableWriter(ctx, doltdb.TableName{Name: doltdb.WorkflowsTableName}, dbName, dSess.SetWorkingRoot)
		if err != nil {
			return err
		}
		tableWriter.StatementBegin(ctx)
		defer tableWriter.Close(ctx)

		return tableWriter.StatementComplete(ctx)
	}
	return nil
}
