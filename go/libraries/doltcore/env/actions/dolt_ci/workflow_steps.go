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

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	stypes "github.com/dolthub/dolt/go/store/types"
)

func createWorkflowStepsTable(ctx *sql.Context) error {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)
	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return err
	}

	root := ws.WorkingRoot()

	found, err := root.HasTable(ctx, doltdb.TableName{Name: doltdb.WorkflowStepsTableName})
	if err != nil {
		return err
	}
	if found {
		return nil
	}

	colCollection := schema.NewColCollection(
		schema.Column{
			Name:          doltdb.WorkflowStepsIdPkColName,
			Tag:           schema.WorkflowStepsIdTag,
			Kind:          stypes.StringKind,
			IsPartOfPK:    true,
			TypeInfo:      typeinfo.FromKind(stypes.StringKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
		schema.Column{
			Name:          doltdb.WorkflowStepsNameColName,
			Tag:           schema.WorkflowStepsNameTag,
			Kind:          stypes.StringKind,
			IsPartOfPK:    false,
			TypeInfo:      typeinfo.FromKind(stypes.StringKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
		schema.Column{
			Name:          doltdb.WorkflowStepsWorkflowJobIdFkColName,
			Tag:           schema.WorkflowStepsWorkflowJobIdFkTag,
			Kind:          stypes.StringKind,
			IsPartOfPK:    false,
			TypeInfo:      typeinfo.FromKind(stypes.StringKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
		schema.Column{
			Name:          doltdb.WorkflowStepsStepOrderColName,
			Tag:           schema.WorkflowStepsStepOrderTag,
			Kind:          stypes.IntKind,
			IsPartOfPK:    false,
			TypeInfo:      typeinfo.FromKind(stypes.IntKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
		schema.Column{
			Name:          doltdb.WorkflowStepsStepTypeColName,
			Tag:           schema.WorkflowStepsStepTypeTag,
			Kind:          stypes.IntKind,
			IsPartOfPK:    false,
			TypeInfo:      typeinfo.FromKind(stypes.IntKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
		schema.Column{
			Name:          doltdb.WorkflowStepsCreatedAtColName,
			Tag:           schema.WorkflowStepsCreatedAtTag,
			Kind:          stypes.TimestampKind,
			IsPartOfPK:    false,
			TypeInfo:      typeinfo.FromKind(stypes.TimestampKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
		schema.Column{
			Name:          doltdb.WorkflowStepsUpdatedAtColName,
			Tag:           schema.WorkflowStepsUpdatedAtTag,
			Kind:          stypes.TimestampKind,
			IsPartOfPK:    false,
			TypeInfo:      typeinfo.FromKind(stypes.TimestampKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
	)

	sch, err := schema.NewSchema(colCollection, nil, schema.Collation_Default, nil, nil)
	if err != nil {
		return err
	}

	// underlying table doesn't exist. Record this, then create the table.
	nrv, err := doltdb.CreateEmptyTable(ctx, root, doltdb.TableName{Name: doltdb.WorkflowStepsTableName}, sch)
	if err != nil {
		return err
	}

	sfkc := sql.ForeignKeyConstraint{
		Name:           fmt.Sprintf("%s_%s", doltdb.WorkflowStepsTableName, doltdb.WorkflowStepsWorkflowJobIdFkColName),
		Database:       dbName,
		Table:          doltdb.WorkflowStepsTableName,
		Columns:        []string{doltdb.WorkflowStepsWorkflowJobIdFkColName},
		ParentDatabase: dbName,
		ParentTable:    doltdb.WorkflowJobsTableName,
		ParentColumns:  []string{doltdb.WorkflowJobsIdPkColName},
		OnDelete:       sql.ForeignKeyReferentialAction_Cascade,
		OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
		IsResolved:     false,
	}

	onUpdateRefAction, err := sqle.ParseFkReferentialAction(sfkc.OnUpdate)
	if err != nil {
		return err
	}

	onDeleteRefAction, err := sqle.ParseFkReferentialAction(sfkc.OnDelete)
	if err != nil {
		return err
	}

	vrw := nrv.VRW()
	ns := nrv.NodeStore()

	empty, err := durable.NewEmptyIndex(ctx, vrw, ns, sch, false)
	if err != nil {
		return errhand.BuildDError("error: failed to get table.").AddCause(err).Build()
	}

	indexSet, err := durable.NewIndexSetWithEmptyIndexes(ctx, vrw, ns, sch)
	if err != nil {
		return errhand.BuildDError("error: failed to get table.").AddCause(err).Build()
	}

	tbl, err := doltdb.NewTable(ctx, vrw, ns, sch, empty, indexSet, nil)
	if err != nil {
		return err
	}

	nrv, err = nrv.PutTable(ctx, doltdb.TableName{Name: doltdb.WorkflowStepsTableName}, tbl)
	if err != nil {
		return err
	}

	doltFk, err := createDoltCITableForeignKey(ctx, nrv, tbl, sch, sfkc, onUpdateRefAction, onDeleteRefAction, dbName)
	if err != nil {
		return err
	}

	fkc, err := nrv.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}

	err = fkc.AddKeys(doltFk)
	if err != nil {
		return err
	}

	nrv, err = nrv.PutForeignKeyCollection(ctx, fkc)
	if err != nil {
		return err
	}

	newWorkingSet := ws.WithWorkingRoot(nrv)
	err = dSess.SetWorkingSet(ctx, dbName, newWorkingSet)
	if err != nil {
		return err
	}

	return dSess.SetWorkingRoot(ctx, dbName, nrv)
}
