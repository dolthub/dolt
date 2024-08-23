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
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	stypes "github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
)

type doltCIWorkflowEventsTableCreator struct {
}

var _ DoltCITableCreator = (*doltCIWorkflowEventsTableCreator)(nil)

func NewDoltCIWorkflowEventsTableCreator() *doltCIWorkflowEventsTableCreator {
	return &doltCIWorkflowEventsTableCreator{}
}

func (d *doltCIWorkflowEventsTableCreator) CreateTable(ctx *sql.Context) error {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return err
	}

	startHash, err := ws.HashOf()
	if err != nil {
		return err
	}

	fmt.Fprintf(color.Output, "workflow events working set hash at start: %s\n", startHash)

	roots, _ := dSess.GetRoots(ctx, dbName)

	found, err := roots.Working.HasTable(ctx, doltdb.TableName{Name: doltdb.WorkflowEventsTableName})
	if err != nil {
		return err
	}
	if found {
		return nil
	}

	colCollection := schema.NewColCollection(
		schema.Column{
			Name:          doltdb.WorkflowEventsIdPkColName,
			Tag:           schema.WorkflowEventsIdTag,
			Kind:          stypes.StringKind,
			IsPartOfPK:    true,
			TypeInfo:      typeinfo.FromKind(stypes.StringKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
		schema.Column{
			Name:          doltdb.WorkflowEventsWorkflowNameFkColName,
			Tag:           schema.WorkflowEventsWorkflowNameFkTag,
			Kind:          stypes.StringKind,
			IsPartOfPK:    false,
			TypeInfo:      typeinfo.FromKind(stypes.StringKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
		schema.Column{
			Name:          doltdb.WorkflowEventsEventTypeColName,
			Tag:           schema.WorkflowEventsEventTypeTag,
			Kind:          stypes.IntKind,
			IsPartOfPK:    false,
			TypeInfo:      typeinfo.FromKind(stypes.IntKind),
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
	nrv, err := doltdb.CreateEmptyTable(ctx, roots.Working, doltdb.TableName{Name: doltdb.WorkflowEventsTableName}, sch)
	if err != nil {
		return err
	}

	sfkc := sql.ForeignKeyConstraint{
		Name:           fmt.Sprintf("%s_%s", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsWorkflowNameFkColName),
		Database:       dbName,
		Table:          doltdb.WorkflowEventsTableName,
		Columns:        []string{doltdb.WorkflowEventsWorkflowNameFkColName},
		ParentDatabase: dbName,
		ParentTable:    doltdb.WorkflowsTableName,
		ParentColumns:  []string{doltdb.WorkflowsNameColName},
		OnDelete:       sql.ForeignKeyReferentialAction_Cascade,
		OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
		IsResolved:     false,
	}

	onUpdateRefAction, err := ParseFkReferentialAction(sfkc.OnUpdate)
	if err != nil {
		return err
	}

	onDeleteRefAction, err := ParseFkReferentialAction(sfkc.OnDelete)
	if err != nil {
		return err
	}

	vrw := nrv.VRW()
	ns := nrv.NodeStore()

	empty, err := durable.NewEmptyIndex(ctx, vrw, ns, sch)
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

	nrv, err = nrv.PutTable(ctx, doltdb.TableName{Name: doltdb.WorkflowEventsTableName}, tbl)
	if err != nil {
		return err
	}

	doltFk, err := CreateDoltCITableForeignKey(ctx, nrv, tbl, sch, sfkc, onUpdateRefAction, onDeleteRefAction, dbName)
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

	//newWorkingSet := dbState.WorkingSet().WithWorkingRoot(nrv)
	newWorkingSet := ws.WithWorkingRoot(nrv)
	err = dSess.SetWorkingSet(ctx, dbName, newWorkingSet)
	if err != nil {
		return err
	}

	err = dSess.SetWorkingRoot(ctx, dbName, nrv)
	if err != nil {
		return err
	}

	//newWorkingSetRef := newWorkingSet.Ref()
	//ddb, exists := dSess.GetDoltDB(ctx, dbName)
	//if !exists {
	//	return fmt.Errorf("database not found in database %s", dbName)
	//}

	////oldHash, err := newWorkingSet.HashOf()
	////if err != nil {
	////	return err
	////}
	//
	//oldHash, err := dbState.WorkingSet().HashOf()
	//if err != nil {
	//	return err
	//}

	//err = ddb.UpdateWorkingSet(ctx, newWorkingSetRef, newWorkingSet, startHash, doltdb.TodoWorkingSetMeta(), nil)
	//if err != nil {
	//	return err
	//}

	nws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return err
	}

	endHash, err := nws.HashOf()
	if err != nil {
		return err
	}

	fmt.Fprintf(color.Output, "workflow events working set hash at end: %s\n", endHash)
	return nil

}
