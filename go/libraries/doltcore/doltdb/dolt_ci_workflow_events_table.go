// Copyright 2019 Dolthub, Inc.
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

package doltdb

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	stypes "github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
)

type doltCIWorkflowEventsTableCreator struct {
	dbName string
	//schemaName string
}

var _ DoltCITableCreator = (*doltCIWorkflowEventsTableCreator)(nil)

func NewDoltCIWorkflowEventsTableCreator(dbName string) *doltCIWorkflowEventsTableCreator {
	return &doltCIWorkflowEventsTableCreator{dbName: dbName}
}

func (d *doltCIWorkflowEventsTableCreator) CreateTable(ctx context.Context, rv RootValue) (RootValue, error) {
	found, err := rv.HasTable(ctx, TableName{Name: WorkflowEventsTableName})
	if err != nil {
		return nil, err
	}
	if found {
		return rv, nil
	}

	colCollection := schema.NewColCollection(
		schema.Column{
			Name:          WorkflowEventsIdPkColName,
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
			Name:          WorkflowEventsWorkflowNameFkColName,
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
			Name:          WorkflowEventsEventTypeColName,
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
		return nil, err
	}

	// underlying table doesn't exist. Record this, then create the table.
	newRootValue, err := CreateEmptyTable(ctx, rv, TableName{Name: WorkflowEventsTableName}, sch)
	if err != nil {
		return nil, err
	}

	sfkc := sql.ForeignKeyConstraint{
		Name:           fmt.Sprintf("%s_%s", WorkflowEventsTableName, WorkflowEventsWorkflowNameFkColName),
		Database:       d.dbName,
		Table:          WorkflowEventsTableName,
		Columns:        []string{WorkflowEventsWorkflowNameFkColName},
		ParentDatabase: d.dbName,
		ParentTable:    WorkflowsTableName,
		ParentColumns:  []string{WorkflowsNameColName},
		OnDelete:       sql.ForeignKeyReferentialAction_Cascade,
		OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
		IsResolved:     false,
	}

	onUpdateRefAction, err := ParseFkReferentialAction(sfkc.OnUpdate)
	if err != nil {
		return nil, err
	}

	onDeleteRefAction, err := ParseFkReferentialAction(sfkc.OnDelete)
	if err != nil {
		return nil, err
	}

	vrw := newRootValue.VRW()
	ns := newRootValue.NodeStore()

	empty, err := durable.NewEmptyIndex(ctx, vrw, ns, sch)
	if err != nil {
		return nil, errhand.BuildDError("error: failed to get table.").AddCause(err).Build()
	}

	indexSet, err := durable.NewIndexSetWithEmptyIndexes(ctx, vrw, ns, sch)
	if err != nil {
		return nil, errhand.BuildDError("error: failed to get table.").AddCause(err).Build()
	}

	tbl, err := NewTable(ctx, vrw, ns, sch, empty, indexSet, nil)
	if err != nil {
		return nil, err
	}

	newRootValue, err = newRootValue.PutTable(ctx, TableName{Name: WorkflowEventsTableName}, tbl)
	if err != nil {
		return nil, err
	}

	dbSchemas, err := rv.GetDatabaseSchemas(ctx)
	if err != nil {
		return nil, err
	}

	// todo: wtf bro
	schemaName := ""
	if len(dbSchemas) > 0 {
		schemaName = dbSchemas[0].Name
	}

	doltFk, err := CreateDoltCITableForeignKey(ctx, newRootValue, tbl, sch, sfkc, onUpdateRefAction, onDeleteRefAction, schemaName)
	if err != nil {
		return nil, err
	}

	fkc, err := newRootValue.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	err = fkc.AddKeys(doltFk)
	if err != nil {
		return nil, err
	}

	return newRootValue.PutForeignKeyCollection(ctx, fkc)
}

func ParseFkReferentialAction(refOp sql.ForeignKeyReferentialAction) (ForeignKeyReferentialAction, error) {
	switch refOp {
	case sql.ForeignKeyReferentialAction_DefaultAction:
		return ForeignKeyReferentialAction_DefaultAction, nil
	case sql.ForeignKeyReferentialAction_Restrict:
		return ForeignKeyReferentialAction_Restrict, nil
	case sql.ForeignKeyReferentialAction_Cascade:
		return ForeignKeyReferentialAction_Cascade, nil
	case sql.ForeignKeyReferentialAction_NoAction:
		return ForeignKeyReferentialAction_NoAction, nil
	case sql.ForeignKeyReferentialAction_SetNull:
		return ForeignKeyReferentialAction_SetNull, nil
	case sql.ForeignKeyReferentialAction_SetDefault:
		return ForeignKeyReferentialAction_DefaultAction, sql.ErrForeignKeySetDefault.New()
	default:
		return ForeignKeyReferentialAction_DefaultAction, fmt.Errorf("unknown foreign key referential action: %v", refOp)
	}
}
