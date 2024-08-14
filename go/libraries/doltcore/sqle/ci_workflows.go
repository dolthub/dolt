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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	stypes "github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.StatisticsTable = (*WorkflowsTable)(nil)
var _ sql.Table = (*WorkflowsTable)(nil)
var _ dtables.VersionableTable = (*WorkflowsTable)(nil)

// WorkflowsTable is a sql.Table implementation that implements a system table which stores dolt CI workflows
type WorkflowsTable struct {
	*WritableDoltTable
}

// NewWorkflowsTable creates a WorkflowsTable
func NewWorkflowsTable(_ *sql.Context, backingTable *WritableDoltTable) sql.Table {
	return &WorkflowsTable{backingTable}
}

// NewEmptyWorkflowsTable creates a WorkflowsTable
func NewEmptyWorkflowsTable(ctx *sql.Context, db Database, rv doltdb.RootValue) (sql.Table, error) {
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

	sch, err := schema.NewSchema(colCollection, nil, schema.Collation_Default, nil, nil)
	if err != nil {
		return nil, err
	}

	//empty, err := durable.NewEmptyIndex(ctx, rv.VRW(), rv.NodeStore(), sch)
	//if err != nil {
	//	return nil, errhand.BuildDError("error: failed to get table.").AddCause(err).Build()
	//}
	//
	//indexSet, err := durable.NewIndexSetWithEmptyIndexes(ctx, rv.VRW(), rv.NodeStore(), sch)
	//if err != nil {
	//	return nil, errhand.BuildDError("error: failed to get table.").AddCause(err).Build()
	//}
	//
	//tbl, err := doltdb.NewTable(ctx, rv.VRW(), rv.NodeStore(), sch, empty, indexSet, nil)
	//if err != nil {
	//	return nil, err
	//}
	//
	//dt, err := NewDoltTable(doltdb.WorkflowsTableName, sch, tbl, db, db.editOpts)
	//if err != nil {
	//	return nil, err
	//}

	tbl, err := doltdb.NewEmptyTable(ctx, rv.VRW(), rv.NodeStore(), sch)
	if err != nil {
		return nil, err
	}

	dt, err := NewDoltTable(doltdb.WorkflowsTableName, sch, tbl, db, db.editOpts)
	if err != nil {
		return nil, err
	}

	return &WorkflowsTable{&WritableDoltTable{DoltTable: dt}}, nil
}
