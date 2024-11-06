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
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func createDoltCITableForeignKey(
	ctx context.Context,
	root doltdb.RootValue,
	tbl *doltdb.Table,
	sch schema.Schema,
	sqlFk sql.ForeignKeyConstraint,
	onUpdateRefAction, onDeleteRefAction doltdb.ForeignKeyReferentialAction,
	schemaName string) (doltdb.ForeignKey, error) {
	if !sqlFk.IsResolved {
		return doltdb.ForeignKey{
			Name:                   sqlFk.Name,
			TableName:              doltdb.TableName{Name: sqlFk.Table},
			TableIndex:             "",
			TableColumns:           nil,
			ReferencedTableName:    doltdb.TableName{Name: sqlFk.ParentTable},
			ReferencedTableIndex:   "",
			ReferencedTableColumns: nil,
			OnUpdate:               onUpdateRefAction,
			OnDelete:               onDeleteRefAction,
			UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
				TableColumns:           sqlFk.Columns,
				ReferencedTableColumns: sqlFk.ParentColumns,
			},
		}, nil
	}
	colTags := make([]uint64, len(sqlFk.Columns))
	for i, col := range sqlFk.Columns {
		tableCol, ok := sch.GetAllCols().GetByNameCaseInsensitive(col)
		if !ok {
			return doltdb.ForeignKey{}, fmt.Errorf("table `%s` does not have column `%s`", sqlFk.Table, col)
		}
		colTags[i] = tableCol.Tag
	}

	var refTbl *doltdb.Table
	var refSch schema.Schema
	if sqlFk.IsSelfReferential() {
		refTbl = tbl
		refSch = sch
	} else {
		var ok bool
		var err error
		// TODO: the parent table can be in another schema

		refTbl, _, ok, err = doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: sqlFk.ParentTable, Schema: schemaName})
		if err != nil {
			return doltdb.ForeignKey{}, err
		}
		if !ok {
			return doltdb.ForeignKey{}, fmt.Errorf("referenced table `%s` does not exist", sqlFk.ParentTable)
		}
		refSch, err = refTbl.GetSchema(ctx)
		if err != nil {
			return doltdb.ForeignKey{}, err
		}
	}

	refColTags := make([]uint64, len(sqlFk.ParentColumns))
	for i, name := range sqlFk.ParentColumns {
		refCol, ok := refSch.GetAllCols().GetByNameCaseInsensitive(name)
		if !ok {
			return doltdb.ForeignKey{}, fmt.Errorf("table `%s` does not have column `%s`", sqlFk.ParentTable, name)
		}
		refColTags[i] = refCol.Tag
	}

	var tableIndexName, refTableIndexName string
	tableIndex, ok, err := sqle.FindIndexWithPrefix(sch, sqlFk.Columns)
	if err != nil {
		return doltdb.ForeignKey{}, err
	}
	// Use secondary index if found; otherwise it will use empty string, indicating primary key
	if ok {
		tableIndexName = tableIndex.Name()
	}
	refTableIndex, ok, err := sqle.FindIndexWithPrefix(refSch, sqlFk.ParentColumns)
	if err != nil {
		return doltdb.ForeignKey{}, err
	}
	// Use secondary index if found; otherwise it will use  empty string, indicating primary key
	if ok {
		refTableIndexName = refTableIndex.Name()
	}
	return doltdb.ForeignKey{
		Name:                   sqlFk.Name,
		TableName:              doltdb.TableName{Name: sqlFk.Table},
		TableIndex:             tableIndexName,
		TableColumns:           colTags,
		ReferencedTableName:    doltdb.TableName{Name: sqlFk.ParentTable},
		ReferencedTableIndex:   refTableIndexName,
		ReferencedTableColumns: refColTags,
		OnUpdate:               onUpdateRefAction,
		OnDelete:               onDeleteRefAction,
		UnresolvedFKDetails: doltdb.UnresolvedFKDetails{
			TableColumns:           sqlFk.Columns,
			ReferencedTableColumns: sqlFk.ParentColumns,
		},
	}, nil
}
