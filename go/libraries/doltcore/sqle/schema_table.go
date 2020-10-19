// Copyright 2020 Liquidata, Inc.
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
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	sqleSchema "github.com/dolthub/dolt/go/libraries/doltcore/sqle/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// The fixed SQL schema for the `dolt_schemas` table.
func SchemasTableSqlSchema() sql.Schema {
	sqlSchema, err := sqleSchema.FromDoltSchema(doltdb.SchemasTableName, SchemasTableSchema())
	if err != nil {
		panic(err) // should never happen
	}
	return sqlSchema
}

// The fixed dolt schema for the `dolt_schemas` table.
func SchemasTableSchema() schema.Schema {
	colColl, err := schema.NewColCollection(
		schema.NewColumn(doltdb.SchemasTablesTypeCol, doltdb.DoltSchemasTypeTag, types.StringKind, false, "", false, ""),
		schema.NewColumn(doltdb.SchemasTablesNameCol, doltdb.DoltSchemasNameTag, types.StringKind, false, "", false, ""),
		schema.NewColumn(doltdb.SchemasTablesFragmentCol, doltdb.DoltSchemasFragmentTag, types.StringKind, false, "", false, ""),
		schema.NewColumn(doltdb.SchemasTablesIdCol, doltdb.DoltSchemasIdTag, types.IntKind, true, "", false, "", schema.NotNullConstraint{}),
	)
	if err != nil {
		panic(err) // should never happen
	}
	return schema.SchemaFromCols(colColl)
}

// GetOrCreateDoltSchemasTable returns the `dolt_schemas` table in `db`, creating it if it does not already exist.
func GetOrCreateDoltSchemasTable(ctx *sql.Context, db Database) (retTbl *WritableDoltTable, retErr error) {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}
	tbl, found, err := db.GetTableInsensitiveWithRoot(ctx, root, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}
	var rowsToAdd []sql.Row
	if found {
		schemasTable := tbl.(*WritableDoltTable)
		// Old schemas table does not contain the `id` column.
		if !tbl.Schema().Contains(doltdb.SchemasTablesIdCol, doltdb.SchemasTableName) {
			root, rowsToAdd, err = migrateOldSchemasTableToNew(ctx, db, root, schemasTable)
			if err != nil {
				return nil, err
			}
		} else {
			return schemasTable, nil
		}
	}
	// Create the schemas table as an empty table
	err = db.createDoltTable(ctx, doltdb.SchemasTableName, root, SchemasTableSchema())
	if err != nil {
		return nil, err
	}
	root, err = db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}
	tbl, found, err = db.GetTableInsensitiveWithRoot(ctx, root, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sql.ErrTableNotFound.New("dolt_schemas")
	}
	// Create a unique index on the old primary key columns (type, name)
	err = (&AlterableDoltTable{*tbl.(*WritableDoltTable)}).CreateIndex(ctx,
		doltdb.SchemasTablesIndexName,
		sql.IndexUsing_Default,
		sql.IndexConstraint_Unique,
		[]sql.IndexColumn{
			{Name: doltdb.SchemasTablesTypeCol, Length: 0},
			{Name: doltdb.SchemasTablesNameCol, Length: 0},
		},
		"",
	)
	if err != nil {
		return nil, err
	}
	// If there was an old schemas table that contained rows, then add that data here
	root, err = db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}
	tbl, found, err = db.GetTableInsensitiveWithRoot(ctx, root, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sql.ErrTableNotFound.New("dolt_schemas")
	}
	if len(rowsToAdd) != 0 {
		err = func() (retErr error) {
			inserter := tbl.(*WritableDoltTable).Inserter(ctx)
			defer func() {
				err := inserter.Close(ctx)
				if retErr == nil {
					retErr = err
				}
			}()
			for _, sqlRow := range rowsToAdd {
				err = inserter.Insert(ctx, sqlRow)
				if err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}
	return tbl.(*WritableDoltTable), nil
}

func migrateOldSchemasTableToNew(
	ctx *sql.Context,
	db Database,
	root *doltdb.RootValue,
	schemasTable *WritableDoltTable,
) (
	*doltdb.RootValue,
	[]sql.Row,
	error,
) {
	// Copy all of the old data over and add an index column
	var rowsToAdd []sql.Row
	rowData, err := schemasTable.table.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	id := int64(1)
	err = rowData.IterAll(ctx, func(key, val types.Value) error {
		dRow, err := row.FromNoms(schemasTable.sch, key.(types.Tuple), val.(types.Tuple))
		if err != nil {
			return err
		}
		sqlRow, err := doltRowToSqlRow(dRow, schemasTable.sch)
		if err != nil {
			return err
		}
		// prepend the new id to each row
		sqlRow = append(sqlRow, id)
		rowsToAdd = append(rowsToAdd, sqlRow)
		id++
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	err = db.DropTable(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, nil, err
	}
	root, err = db.GetRoot(ctx)
	if err != nil {
		return nil, nil, err
	}
	return root, rowsToAdd, nil
}

// fragFromSchemasTable returns the row with the given schema fragment if it exists.
func fragFromSchemasTable(ctx *sql.Context, tbl *WritableDoltTable, fragType string, name string) (sql.Row, bool, error) {
	indexes, err := tbl.GetIndexes(ctx)
	if err != nil {
		return nil, false, err
	}
	var fragNameIndex sql.Index
	for _, index := range indexes {
		if index.ID() == doltdb.SchemasTablesIndexName {
			fragNameIndex = index
			break
		}
	}
	if fragNameIndex == nil {
		return nil, false, fmt.Errorf("could not find index `%s` on system table `%s`", doltdb.SchemasTablesIndexName, doltdb.SchemasTableName)
	}

	indexLookup, err := fragNameIndex.Get(fragType, name)
	if err != nil {
		return nil, false, err
	}
	rowIter, err := indexLookup.(*doltIndexLookup).RowIter(ctx)
	if err != nil {
		return nil, false, err
	}
	defer rowIter.Close()
	sqlRow, err := rowIter.Next()
	if err == nil {
		return sqlRow, true, nil
	} else if err == io.EOF {
		return nil, false, nil
	} else {
		return nil, false, err
	}
}
