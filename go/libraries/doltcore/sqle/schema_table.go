// Copyright 2020 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

var errDoltSchemasTableFormat = fmt.Errorf("`%s` schema in unexpected format", doltdb.SchemasTableName)
var noSchemaIndexDefined = fmt.Errorf("could not find index `%s` on system table `%s`", doltdb.SchemasTablesIndexName, doltdb.SchemasTableName)

// The fixed dolt schema for the `dolt_schemas` table.
func SchemasTableSchema() schema.Schema {
	typeCol, err := schema.NewColumnWithTypeInfo(doltdb.SchemasTablesTypeCol, schema.DoltSchemasTypeTag, typeinfo.LegacyStringDefaultType, false, "", false, "")
	if err != nil {
		panic(err)
	}
	nameCol, err := schema.NewColumnWithTypeInfo(doltdb.SchemasTablesNameCol, schema.DoltSchemasNameTag, typeinfo.LegacyStringDefaultType, false, "", false, "")
	if err != nil {
		panic(err)
	}
	fragmentCol, err := schema.NewColumnWithTypeInfo(doltdb.SchemasTablesFragmentCol, schema.DoltSchemasFragmentTag, typeinfo.LegacyStringDefaultType, false, "", false, "")
	if err != nil {
		panic(err)
	}
	idCol, err := schema.NewColumnWithTypeInfo(doltdb.SchemasTablesIdCol, schema.DoltSchemasIdTag, typeinfo.Int64Type, true, "", false, "", schema.NotNullConstraint{})
	if err != nil {
		panic(err)
	}
	colColl := schema.NewColCollection(typeCol, nameCol, fragmentCol, idCol)
	return schema.MustSchemaFromCols(colColl)
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
	table, err := schemasTable.doltTable(ctx)
	if err != nil {
		return nil, nil, err
	}

	rowData, err := table.GetNomsRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	id := int64(1)
	err = rowData.IterAll(ctx, func(key, val types.Value) error {
		dRow, err := row.FromNoms(schemasTable.sch, key.(types.Tuple), val.(types.Tuple))
		if err != nil {
			return err
		}
		sqlRow, err := sqlutil.DoltRowToSqlRow(dRow, schemasTable.sch)
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

func nextSchemasTableIndex(ctx *sql.Context, root *doltdb.RootValue) (int64, error) {
	tbl, _, err := root.GetTable(ctx, doltdb.SchemasTableName)
	if err != nil {
		return 0, err
	}

	rows, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return 0, err
	}

	idx := int64(1)
	if rows.Len() > 0 {
		keyTpl, _, err := rows.Last(ctx)
		if err != nil {
			return 0, err
		}
		if keyTpl != nil {
			key, err := keyTpl.(types.Tuple).Get(1)
			if err != nil {
				return 0, err
			}
			idx = int64(key.(types.Int)) + 1
		}
	}
	return idx, nil
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
		return nil, false, noSchemaIndexDefined
	}

	exprs := fragNameIndex.Expressions()
	lookup, err := sql.NewIndexBuilder(ctx, fragNameIndex).Equals(ctx, exprs[0], fragType).Equals(ctx, exprs[1], name).Build(ctx)
	if err != nil {
		return nil, false, err
	}

	iter, err := index.RowIterForIndexLookup(ctx, lookup, nil)
	if err != nil {
		return nil, false, err
	}

	defer func() {
		if cerr := iter.Close(ctx); cerr != nil {
			err = cerr
		}
	}()

	sqlRow, err := iter.Next(ctx)
	if err == nil {
		return sqlRow, true, nil
	} else if err == io.EOF {
		return nil, false, nil
	} else {
		return nil, false, err
	}
}

type schemaFragment struct {
	name     string
	fragment string
}

func getSchemaFragmentsOfType(ctx *sql.Context, tbl *doltdb.Table, fragmentType string) ([]schemaFragment, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	typeCol, ok := sch.GetAllCols().GetByName(doltdb.SchemasTablesTypeCol)
	if !ok {
		return nil, errDoltSchemasTableFormat
	}
	nameCol, ok := sch.GetAllCols().GetByName(doltdb.SchemasTablesNameCol)
	if !ok {
		return nil, errDoltSchemasTableFormat
	}
	fragCol, ok := sch.GetAllCols().GetByName(doltdb.SchemasTablesFragmentCol)
	if !ok {
		return nil, errDoltSchemasTableFormat
	}

	rowData, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	var fragments []schemaFragment
	err = rowData.Iter(ctx, func(key, val types.Value) (stop bool, err error) {
		dRow, err := row.FromNoms(sch, key.(types.Tuple), val.(types.Tuple))
		if err != nil {
			return true, err
		}
		if typeColVal, ok := dRow.GetColVal(typeCol.Tag); ok && typeColVal.Equals(types.String(fragmentType)) {
			name, ok := dRow.GetColVal(nameCol.Tag)
			if !ok {
				taggedVals, _ := dRow.TaggedValues()
				return true, fmt.Errorf("missing `%s` value for view row: (%s)", doltdb.SchemasTablesNameCol, taggedVals)
			}
			def, ok := dRow.GetColVal(fragCol.Tag)
			if !ok {
				taggedVals, _ := dRow.TaggedValues()
				return true, fmt.Errorf("missing `%s` value for view row: (%s)", doltdb.SchemasTablesFragmentCol, taggedVals)
			}
			fragments = append(fragments, schemaFragment{
				name:     string(name.(types.String)),
				fragment: string(def.(types.String)),
			})
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	return fragments, nil
}
