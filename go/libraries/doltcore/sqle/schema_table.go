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
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

var errDoltSchemasTableFormat = fmt.Errorf("`%s` schema in unexpected format", doltdb.SchemasTableName)
var noSchemaIndexDefined = fmt.Errorf("could not find index `%s` on system table `%s`", doltdb.SchemasTablesIndexName, doltdb.SchemasTableName)

const (
	viewFragment    = "view"
	triggerFragment = "trigger"
)

type Extra struct {
	CreatedAt int64
}

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
	extraCol, err := schema.NewColumnWithTypeInfo(doltdb.SchemasTablesExtraCol, schema.DoltSchemasExtraTag, typeinfo.JSONType, false, "", false, "")
	if err != nil {
		panic(err)
	}
	colColl := schema.NewColCollection(typeCol, nameCol, fragmentCol, idCol, extraCol)
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
		// Old schemas table does not contain the `id` or `extra` column.
		if !tbl.Schema().Contains(doltdb.SchemasTablesIdCol, doltdb.SchemasTableName) || !tbl.Schema().Contains(doltdb.SchemasTablesExtraCol, doltdb.SchemasTableName) {
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
	// Copy all of the old data over and add an index column and an extra column
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
		// append the new id to row, if missing
		if !schemasTable.sqlSchema().Contains(doltdb.SchemasTablesIdCol, doltdb.SchemasTableName) {
			sqlRow = append(sqlRow, id)
		}
		// append the extra cols to row
		sqlRow = append(sqlRow, nil)
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

	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return 0, err
	}

	if rows.Empty() {
		return 1, nil
	}

	if types.IsFormat_DOLT_1(tbl.Format()) {
		p := durable.ProllyMapFromIndex(rows)
		key, _, err := p.Last(ctx)
		if err != nil {
			return 0, err
		}
		kd, _ := p.Descriptors()

		i, _ := kd.GetInt64(0, key)
		return i + 1, nil
	} else {
		m := durable.NomsMapFromIndex(rows)
		keyTpl, _, err := m.Last(ctx)
		if err != nil {
			return 0, err
		}
		if keyTpl == nil {
			return 1, nil
		}

		key, err := keyTpl.(types.Tuple).Get(1)
		if err != nil {
			return 0, err
		}
		return int64(key.(types.Int)) + 1, nil
	}
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

	dt, err := tbl.doltTable(ctx)
	if err != nil {
		return nil, false, err
	}

	iter, err := index.RowIterForIndexLookup(ctx, dt, lookup, tbl.sqlSch, nil)
	if err != nil {
		return nil, false, err
	}

	defer func() {
		if cerr := iter.Close(ctx); cerr != nil {
			err = cerr
		}
	}()

	// todo(andy): use filtered reader?
	for {
		sqlRow, err := iter.Next(ctx)
		if err == io.EOF {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if sqlRow[0] != fragType || sqlRow[1] != name {
			continue
		}
		return sqlRow, true, nil
	}
}

type schemaFragment struct {
	name     string
	fragment string
	created  time.Time
}

func getSchemaFragmentsOfType(ctx *sql.Context, tbl *WritableDoltTable, fragType string) ([]schemaFragment, error) {
	iter, err := TableToRowIter(ctx, tbl, nil)
	if err != nil {
		return nil, err
	}

	var frags []schemaFragment
	for {
		sqlRow, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if sqlRow[0] != fragType {
			continue
		}

		// For tables that haven't been converted yet or are filled with nil, use 1 as the trigger creation time
		if len(sqlRow) < 5 || sqlRow[4] == nil {
			frags = append(frags, schemaFragment{
				name:     sqlRow[1].(string),
				fragment: sqlRow[2].(string),
				created:  time.Unix(1, 0).UTC(), // TablePlus editor thinks 0 is out of range
			})
			continue
		}

		// Extract Created Time from JSON column
		createdTime, err := getCreatedTime(ctx, sqlRow)

		frags = append(frags, schemaFragment{
			name:     sqlRow[1].(string),
			fragment: sqlRow[2].(string),
			created:  time.Unix(createdTime, 0).UTC(),
		})
	}
	return frags, nil
}

func getCreatedTime(ctx *sql.Context, row sql.Row) (int64, error) {
	doc, err := row[4].(sql.JSONValue).Unmarshall(ctx)
	if err != nil {
		return 0, err
	}

	err = fmt.Errorf("value %v does not contain creation time", doc.Val)

	obj, ok := doc.Val.(map[string]interface{})
	if !ok {
		return 0, err
	}

	v, ok := obj["CreatedAt"]
	if !ok {
		return 0, err
	}

	f, ok := v.(float64)
	if !ok {
		return 0, err
	}
	return int64(f), nil
}
