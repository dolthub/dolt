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
	"github.com/liquidata-inc/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
)

// The fixed schema for the `dolt_schemas` table.
func SchemasTableSchema() sql.Schema {
	return []*sql.Column{
		// Currently: `view`.
		{Name: doltdb.SchemasTablesTypeCol, Type: sql.Text, Source: doltdb.SchemasTableName, PrimaryKey: true},
		// The name of the database entity.
		{Name: doltdb.SchemasTablesNameCol, Type: sql.Text, Source: doltdb.SchemasTableName, PrimaryKey: true},
		// The schema fragment associated with the database entity.
		// For example, the SELECT statement for a CREATE VIEW.
		{Name: doltdb.SchemasTablesFragmentCol, Type: sql.Text, Source: doltdb.SchemasTableName, PrimaryKey: false},
	}
}

// GetOrCreateDoltSchemasTable returns the `dolt_schemas` table in `db`, creating it if it does not already exist.
func GetOrCreateDoltSchemasTable(ctx *sql.Context, db Database) (*WritableDoltTable, error) {
	tbl, found, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}
	if found {
		return tbl.(*WritableDoltTable), nil
	}
	err = db.createTable(ctx, doltdb.SchemasTableName, SchemasTableSchema())
	if err != nil {
		return nil, err
	}
	tbl, found, err = db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sql.ErrTableNotFound.New("dolt_schemas")
	}
	return tbl.(*WritableDoltTable), nil
}

// Return `true` if a schema fragment for a view with name `name`
// exists in `tbl`. `tbl` should be the `dolt_schemas` table in the
// Database. Returns `false` otherwise.
func viewExistsInSchemasTable(ctx *sql.Context, tbl *WritableDoltTable, name string) (bool, error) {
	row := sql.Row{"view", name}
	doltLookup, err := SqlRowToDoltRow(tbl.table.Format(), row, tbl.sch)
	if err != nil {
		return false, err
	}

	keyVl := doltLookup.NomsMapKey(tbl.sch)
	key, err := keyVl.Value(ctx)
	if err != nil {
		return false, err
	}
	rows, err := tbl.table.GetRowData(ctx)
	if err != nil {
		return false, err
	}
	return rows.Has(ctx, key)
}
