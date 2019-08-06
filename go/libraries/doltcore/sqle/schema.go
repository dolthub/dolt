// Copyright 2019 Liquidata, Inc.
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
	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

// doltSchemaToSqlSchema returns the sql.Schema corresponding to the dolt schema given.
func doltSchemaToSqlSchema(tableName string, sch schema.Schema) (sql.Schema, error) {
	cols := make([]*sql.Column, sch.GetAllCols().Size())

	var i int
	 err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		cols[i] = doltColToSqlCol(tableName, col)
		i++
		return false ,nil
	})

	return cols, err
}

func SqlSchemaToDoltSchema(sqlSchema sql.Schema) schema.Schema {
	var cols []schema.Column
	for i, col := range sqlSchema {
		cols = append(cols, SqlColToDoltCol(uint64(i), false, col))
	}

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		panic(err)
	}

	return schema.UnkeyedSchemaFromCols(colColl)
}

// doltColToSqlCol returns the SQL column corresponding to the dolt column given.
func doltColToSqlCol(tableName string, col schema.Column) *sql.Column {
	return &sql.Column{
		Name:     col.Name,
		Type:     nomsTypeToSqlType(col.Kind),
		Default:  nil,
		Nullable: col.IsNullable(),
		Source:   tableName,
	}
}

// doltColToSqlCol returns the dolt column corresponding to the SQL column given
func SqlColToDoltCol(tag uint64, isPk bool, col *sql.Column) schema.Column {
	// TODO: nullness constraint
	return schema.NewColumn(col.Name, tag, SqlTypeToNomsKind(col.Type), isPk)
}
