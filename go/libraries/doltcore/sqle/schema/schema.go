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

package schema

import (
	"context"
	"fmt"

	sqle "github.com/liquidata-inc/go-mysql-server"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/parse"
	"github.com/liquidata-inc/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// ApplyDefaults applies the default values to the given indices, returning the resulting row.
func ApplyDefaults(ctx context.Context, doltSchema schema.Schema, sqlSchema sql.Schema, indicesOfColumns []int, dRow row.Row) (row.Row, error) {
	if len(indicesOfColumns) == 0 {
		return dRow, nil
	}
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		sqlCtx = sql.NewContext(ctx)
	}
	doltCols := doltSchema.GetAllCols()
	oldSqlRow := make(sql.Row, len(sqlSchema))
	for i, tag := range doltCols.Tags {
		val, ok := dRow.GetColVal(tag)
		if ok {
			var err error
			oldSqlRow[i], err = doltCols.TagToCol[tag].TypeInfo.ConvertNomsValueToValue(val)
			if err != nil {
				return nil, err
			}
		} else {
			oldSqlRow[i] = nil
		}
	}
	newSqlRow, err := sqle.ApplyDefaults(sqlCtx, sqlSchema, indicesOfColumns, oldSqlRow)
	if err != nil {
		return nil, err
	}
	newRow := make(row.TaggedValues)
	for i, tag := range doltCols.Tags {
		if newSqlRow[i] == nil {
			continue
		}
		val, err := doltCols.TagToCol[tag].TypeInfo.ConvertValueToNomsValue(newSqlRow[i])
		if err != nil {
			return nil, err
		}
		newRow[tag] = val
	}
	return row.New(dRow.Format(), doltSchema, newRow)
}

// ToDoltResultSchema returns a dolt Schema from the sql schema given, suitable for use as a result set. For
// creating tables, use ToDoltSchema.
func ToDoltResultSchema(sqlSchema sql.Schema) (schema.Schema, error) {
	var cols []schema.Column
	for i, col := range sqlSchema {
		convertedCol, err := ToDoltCol(uint64(i), col)
		if err != nil {
			return nil, err
		}
		cols = append(cols, convertedCol)
	}

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		return nil, err
	}

	return schema.UnkeyedSchemaFromCols(colColl), nil
}

// ParseCreateTableStatement will parse a CREATE TABLE ddl statement and use it to create a Dolt Schema. A RootValue
// is used to generate unique tags for the Schema
func ParseCreateTableStatement(ctx context.Context, root *doltdb.RootValue, query string) (string, schema.Schema, error) {
	// todo: verify create table statement
	ddl, err := sqlparser.ParseStrictDDL(query)

	if err != nil {
		return "", nil, err
	}

	ts := ddl.(*sqlparser.DDL).TableSpec
	s, err := parse.TableSpecToSchema(sql.NewContext(ctx), ts)

	if err != nil {
		return "", nil, err
	}

	tn := ddl.(*sqlparser.DDL).Table
	buf := sqlparser.NewTrackedBuffer(nil)
	tn.Format(buf)
	tableName := buf.String()
	sch, err := ToDoltSchema(ctx, root, tableName, s)

	if err != nil {
		return "", nil, err
	}

	return tableName, sch, err
}

func FromDoltSchema(tableName string, sch schema.Schema) (sql.Schema, error) {
	cols := make([]*sqle.ColumnWithRawDefault, sch.GetAllCols().Size())

	var i int
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		sqlType := col.TypeInfo.ToSqlType()
		cols[i] = &sqle.ColumnWithRawDefault{
			SqlColumn: &sql.Column{
				Name:       col.Name,
				Type:       sqlType,
				Default:    nil,
				Nullable:   col.IsNullable(),
				Source:     tableName,
				PrimaryKey: col.IsPartOfPK,
				Comment:    col.Comment,
				Extra:      fmt.Sprintf("tag:%d", tag),
			},
			Default: col.Default,
		}
		i++
		return false, nil
	})

	return sqle.ResolveDefaults(tableName, cols)
}

// ToDoltSchema returns a dolt Schema from the sql schema given, suitable for use in creating a table.
// For result set schemas, see ToDoltResultSchema.
func ToDoltSchema(ctx context.Context, root *doltdb.RootValue, tableName string, sqlSchema sql.Schema) (schema.Schema, error) {
	var cols []schema.Column
	var err error

	// generate tags for all columns
	var names []string
	var kinds []types.NomsKind
	for _, col := range sqlSchema {
		names = append(names, col.Name)
		ti, err := typeinfo.FromSqlType(col.Type)
		if err != nil {
			return nil, err
		}
		kinds = append(kinds, ti.NomsKind())
	}
	tags, err := root.GenerateTagsForNewColumns(ctx, tableName, names, kinds)
	if err != nil {
		return nil, err
	}

	if len(tags) != len(sqlSchema) {
		return nil, fmt.Errorf("number of tags should equal number of columns")
	}

	for i, col := range sqlSchema {
		convertedCol, err := ToDoltCol(tags[i], col)
		if err != nil {
			return nil, err
		}
		cols = append(cols, convertedCol)
	}

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		return nil, err
	}

	err = schema.ValidateForInsert(colColl)
	if err != nil {
		return nil, err
	}

	return schema.SchemaFromCols(colColl), nil
}

// ToDoltCol returns the dolt column corresponding to the SQL column given
func ToDoltCol(tag uint64, col *sql.Column) (schema.Column, error) {
	var constraints []schema.ColConstraint
	if !col.Nullable {
		constraints = append(constraints, schema.NotNullConstraint{})
	}
	typeInfo, err := typeinfo.FromSqlType(col.Type)
	if err != nil {
		return schema.Column{}, err
	}

	return schema.NewColumnWithTypeInfo(col.Name, tag, typeInfo, col.PrimaryKey, col.Default.String(), col.Comment, constraints...)
}
