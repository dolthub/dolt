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
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
)

// doltSchemaToSqlSchema returns the sql.Schema corresponding to the dolt schema given.
func doltSchemaToSqlSchema(tableName string, sch schema.Schema) (sql.Schema, error) {
	cols := make([]*sql.Column, sch.GetAllCols().Size())

	var i int
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		var innerErr error
		cols[i], innerErr = doltColToSqlCol(tableName, col)
		if innerErr != nil {
			return true, innerErr
		}
		i++
		return false, nil
	})

	return cols, err
}

// SqlSchemaToDoltResultSchema returns a dolt Schema from the sql schema given, suitable for use as a result set. For
// creating tables, use SqlSchemaToDoltSchema.
func SqlSchemaToDoltResultSchema(sqlSchema sql.Schema) (schema.Schema, error) {
	var cols []schema.Column
	for i, col := range sqlSchema {
		convertedCol, err := SqlColToDoltCol(uint64(i), col)
		if err != nil {
			return nil, err
		}
		cols = append(cols, convertedCol)
	}

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		panic(err)
	}

	return schema.UnkeyedSchemaFromCols(colColl), nil
}

// SqlSchemaToDoltResultSchema returns a dolt Schema from the sql schema given, suitable for use in creating a table.
// For result set schemas, see SqlSchemaToDoltResultSchema.
func SqlSchemaToDoltSchema(ctx context.Context, root *doltdb.RootValue, sqlSchema sql.Schema) (schema.Schema, error) {
	var cols []schema.Column
	var err error

	var tag uint64
	for _, col := range sqlSchema {
		commentTag := extractTag(col)
		// TODO: are we sure we want to silently autogen new tags here?
		if commentTag != schema.InvalidTag {
			tag = commentTag
		} else {
			tag, err = root.GetUniqueTag(ctx)
			if err != nil {
				return nil, err
			}
		}

		convertedCol, err := SqlColToDoltCol(tag, col)
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

// doltColToSqlCol returns the SQL column corresponding to the dolt column given.
func doltColToSqlCol(tableName string, col schema.Column) (*sql.Column, error) {
	sqlType := col.TypeInfo.ToSqlType()
	return &sql.Column{
		Name:       col.Name,
		Type:       sqlType,
		Default:    nil,
		Nullable:   col.IsNullable(),
		Source:     tableName,
		PrimaryKey: col.IsPartOfPK,
		Comment:    fmt.Sprintf("tag:%d", col.Tag),
	}, nil
}

// doltColToSqlCol returns the dolt column corresponding to the SQL column given
func SqlColToDoltCol(tag uint64, col *sql.Column) (schema.Column, error) {
	var constraints []schema.ColConstraint
	if !col.Nullable {
		constraints = append(constraints, schema.NotNullConstraint{})
	}
	typeInfo, err := typeinfo.FromSqlType(col.Type)
	if err != nil {
		return schema.Column{}, err
	}

	return schema.NewColumnWithTypeInfo(col.Name, tag, typeInfo, col.PrimaryKey, constraints...)
}

const tagCommentPrefix = "tag:"

// Extracts the optional comment tag from a column type defn, or InvalidTag if it can't be extracted
func extractTag(col *sql.Column) uint64 {
	if len(col.Comment) == 0 {
		return schema.InvalidTag
	}

	i := strings.Index(col.Comment, tagCommentPrefix)
	if i >= 0 {
		startIdx := i + len(tagCommentPrefix)
		tag, err := strconv.ParseUint(col.Comment[startIdx:], 10, 64)
		if err != nil {
			return schema.InvalidTag
		}
		return tag
	}

	return schema.InvalidTag
}
