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

package sqlutil

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

func FromDoltSchema(tableName string, sch schema.Schema) (sql.PrimaryKeySchema, error) {
	cols := make(sql.Schema, sch.GetAllCols().Size())

	var i int
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		sqlType := col.TypeInfo.ToSqlType()
		var extra string
		if col.AutoIncrement {
			extra = "auto_increment"
		}

		var deflt *sql.ColumnDefaultValue
		if col.Default != "" {
			deflt = sql.NewUnresolvedColumnDefaultValue(col.Default)
		}

		cols[i] = &sql.Column{
			Name:          col.Name,
			Type:          sqlType,
			Default:       deflt,
			Nullable:      col.IsNullable(),
			Source:        tableName,
			PrimaryKey:    col.IsPartOfPK,
			AutoIncrement: col.AutoIncrement,
			Comment:       col.Comment,
			Extra:         extra,
		}
		i++
		return false, nil
	})

	return sql.NewPrimaryKeySchema(cols, sch.GetPkOrdinals()...), nil
}

// ToDoltSchema returns a dolt Schema from the sql schema given, suitable for use in creating a table.
// For result set schemas, see ToDoltResultSchema.
func ToDoltSchema(
	ctx context.Context,
	root *doltdb.RootValue,
	tableName string,
	sqlSchema sql.PrimaryKeySchema,
	headRoot *doltdb.RootValue,
) (schema.Schema, error) {
	var cols []schema.Column
	var err error

	// generate tags for all columns
	var names []string
	var kinds []types.NomsKind
	for _, col := range sqlSchema.Schema {
		names = append(names, col.Name)
		ti, err := typeinfo.FromSqlType(col.Type)
		if err != nil {
			return nil, err
		}
		kinds = append(kinds, ti.NomsKind())
	}

	tags, err := root.GenerateTagsForNewColumns(ctx, tableName, names, kinds, headRoot)
	if err != nil {
		return nil, err
	}

	if len(tags) != len(sqlSchema.Schema) {
		return nil, fmt.Errorf("number of tags should equal number of columns")
	}

	for i, col := range sqlSchema.Schema {
		convertedCol, err := ToDoltCol(tags[i], col)
		if err != nil {
			return nil, err
		}
		cols = append(cols, convertedCol)
	}

	colColl := schema.NewColCollection(cols...)

	err = schema.ValidateForInsert(colColl)
	if err != nil {
		return nil, err
	}

	sch, err := schema.SchemaFromCols(colColl)
	if err != nil {
		return nil, err
	}

	err = sch.SetPkOrdinals(sqlSchema.PkOrdinals)
	if err != nil {
		return nil, err
	}

	return sch, nil
}

// ToDoltCol returns the dolt column corresponding to the SQL column given
func ToDoltCol(tag uint64, col *sql.Column) (schema.Column, error) {
	var constraints []schema.ColConstraint
	if !col.Nullable || col.PrimaryKey {
		constraints = append(constraints, schema.NotNullConstraint{})
	}
	typeInfo, err := typeinfo.FromSqlType(col.Type)
	if err != nil {
		return schema.Column{}, err
	}

	return schema.NewColumnWithTypeInfo(col.Name, tag, typeInfo, col.PrimaryKey, col.Default.String(), col.AutoIncrement, col.Comment, constraints...)
}

func GetColNamesFromSqlSchema(sqlSch sql.Schema) []string {
	colNames := make([]string, len(sqlSch))

	for i, col := range sqlSch {
		colNames[i] = col.Name
	}

	return colNames
}
