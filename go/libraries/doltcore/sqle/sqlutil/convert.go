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
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

// TODO: Many callers only care about field names and types, not the table or db names.
// Those callers may be passing in "" for these values, or may be passing in incorrect values
// that are currently unused.
func FromDoltSchema(dbName, tableName string, sch schema.Schema) (sql.PrimaryKeySchema, error) {
	cols := make(sql.Schema, sch.GetAllCols().Size())

	var i int
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		sqlType := col.TypeInfo.ToSqlType()
		var extra string
		if col.AutoIncrement {
			extra = "auto_increment"
		}

		var deflt, generated, onUpdate *sql.ColumnDefaultValue
		if col.Default != "" {
			deflt = sql.NewUnresolvedColumnDefaultValue(col.Default)
		}
		if col.Generated != "" {
			generated = sql.NewUnresolvedColumnDefaultValue(col.Generated)
		}
		if col.OnUpdate != "" {
			onUpdate = sql.NewUnresolvedColumnDefaultValue(col.OnUpdate)
		}

		cols[i] = &sql.Column{
			Name:           col.Name,
			Type:           sqlType,
			Default:        deflt,
			Generated:      generated,
			OnUpdate:       onUpdate,
			Nullable:       col.IsNullable(),
			DatabaseSource: dbName,
			Source:         tableName,
			PrimaryKey:     col.IsPartOfPK,
			AutoIncrement:  col.AutoIncrement,
			Comment:        col.Comment,
			Virtual:        col.Virtual,
			Extra:          extra,
		}
		i++
		return false, nil
	})

	return sql.NewPrimaryKeySchema(cols, sch.GetPkOrdinals()...), nil
}

// ToDoltSchema returns a dolt Schema from the sql schema given, suitable for use in creating a table.
func ToDoltSchema(
	ctx context.Context,
	root doltdb.RootValue,
	tableName string,
	sqlSchema sql.PrimaryKeySchema,
	headRoot doltdb.RootValue,
	collation sql.CollationID,
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

	tags, err := doltdb.GenerateTagsForNewColumns(ctx, root, tableName, names, kinds, headRoot)
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

	sch, err := schema.NewSchema(colColl,
		sqlSchema.PkOrdinals,
		schema.Collation(collation),
		nil,
		nil,
	)
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

	var defaultVal, generatedVal, onUpdateVal string
	if col.Default != nil {
		defaultVal = col.Default.String()
		if defaultVal != "NULL" && col.Default.IsLiteral() && !gmstypes.IsTime(col.Default.Type()) && !gmstypes.IsText(col.Default.Type()) {
			v, err := col.Default.Eval(nil, nil)
			if err == nil {
				defaultVal = fmt.Sprintf("'%v'", v)
			}
		}
	} else {
		generatedVal = col.Generated.String()
	}

	if col.OnUpdate != nil {
		onUpdateVal = col.OnUpdate.String()
	}

	c := schema.Column{
		Name:          col.Name,
		Tag:           tag,
		Kind:          typeInfo.NomsKind(),
		IsPartOfPK:    col.PrimaryKey,
		TypeInfo:      typeInfo,
		Default:       defaultVal,
		Generated:     generatedVal,
		OnUpdate:      onUpdateVal,
		Virtual:       col.Virtual,
		AutoIncrement: col.AutoIncrement,
		Comment:       col.Comment,
		Constraints:   constraints,
	}

	err = schema.ValidateColumn(c)
	if err != nil {
		return schema.Column{}, err
	}

	return c, nil
}
