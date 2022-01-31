// Copyright 2019 Dolthub, Inc.
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

package alterschema

import (
	"context"
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

const tableName = "people"

func TestAddColumnToTable(t *testing.T) {
	tests := []struct {
		name           string
		tag            uint64
		newColName     string
		colKind        types.NomsKind
		nullable       Nullable
		defaultVal     *sql.ColumnDefaultValue
		order          *ColumnOrder
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:       "bool column no default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.BoolKind,
			nullable:   Null,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schema.NewColumn("newCol", dtestutils.NextTag, types.BoolKind, false)),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:       "nullable with nil default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schemaNewColumn("newCol", dtestutils.NextTag, types.IntKind, false, "")),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:       "nullable with non-nil default",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			defaultVal: mustStringToColumnDefault("42"),
			expectedSchema: dtestutils.AddColumnToSchema(dtestutils.TypedSchema,
				schemaNewColumn("newCol", dtestutils.NextTag, types.IntKind, false, "42")),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.NullValue),
		},
		{
			name:       "first order",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			defaultVal: mustStringToColumnDefault("42"),
			order:      &ColumnOrder{First: true},
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn("newCol", dtestutils.NextTag, types.IntKind, false, "42"),
				schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.NullValue),
		},
		{
			name:       "middle order",
			tag:        dtestutils.NextTag,
			newColName: "newCol",
			colKind:    types.IntKind,
			nullable:   Null,
			defaultVal: mustStringToColumnDefault("42"),
			order:      &ColumnOrder{After: "age"},
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schemaNewColumn("newCol", dtestutils.NextTag, types.IntKind, false, "42"),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.AddColToRows(t, dtestutils.TypedRows, dtestutils.NextTag, types.NullValue),
		},
		{
			name:        "tag collision",
			tag:         dtestutils.AgeTag,
			newColName:  "newCol",
			colKind:     types.IntKind,
			nullable:    NotNull,
			expectedErr: fmt.Sprintf("Cannot create column newCol, the tag %d was already used in table people", dtestutils.AgeTag),
		},
		{
			name:        "name collision",
			tag:         dtestutils.NextTag,
			newColName:  "age",
			colKind:     types.IntKind,
			nullable:    NotNull,
			defaultVal:  mustStringToColumnDefault("10"),
			expectedErr: "A column with the name age already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateEnvWithSeedData(t)
			ctx := context.Background()

			root, err := dEnv.WorkingRoot(ctx)
			assert.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, tableName)
			assert.NoError(t, err)

			updatedTable, err := AddColumnToTable(ctx, root, tbl, tableName, tt.tag, tt.newColName, typeinfo.FromKind(tt.colKind), tt.nullable, tt.defaultVal, "", tt.order)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
				require.NoError(t, err)
			}

			sch, err := updatedTable.GetSchema(ctx)
			require.NoError(t, err)
			index := sch.Indexes().GetByName(dtestutils.IndexName)
			assert.NotNil(t, index)
			tt.expectedSchema.Indexes().AddIndex(index)
			tt.expectedSchema.Checks().AddCheck("test-check", "age < 123", true)
			require.Equal(t, tt.expectedSchema, sch)

			rowData, err := updatedTable.GetNomsRowData(ctx)
			require.NoError(t, err)

			var foundRows []row.Row
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				tpl, err := row.FromNoms(tt.expectedSchema, key.(types.Tuple), value.(types.Tuple))

				if err != nil {
					return false, err
				}

				foundRows = append(foundRows, tpl)
				return false, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, foundRows)

			indexRowData, err := updatedTable.GetNomsIndexRowData(ctx, dtestutils.IndexName)
			require.NoError(t, err)
			assert.Greater(t, indexRowData.Len(), uint64(0))
		})
	}
}

func mustStringToColumnDefault(defaultString string) *sql.ColumnDefaultValue {
	def, err := parse.StringToColumnDefaultValue(sql.NewEmptyContext(), defaultString)
	if err != nil {
		panic(err)
	}
	return def
}

func schemaNewColumn(name string, tag uint64, kind types.NomsKind, partOfPK bool, defaultVal string, constraints ...schema.ColConstraint) schema.Column {
	col := schema.NewColumn(name, tag, kind, partOfPK, constraints...)
	col.Default = defaultVal
	return col
}
