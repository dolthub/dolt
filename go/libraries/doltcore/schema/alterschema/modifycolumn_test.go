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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

func TestModifyColumn(t *testing.T) {
	alteredTypeSch := dtestutils.CreateSchema(
		schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
	)
	ti, err := typeinfo.FromSqlType(sql.MustCreateStringWithDefaults(sqltypes.VarChar, 599))
	require.NoError(t, err)
	newNameColSameTag, err := schema.NewColumnWithTypeInfo("name", dtestutils.NameTag, ti, false, "", false, "", schema.NotNullConstraint{})
	require.NoError(t, err)
	alteredTypeSch2 := dtestutils.CreateSchema(
		schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
		newNameColSameTag,
		schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
	)

	tests := []struct {
		name           string
		existingColumn schema.Column
		newColumn      schema.Column
		order          *ColumnOrder
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:           "column rename",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newId", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("newId", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:           "remove null constraint",
			existingColumn: schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false),
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:           "reorder first",
			existingColumn: schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			order:          &ColumnOrder{First: true},
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:           "reorder middle",
			existingColumn: schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			order:          &ColumnOrder{After: "is_married"},
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:           "tag collision",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newId", dtestutils.NameTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			expectedErr:    "two different columns with the same tag",
		},
		{
			name:           "name collision",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("name", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			expectedErr:    "A column with the name name already exists",
		},
		{
			name:           "type change",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			newColumn:      schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
			expectedSchema: alteredTypeSch,
			expectedRows: []row.Row{
				dtestutils.NewRow(
					alteredTypeSch,
					types.String("00000000-0000-0000-0000-000000000000"),
					types.String("Bill Billerson"),
					types.Uint(32),
					types.Bool(true),
					types.String("Senior Dufus"),
				),
				dtestutils.NewRow(
					alteredTypeSch,
					types.String("00000000-0000-0000-0000-000000000001"),
					types.String("John Johnson"),
					types.Uint(25),
					types.Bool(false),
					types.String("Dufus"),
				),
				dtestutils.NewRow(
					alteredTypeSch,
					types.String("00000000-0000-0000-0000-000000000002"),
					types.String("Rob Robertson"),
					types.Uint(21),
					types.Bool(false),
					types.String(""),
				),
			},
		},
		{
			name:           "type change same tag",
			existingColumn: schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
			newColumn:      newNameColSameTag,
			expectedSchema: alteredTypeSch2,
			expectedRows:   dtestutils.TypedRows,
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

			opts := editor.Options{Deaf: dEnv.DbEaFactory()}
			updatedTable, err := ModifyColumn(ctx, tbl, tt.existingColumn, tt.newColumn, tt.order, opts)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}

			sch, err := updatedTable.GetSchema(ctx)
			require.NoError(t, err)
			index := sch.Indexes().GetByName(dtestutils.IndexName)
			assert.NotNil(t, index)
			tt.expectedSchema.Indexes().AddIndex(index)
			tt.expectedSchema.SetPkOrdinals(sch.GetPkOrdinals())
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

			updatedIndexRows, err := updatedTable.GetNomsIndexRowData(context.Background(), index.Name())
			require.NoError(t, err)
			expectedIndexRows, err := editor.RebuildIndex(context.Background(), updatedTable, index.Name(), opts)
			require.NoError(t, err)
			if uint64(len(foundRows)) != updatedIndexRows.Len() || !updatedIndexRows.Equals(expectedIndexRows) {
				t.Error("index contents are incorrect")
			}
		})
	}
}
