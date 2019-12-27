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

package alterschema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestModifyColumn(t *testing.T) {
	tests := []struct {
		name           string
		existingColumn schema.Column
		newColumn      schema.Column
		defaultVal     types.Value
		order          *ColumnOrder
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:       "column rename",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			newColumn: schema.NewColumn("newId", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
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
			name:       "remove null constraint",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			newColumn: schema.NewColumn("newId", dtestutils.IdTag, types.UUIDKind, true),
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("newId", dtestutils.IdTag, types.UUIDKind, true),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:       "reorder first",
			existingColumn: schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			newColumn: schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			order: &ColumnOrder{ First: true, },
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
			name:       "reorder middle",
			existingColumn: schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			newColumn: schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			order: &ColumnOrder{ After: "is_married" },
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
			name:        "tag collision",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			newColumn: schema.NewColumn("newId", dtestutils.NameTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			expectedErr: "two different columns with the same tag",
		},
		{
			name:        "name collision",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			newColumn: schema.NewColumn("name", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			expectedErr: "A column with the name name already exists",
		},
		{
			name:        "wrong type for default value",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			newColumn: schema.NewColumn("newId", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			defaultVal: types.String("not a string"),
			expectedErr: "Type of default value (String) doesn't match type of column (UUID)",
		},
		{
			name:        "type change",
			existingColumn: schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
			newColumn: schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, true, schema.NotNullConstraint{}),
			expectedErr: "unsupported feature: column types cannot be changed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := createEnvWithSeedData(t)
			ctx := context.Background()

			root, err := dEnv.WorkingRoot(ctx)
			assert.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, tableName)
			assert.NoError(t, err)

			updatedTable, err := ModifyColumn(ctx, tbl, tt.existingColumn, tt.newColumn, tt.defaultVal, tt.order)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}

			sch, err := updatedTable.GetSchema(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expectedSchema, sch)

			rowData, err := updatedTable.GetRowData(ctx)
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
		})
	}
}