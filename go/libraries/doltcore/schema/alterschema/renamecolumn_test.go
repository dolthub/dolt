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

func TestRenameColumn(t *testing.T) {
	tests := []struct {
		name           string
		colName        string
		newName        string
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:    "rename int",
			colName: "age",
			newName: "newAge",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("name", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("newAge", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:    "rename string",
			colName: "name",
			newName: "newName",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", dtestutils.IdTag, types.UUIDKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("newName", dtestutils.NameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
			),
			expectedRows: dtestutils.TypedRows,
		},
		{
			name:        "column not found",
			colName:     "not found",
			expectedErr: "column not found",
		},
		{
			name:        "name collision",
			colName:     "age",
			newName:     "title",
			expectedErr: "two different columns with the same name exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := createEnvWithSeedData(t)
			ctx := context.Background()

			root, err := dEnv.WorkingRoot(ctx)
			assert.NoError(t, err)
			tbl, _, err := root.GetTable(ctx, tableName)
			require.NoError(t, err)

			updatedTable, err := RenameColumn(ctx, dEnv.DoltDB, tbl, tt.colName, tt.newName)
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

			assert.Equal(t, tt.expectedRows, foundRows)
		})
	}
}
