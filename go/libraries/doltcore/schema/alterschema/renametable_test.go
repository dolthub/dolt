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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

func TestRenameTable(t *testing.T) {
	otherTable := "other"
	cc := schema.NewColCollection(
		schema.NewColumn("id", uint64(100), types.UUIDKind, true, schema.NotNullConstraint{}),
	)
	otherSch, err := schema.SchemaFromCols(cc)
	require.NoError(t, err)

	tests := []struct {
		name           string
		tableName      string
		newTableName   string
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:           "rename table",
			tableName:      "people",
			newTableName:   "newPeople",
			expectedSchema: dtestutils.TypedSchema,
			expectedRows:   dtestutils.TypedRows,
		},
		{
			name:         "table not found",
			tableName:    "notFound",
			newTableName: "newNotfound",
			expectedErr:  doltdb.ErrTableNotFound.Error(),
		},
		{
			name:         "name already in use",
			tableName:    "people",
			newTableName: otherTable,
			expectedErr:  doltdb.ErrTableExists.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateEnvWithSeedData(t)
			ctx := context.Background()

			dtestutils.CreateTestTable(t, dEnv, otherTable, otherSch)

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			updatedRoot, err := RenameTable(ctx, root, tt.tableName, tt.newTableName)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}

			has, err := updatedRoot.HasTable(ctx, tt.tableName)
			require.NoError(t, err)
			assert.False(t, has)
			newTable, ok, err := updatedRoot.GetTable(ctx, tt.newTableName)
			require.NoError(t, err)
			require.True(t, ok)

			sch, err := newTable.GetSchema(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expectedSchema, sch)

			rowData, err := newTable.GetNomsRowData(ctx)
			require.NoError(t, err)
			var foundRows []row.Row
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				tpl, err := row.FromNoms(tt.expectedSchema, key.(types.Tuple), value.(types.Tuple))
				foundRows = append(foundRows, tpl)
				return false, err
			})

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, foundRows)
		})
	}
}
