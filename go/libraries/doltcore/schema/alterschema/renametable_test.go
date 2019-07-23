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

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func TestRenameTable(t *testing.T) {
	otherTable := "other"

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
			dEnv := createEnvWithSeedData(t)
			ctx := context.Background()

			dtestutils.CreateTestTable(t, dEnv, otherTable, dtestutils.UntypedSchema)

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			updatedRoot, err := RenameTable(ctx, dEnv.DoltDB, root, tt.tableName, tt.newTableName)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}

			assert.False(t, updatedRoot.HasTable(ctx, tt.tableName))
			newTable, ok := updatedRoot.GetTable(ctx, tt.newTableName)
			require.True(t, ok)

			require.Equal(t, tt.expectedSchema, newTable.GetSchema(ctx))

			rowData := newTable.GetRowData(ctx)
			var foundRows []row.Row
			rowData.Iter(ctx, func(key, value types.Value) (stop bool) {
				foundRows = append(foundRows, row.FromNoms(tt.expectedSchema, key.(types.Tuple), value.(types.Tuple)))
				return false
			})

			assert.Equal(t, tt.expectedRows, foundRows)
		})
	}
}
