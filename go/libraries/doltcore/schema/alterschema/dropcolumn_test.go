package alterschema

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDropColumn(t *testing.T) {
	tests := []struct {
		name           string
		colName        string
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:           "remove int",
			colName:        "age",
			expectedSchema: dtestutils.RemoveColumnFromSchema(dtestutils.TypedSchema, dtestutils.AgeTag),
			expectedRows:   dtestutils.TypedRows,
		},
		{
			name:           "remove string",
			colName:        "title",
			expectedSchema: dtestutils.RemoveColumnFromSchema(dtestutils.TypedSchema, dtestutils.TitleTag),
			expectedRows:   dtestutils.TypedRows,
		},
		{
			name:        "column not found",
			colName:     "not found",
			expectedErr: "column not found",
		},
		{
			name:        "remove primary key col",
			colName:     "id",
			expectedErr: "Cannot drop column in primary key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := createEnvWithSeedData(t)
			ctx := context.Background()

			root, err := dEnv.WorkingRoot(ctx)
			assert.NoError(t, err)
			tbl, _ := root.GetTable(ctx, tableName)

			updatedTable, err := DropColumn(ctx, dEnv.DoltDB, tbl, tt.colName)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.expectedSchema, updatedTable.GetSchema(ctx))

			rowData := updatedTable.GetRowData(ctx)
			var foundRows []row.Row
			rowData.Iter(ctx, func(key, value types.Value) (stop bool) {
				foundRows = append(foundRows, row.FromNoms(dtestutils.TypedSchema, key.(types.Tuple), value.(types.Tuple)))
				return false
			})

			assert.Equal(t, tt.expectedRows, foundRows)
		})
	}
}
