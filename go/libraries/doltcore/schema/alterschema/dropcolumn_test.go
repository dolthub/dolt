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

var TypedRowsWithoutAge []row.Row
var TypedRowsWithoutTitle []row.Row

func init() {
	for i := 0; i < len(dtestutils.UUIDS); i++ {

		taggedValsSansAge := row.TaggedValues{
			dtestutils.IdTag:        types.UUID(dtestutils.UUIDS[i]),
			dtestutils.NameTag:      types.String(dtestutils.Names[i]),
			dtestutils.TitleTag:     types.String(dtestutils.Titles[i]),
			dtestutils.IsMarriedTag: types.Bool(dtestutils.MaritalStatus[i]),
		}

		schSansAge := dtestutils.RemoveColumnFromSchema(dtestutils.TypedSchema, dtestutils.AgeTag)
		r, err := row.New(types.Format_7_18, schSansAge, taggedValsSansAge)

		if err != nil {
			panic(err)
		}

		TypedRowsWithoutAge = append(TypedRowsWithoutAge, r)

		taggedValsSansTitle := row.TaggedValues{
			dtestutils.IdTag:        types.UUID(dtestutils.UUIDS[i]),
			dtestutils.NameTag:      types.String(dtestutils.Names[i]),
			dtestutils.AgeTag:       types.Uint(dtestutils.Ages[i]),
			dtestutils.IsMarriedTag: types.Bool(dtestutils.MaritalStatus[i]),
		}

		schSansTitle := dtestutils.RemoveColumnFromSchema(dtestutils.TypedSchema, dtestutils.TitleTag)
		r, err = row.New(types.Format_7_18, schSansTitle, taggedValsSansTitle)

		if err != nil {
			panic(err)
		}

		TypedRowsWithoutTitle = append(TypedRowsWithoutTitle, r)
	}
}

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
			expectedRows:   TypedRowsWithoutAge,
		},
		{
			name:           "remove string",
			colName:        "title",
			expectedSchema: dtestutils.RemoveColumnFromSchema(dtestutils.TypedSchema, dtestutils.TitleTag),
			expectedRows:   TypedRowsWithoutTitle,
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
			tbl, _, err := root.GetTable(ctx, tableName)

			require.NoError(t, err)

			updatedTable, err := DropColumn(ctx, tbl, tt.colName)
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
				tpl, err := row.FromNoms(dtestutils.TypedSchema, key.(types.Tuple), value.(types.Tuple))
				foundRows = append(foundRows, tpl)
				return false, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, foundRows)
		})
	}
}
