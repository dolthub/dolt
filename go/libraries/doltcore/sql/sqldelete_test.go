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

package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	. "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestExecuteDelete(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		deletedRows    []row.Row
		expectedResult DeleteResult // root is not compared, but it's used for other assertions
		expectedErr    bool
	}{
		{
			name:           "delete one row, one col, primary key where clause",
			query:          `delete from people where id = 0`,
			deletedRows:    []row.Row{Homer},
			expectedResult: DeleteResult{NumRowsDeleted: 1},
		},
		{
			name:           "delete one row, non-primary key where clause",
			query:          `delete from people where first = "Homer"`,
			deletedRows:    []row.Row{Homer},
			expectedResult: DeleteResult{NumRowsDeleted: 1},
		},
		{
			name:           "delete without where clause",
			query:          `delete from people`,
			deletedRows:    []row.Row{Homer, Marge, Bart, Lisa, Moe, Barney},
			expectedResult: DeleteResult{NumRowsDeleted: 6},
		},
		{
			name:           "delete no matching rows",
			query:          `delete from people where last = "Flanders"`,
			deletedRows:    []row.Row{},
			expectedResult: DeleteResult{NumRowsDeleted: 0},
		},
		{
			name:           "delete multiple rows, =",
			query:          `delete from people where last = "Simpson"`,
			deletedRows:    []row.Row{Homer, Marge, Bart, Lisa},
			expectedResult: DeleteResult{NumRowsDeleted: 4},
		},
		{
			name:           "delete multiple rows, <>",
			query:          `delete from people where last <> "Simpson"`,
			deletedRows:    []row.Row{Moe, Barney},
			expectedResult: DeleteResult{NumRowsDeleted: 2},
		},
		{
			name:           "delete multiple rows, >",
			query:          `delete from people where age > 10`,
			deletedRows:    []row.Row{Homer, Marge, Moe, Barney},
			expectedResult: DeleteResult{NumRowsDeleted: 4},
		},
		{
			name:           "delete multiple rows, >=",
			query:          `delete from people where age >= 10`,
			deletedRows:    []row.Row{Homer, Marge, Bart, Moe, Barney},
			expectedResult: DeleteResult{NumRowsDeleted: 5},
		},
		{
			name:           "delete multiple rows, <",
			query:          `delete from people where age < 40`,
			deletedRows:    []row.Row{Marge, Bart, Lisa},
			expectedResult: DeleteResult{NumRowsDeleted: 3},
		},
		{
			name:           "delete multiple rows, <=",
			query:          `delete from people where age <= 40`,
			deletedRows:    []row.Row{Homer, Marge, Bart, Lisa, Barney},
			expectedResult: DeleteResult{NumRowsDeleted: 5},
		},
		{
			name:        "non-existent table",
			query:       `delete from unknown where id = 0`,
			expectedErr: true,
		},
		{
			name:        "non-existent column",
			query:       `delete from people where unknown = 0`,
			expectedErr: true,
		},
		{
			name:        "type mismatch in where clause",
			query:       `delete from people where id = "id"`,
			expectedErr: true,
		},
		{
			name:        "type mismatch in where clause",
			query:       `delete from people where id = "0"`,
			expectedErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)
			root, _ := dEnv.WorkingRoot(context.Background())

			sqlStatement, _ := sqlparser.Parse(tt.query)
			s := sqlStatement.(*sqlparser.Delete)

			result, err := ExecuteDelete(context.Background(), dEnv.DoltDB, root, s, tt.query)
			if tt.expectedErr {
				assert.True(t, err != nil, "expected error")
				assert.Equal(t, DeleteResult{}, tt.expectedResult, "incorrect test setup: cannot assert both an error and expected results")
				assert.Nil(t, tt.deletedRows, "incorrect test setup: cannot assert both an error and deleted ids")
				return
			} else {
				require.Nil(t, err, "unexpected error")
			}

			assert.Equal(t, tt.expectedResult.NumRowsDeleted, result.NumRowsDeleted)

			table, ok := result.Root.GetTable(context.Background(), PeopleTableName)
			assert.True(t, ok)

			// make sure exactly the expected rows are deleted
			for _, r := range AllPeopleRows {
				deletedIdx := FindRowIndex(r, tt.deletedRows)

				key := r.NomsMapKey(PeopleTestSchema)
				_, ok := table.GetRow(ctx, key.Value(ctx).(types.Tuple), PeopleTestSchema)
				if deletedIdx >= 0 {
					assert.False(t, ok, "Row not deleted: %v", r)
				} else {
					assert.True(t, ok, "Row deleted unexpectedly: %v", r)
				}
			}
		})
	}
}
