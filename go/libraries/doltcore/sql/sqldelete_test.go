package sql

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/stretchr/testify/assert"
	"github.com/xwb1989/sqlparser"
	"testing"
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
			deletedRows:    []row.Row{homer},
			expectedResult: DeleteResult{NumRowsDeleted: 1},
		},
		{
			name:           "delete one row, non-primary key where clause",
			query:          `delete from people where first = "Homer"`,
			deletedRows:    []row.Row{homer},
			expectedResult: DeleteResult{NumRowsDeleted: 1},
		},
		{
			name:  "delete without where clause",
			query: `delete from people`,
			deletedRows: []row.Row{homer, marge, bart, lisa, moe, barney},
			expectedResult: DeleteResult{NumRowsDeleted: 6},
		},
		{
			name:  "delete no matching rows",
			query: `delete from people where last = "Flanders"`,
			deletedRows: []row.Row{},
			expectedResult: DeleteResult{NumRowsDeleted: 0},
		},
		{
			name: "delete multiple rows, =",
			query: `delete from people where last = "Simpson"`,
			deletedRows: []row.Row{homer, marge, bart, lisa},
			expectedResult: DeleteResult{NumRowsDeleted: 4},
		},
		{
			name: "delete multiple rows, <>",
			query: `delete from people where last <> "Simpson"`,
			deletedRows: []row.Row{moe, barney},
			expectedResult: DeleteResult{NumRowsDeleted: 2},
		},
		{
			name:  "delete multiple rows, >",
			query: `delete from people where age > 10`,
			deletedRows: []row.Row{homer, marge, moe, barney},
			expectedResult: DeleteResult{NumRowsDeleted: 4},
		},
		{
			name:  "delete multiple rows, >=",
			query: `delete from people where age >= 10`,
			deletedRows: []row.Row{homer, marge, bart, moe, barney},
			expectedResult: DeleteResult{NumRowsDeleted: 5},
		},
		{
			name:  "delete multiple rows, <",
			query: `delete from people where age < 40`,
			deletedRows: []row.Row{marge, bart, lisa},
			expectedResult: DeleteResult{NumRowsDeleted: 3},
		},
		{
			name:  "delete multiple rows, <=",
			query: `delete from people where age <= 40`,
			deletedRows: []row.Row{homer, marge, bart, lisa, barney},
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
		//This should fail but doesn't.
		//{
		//	name: "type mismatch in where clause",
		//	query: `delete from people where id = "0"`,
		//	expectedErr: true,
		//},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			createTestDatabase(dEnv, t)
			root, _ := dEnv.WorkingRoot()

			sqlStatement, _ := sqlparser.Parse(tt.query)
			s := sqlStatement.(*sqlparser.Delete)

			result, err := ExecuteDelete(dEnv.DoltDB, root, s, tt.query)
			if tt.expectedErr {
				assert.True(t, err != nil, "expected error")
				assert.Equal(t, DeleteResult{}, tt.expectedResult, "incorrect test setup: cannot assert both an error and expected results")
				assert.Nil(t, tt.deletedRows, "incorrect test setup: cannot assert both an error and deleted ids")
				return
			} else {
				assert.Nil(t, err, "unexpected error")
			}

			assert.Equal(t, tt.expectedResult.NumRowsDeleted, result.NumRowsDeleted)

			table, ok := result.Root.GetTable(testTableName)
			assert.True(t, ok)

			// make sure exactly the expected rows are deleted
			allRows := []row.Row{homer, marge, bart, lisa, moe, barney}
			for _, r := range allRows {
				expectDel := false
				for _, deletedRow := range tt.deletedRows {
					rowId, _ := r.GetColVal(idTag)
					deletedId, _ := deletedRow.GetColVal(idTag)
					if rowId.Equals(deletedId) {
						expectDel = true
						break
					}
				}

				key := r.NomsMapKey(testSch)
				_, ok := table.GetRow(key.(types.Tuple), testSch)
				if expectDel {
					assert.False(t, ok, "Row not deleted: %v", r)
				} else {
					assert.True(t, ok, "Row deleted unexpectedly: %v", r)
				}
			}
		})
	}
}
