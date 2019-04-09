package sql

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/go-cmp/cmp"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/xwb1989/sqlparser"
)

func TestExecuteInsert(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		insertedValues []row.Row
		expectedResult InsertResult // root is not compared, but it used for other assertions
		expectedErr bool
	}{
		{
			name: "insert one row, all columns",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1)`,
			insertedValues: []row.Row{newRow(7, "Maggie", "Simpson", false, 1, 5.1)},
			expectedResult: InsertResult{ NumRowsInserted: 1 },
		},
		{
			name: "insert two rows, all columns",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1),
					(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
			insertedValues: []row.Row{
				newRow(7, "Maggie", "Simpson", false, 1, 5.1),
				newRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
			},
			expectedResult: InsertResult{ NumRowsInserted: 2 },
		},
		{
			name: "insert five rows, all columns",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1),
					(8, "Milhouse", "Van Houten", false, 8, 3.5),
					(9, "Jacqueline", "Bouvier", true, 80, 2),
					(10, "Patty", "Bouvier", false, 40, 7),
					(11, "Selma", "Bouvier", false, 40, 7)`,
			insertedValues: []row.Row{
				newRow(7, "Maggie", "Simpson", false, 1, 5.1),
				newRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
				newRow(9, "Jacqueline", "Bouvier", true, 80, 2),
				newRow(10, "Patty", "Bouvier", false, 40, 7),
				newRow(11, "Selma", "Bouvier", false, 40, 7),
			},
			expectedResult: InsertResult{ NumRowsInserted: 5 },
		},
		{
			name: "insert two rows, only primary key",
			query: `insert into people (id) values (7), (8)`,
			insertedValues: []row.Row{
				row.New(testSch, row.TaggedValues{idTag: types.Int(7)}),
				row.New(testSch, row.TaggedValues{idTag: types.Int(8)}),
			},
			expectedResult: InsertResult{ NumRowsInserted: 2 },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			createTestDatabase(dEnv, t)
			root, _ := dEnv.WorkingRoot()

			sqlStatement, _ := sqlparser.Parse(tt.query)
			s := sqlStatement.(*sqlparser.Insert)

			result, err := ExecuteInsert(dEnv.DoltDB, root, s, tt.query)
			assert.NotNil(t, result)
			assert.NotNil(t, result.Root)

			assert.Equal(t, tt.expectedErr, err != nil, "unexpected error value")
			assert.Equal(t, tt.expectedResult.NumRowsInserted, result.NumRowsInserted)
			assert.Equal(t, tt.expectedResult.NumErrorsIgnored, result.NumErrorsIgnored)
			assert.Equal(t, tt.expectedResult.NumRowsUpdated, result.NumRowsUpdated)

			table, ok := result.Root.GetTable(testTableName)
			assert.True(t, ok)

			for _, expectedRow := range tt.insertedValues {
				foundRow, ok := table.GetRow(expectedRow.NomsMapKey(testSch).(types.Tuple), testSch)
				assert.True(t, ok, "Row not found: %v", expectedRow)
				opts := cmp.Options{cmp.AllowUnexported(expectedRow), floatComparer}
				assert.True(t, cmp.Equal(expectedRow, foundRow, opts), "Rows not equals, found diff %v", cmp.Diff(expectedRow, foundRow, opts))
			}
		})
	}
}
