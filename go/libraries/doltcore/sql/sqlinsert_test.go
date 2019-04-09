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
	}{
		{
			name: "insert one row, all columns",
			query: `insert into people (id, first, last, is_married, age, rating) values 
					(7, "Maggie", "Simpson", false, 1, 5.1)`,
			insertedValues: []row.Row{newRow(7, "Maggie", "Simpson", false, 2, 5.1)},
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
			table, ok := result.Root.GetTable(testTableName)
			assert.True(t, ok)

			for _, expectedRow := range tt.insertedValues {
				foundRow, ok := table.GetRow(expectedRow.NomsMapKey(testSch).(types.Tuple), testSch)
				assert.True(t, ok, "Row not found: %v", expectedRow)
				opts := cmp.Options{cmp.AllowUnexported(expectedRow), floatComparer}
				assert.True(t, cmp.Equal(expectedRow, foundRow, opts), "Rows not equals, found diff %v", cmp.Diff(expectedRow, foundRow, opts))
			}

			assert.NotNil(t, result)
			assert.Nil(t, err)
		})
	}
}
