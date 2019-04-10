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
		expectedErr    bool
	}{
		{
			name: "insert one row, all columns",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1)`,
			insertedValues: []row.Row{newRow(7, "Maggie", "Simpson", false, 1, 5.1)},
			expectedResult: InsertResult{NumRowsInserted: 1},
		},
		{
			name: "insert one row out of order",
			query: `insert into people (rating, first, id, last, age, is_married) values
					(5.1, "Maggie", 7, "Simpson", 1, false)`,
			insertedValues: []row.Row{newRow(7, "Maggie", "Simpson", false, 1, 5.1)},
			expectedResult: InsertResult{NumRowsInserted: 1},
		},
		{
			name: "insert one row, null values",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", null, null, null)`,
			insertedValues: []row.Row{row.New(testSch, row.TaggedValues{idTag: types.Int(7), firstTag: types.String("Maggie"), lastTag: types.String("Simpson")})},
			expectedResult: InsertResult{NumRowsInserted: 1},
		},
		{
			name: "insert one row, null key columns",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", null, null, null, null)`,
			expectedErr: true,
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
			expectedResult: InsertResult{NumRowsInserted: 2},
		},
		{
			name: "insert two rows, one with null constraint failure",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1),
					(8, "Milhouse", null, false, 8, 3.5)`,
			expectedErr: true,
		},
		{
			name: "type mismatch int -> string",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", 100, false, 1, 5.1)`,
			expectedErr: true,
		},
		{
			name: "type mismatch int -> bool",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", 10, 1, 5.1)`,
			expectedErr: true,
		},
		{
			name: "type mismatch string -> int",
			query: `insert into people (id, first, last, is_married, age, rating) values
					("7", "Maggie", "Simpson", false, 1, 5.1)`,
			expectedErr: true,
		},
		{
			name: "type mismatch string -> float",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, "5.1")`,
			expectedErr: true,
		},
		{
			name: "type mismatch float -> string",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, 8.1, "Simpson", false, 1, 5.1)`,
			expectedErr: true,
		},
		{
			name: "type mismatch float -> bool",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", 0.5, 1, 5.1)`,
			expectedErr: true,
		},
		{
			name: "type mismatch float -> int",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1.0, 5.1)`,
			expectedErr: true,
		},
		{
			name: "insert two rows with ignore, one with null constraint failure",
			query: `insert ignore into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", null, false, 1, 5.1),
					(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
			insertedValues: []row.Row{
				newRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
			},
			expectedResult: InsertResult{NumRowsInserted: 1, NumErrorsIgnored: 1},
		},
		{
			name: "insert existing rows without ignore / replace",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100)`,
			expectedErr: true,
		},
		{
			name: "insert two rows with ignore, one existing in table",
			query: `insert ignore into people (id, first, last, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100),
					(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
			insertedValues: []row.Row{
				newRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
				homer, // verify that homer is unchanged by the insert
			},
			expectedResult: InsertResult{NumRowsInserted: 1, NumErrorsIgnored: 1},
		},
		{
			name: "insert two rows with replace, one existing in table",
			query: `replace into people (id, first, last, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100),
					(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
			insertedValues: []row.Row{
				newRow(0, "Homer", "Simpson", true, 45, 100),
				newRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
			},
			expectedResult: InsertResult{NumRowsInserted: 1, NumRowsUpdated: 1},
		},
		{
			name: "insert two rows with replace ignore, one with errors",
			query: `replace ignore into people (id, first, last, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100),
					(8, "Milhouse", "Van Houten", false, 8, 3.5),
					(7, "Maggie", null, false, 1, 5.1)`,
			insertedValues: []row.Row{
				newRow(0, "Homer", "Simpson", true, 45, 100),
				newRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
			},
			expectedResult: InsertResult{NumRowsInserted: 1, NumRowsUpdated: 1, NumErrorsIgnored: 1},
		},
		{
			name: "insert two rows with replace, one with errors",
			query: `replace into people (id, first, last, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100),
					(8, "Milhouse", "Van Houten", false, 8, 3.5),
					(7, "Maggie", null, false, 1, 5.1)`,
			expectedErr: true,
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
			expectedResult: InsertResult{NumRowsInserted: 5},
		},
		{
			name: "insert two rows, only required columns",
			query: `insert into people (id, first, last) values 
					(7, "Maggie", "Simpson"),
					(8, "Milhouse", "Van Houten")`,
			insertedValues: []row.Row{
				row.New(testSch, row.TaggedValues{idTag: types.Int(7), firstTag: types.String("Maggie"), lastTag: types.String("Simpson")}),
				row.New(testSch, row.TaggedValues{idTag: types.Int(8), firstTag: types.String("Milhouse"), lastTag: types.String("Van Houten")}),
			},
			expectedResult: InsertResult{NumRowsInserted: 2},
		},

		{
			name: "insert two rows, duplicate id",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1),
					(7, "Milhouse", "Van Houten", false, 8, 3.5)`,
			expectedErr: true,
		},
		{
			name: "insert two rows, duplicate id with ignore",
			query: `insert ignore into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1),
					(7, "Milhouse", "Van Houten", false, 8, 3.5)`,
			insertedValues: []row.Row{
				newRow(7, "Maggie", "Simpson", false, 1, 5.1),
			},
			expectedResult: InsertResult{NumRowsInserted: 1, NumErrorsIgnored: 1},
		},
		{
			name:        "insert with missing required columns",
			query:       `insert into people (id) values (7)`,
			expectedErr: true,
		},
		{
			name:        "insert no primary keys",
			query:       `insert into people (age) values (7), (8)`,
			expectedErr: true,
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
			assert.Equal(t, tt.expectedErr, err != nil, "unexpected error value")

			if tt.expectedResult.Root == nil {
				return
			}

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
