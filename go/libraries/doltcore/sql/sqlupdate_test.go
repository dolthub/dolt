package sql

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/xwb1989/sqlparser"
)

func TestExecuteUpdate(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		updatedValues  []row.Row
		expectedResult UpdateResult // root is not compared, but it's used for other assertions
		expectedErr    bool
	}{
		{
			name: "update one row, one col, primary key where clause",
			query: `update people set first = "Domer" where id = 0`,
			updatedValues:  []row.Row{mutateRow(homer, firstTag, "Domer")},
			expectedResult: UpdateResult{NumRowsUpdated: 1},
		},
		{
			name: "update one row, one col, non-primary key where clause",
			query: `update people set first = "Domer" where first = "Homer"`,
			updatedValues:  []row.Row{mutateRow(homer, firstTag, "Domer")},
			expectedResult: UpdateResult{NumRowsUpdated: 1},
		},
		{
			name: "update one row, two cols, primary key where clause",
			query: `update people set first = "Ned", last = "Flanders" where id = 0`,
			updatedValues:  []row.Row{mutateRow(homer, firstTag, "Ned", lastTag, "Flanders")},
			expectedResult: UpdateResult{NumRowsUpdated: 1},
		},
		{
			name: "update one row, all cols, non-primary key where clause",
			query: `update people set first = "Ned", last = "Flanders", is_married = false, rating = 10,
				age = 45, num_episodes = 150, uuid = '00000000-0000-0000-0000-000000000050'
				where age = 38`,
			updatedValues: []row.Row{
				mutateRow(marge, firstTag, "Ned", lastTag, "Flanders", isMarriedTag, false,
					ratingTag, 10.0, ageTag, 45, numEpisodesTag, uint64(150),
					uuidTag, uuid.MustParse("00000000-0000-0000-0000-000000000050"))},
			expectedResult: UpdateResult{NumRowsUpdated: 1},
		},
		{
			name: "update one row, set columns to existing values",
			query: `update people set first = "Homer", last = "Simpson", is_married = true, rating = 8.5, age = 40,
				num_episodes = null, uuid = null
				where id = 0`,
			updatedValues: []row.Row{homer}, // verify homer is unchanged by the update
			expectedResult: UpdateResult{NumRowsUpdated: 0, NumRowsIgnored: 0},
		},


		//{
		//	name: "update one row, all columns",
		//	query: `insert into people (id, first, last, is_married, age, rating, uuid, num_episodes) values
		//			(7, "Maggie", "Simpson", false, 1, 5.1, '00000000-0000-0000-0000-000000000005', 677)`,
		//	updatedValues: []row.Row{newRowWithOptionalFields(7, "Maggie", "Simpson", false, 1, 5.1, uuid.MustParse("00000000-0000-0000-0000-000000000005"), 677)},
		//	expectedResult: UpdateResult{NumRowsUpdated: 1},
		//},
		//{
		//	name: "insert one row, no column list",
		//	query: `insert into people values
		//			(7, "Maggie", "Simpson", false, 1, 5.1, '00000000-0000-0000-0000-000000000005', 677)`,
		//	updatedValues: []row.Row{newRowWithOptionalFields(7, "Maggie", "Simpson", false, 1, 5.1, uuid.MustParse("00000000-0000-0000-0000-000000000005"), 677)},
		//	expectedResult: InsertResult{NumRowsInserted: 1},
		//},
		//{
		//	name: "insert one row out of order",
		//	query: `insert into people (rating, first, id, last, age, is_married) values
		//			(5.1, "Maggie", 7, "Simpson", 1, false)`,
		//	updatedValues: []row.Row{newRow(7, "Maggie", "Simpson", false, 1, 5.1)},
		//	expectedResult: InsertResult{NumRowsInserted: 1},
		//},
		//{
		//	name: "insert one row, null values",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", "Simpson", null, null, null)`,
		//	updatedValues: []row.Row{row.New(testSch, row.TaggedValues{idTag: types.Int(7), firstTag: types.String("Maggie"), lastTag: types.String("Simpson")})},
		//	expectedResult: InsertResult{NumRowsInserted: 1},
		//},
		//{
		//	name: "insert one row, null key columns",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", null, null, null, null)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "duplicate column list",
		//	query: `insert into people (id, first, last, is_married, first, age, rating) values
		//			(7, "Maggie", "Simpson", null, null, null, null)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "insert two rows, all columns",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", "Simpson", false, 1, 5.1),
		//			(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
		//	updatedValues: []row.Row{
		//		newRow(7, "Maggie", "Simpson", false, 1, 5.1),
		//		newRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
		//	},
		//	expectedResult: InsertResult{NumRowsInserted: 2},
		//},
		//{
		//	name: "insert two rows, one with null constraint failure",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", "Simpson", false, 1, 5.1),
		//			(8, "Milhouse", null, false, 8, 3.5)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch int -> string",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", 100, false, 1, 5.1)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch int -> bool",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", "Simpson", 10, 1, 5.1)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch int -> uuid",
		//	query: `insert into people (id, first, last, is_married, age, uuid) values
		//			(7, "Maggie", "Simpson", false, 1, 100)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch string -> int",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			("7", "Maggie", "Simpson", false, 1, 5.1)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch string -> float",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", "Simpson", false, 1, "5.1")`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch string -> uint",
		//	query: `insert into people (id, first, last, is_married, age, num_episodes) values
		//			(7, "Maggie", "Simpson", false, 1, "100")`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch string -> uuid",
		//	query: `insert into people (id, first, last, is_married, age, uuid) values
		//			(7, "Maggie", "Simpson", false, 1, "a uuid but idk what im doing")`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch float -> string",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, 8.1, "Simpson", false, 1, 5.1)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch float -> bool",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", "Simpson", 0.5, 1, 5.1)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch float -> int",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", "Simpson", false, 1.0, 5.1)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch bool -> int",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(true, "Maggie", "Simpson", false, 1, 5.1)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch bool -> float",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", "Simpson", false, 1, true)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch bool -> string",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, true, "Simpson", false, 1, 5.1)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "type mismatch bool -> uuid",
		//	query: `insert into people (id, first, last, is_married, age, uuid) values
		//			(7, "Maggie", "Simpson", false, 1, true)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "insert two rows with ignore, one with null constraint failure",
		//	query: `insert ignore into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", null, false, 1, 5.1),
		//			(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
		//	updatedValues: []row.Row{
		//		newRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
		//	},
		//	expectedResult: InsertResult{NumRowsInserted: 1, NumErrorsIgnored: 1},
		//},
		//{
		//	name: "insert existing rows without ignore / replace",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(0, "Homer", "Simpson", true, 45, 100)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "insert two rows with ignore, one existing in table",
		//	query: `insert ignore into people (id, first, last, is_married, age, rating) values
		//			(0, "Homer", "Simpson", true, 45, 100),
		//			(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
		//	updatedValues: []row.Row{
		//		newRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
		//		homer, // verify that homer is unchanged by the insert
		//	},
		//	expectedResult: InsertResult{NumRowsInserted: 1, NumErrorsIgnored: 1},
		//},
		//{
		//	name: "insert two rows with replace, one existing in table",
		//	query: `replace into people (id, first, last, is_married, age, rating) values
		//			(0, "Homer", "Simpson", true, 45, 100),
		//			(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
		//	updatedValues: []row.Row{
		//		newRow(0, "Homer", "Simpson", true, 45, 100),
		//		newRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
		//	},
		//	expectedResult: InsertResult{NumRowsInserted: 1, NumRowsUpdated: 1},
		//},
		//{
		//	name: "insert two rows with replace ignore, one with errors",
		//	query: `replace ignore into people (id, first, last, is_married, age, rating) values
		//			(0, "Homer", "Simpson", true, 45, 100),
		//			(8, "Milhouse", "Van Houten", false, 8, 3.5),
		//			(7, "Maggie", null, false, 1, 5.1)`,
		//	updatedValues: []row.Row{
		//		newRow(0, "Homer", "Simpson", true, 45, 100),
		//		newRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
		//	},
		//	expectedResult: InsertResult{NumRowsInserted: 1, NumRowsUpdated: 1, NumErrorsIgnored: 1},
		//},
		//{
		//	name: "insert two rows with replace, one with errors",
		//	query: `replace into people (id, first, last, is_married, age, rating) values
		//			(0, "Homer", "Simpson", true, 45, 100),
		//			(8, "Milhouse", "Van Houten", false, 8, 3.5),
		//			(7, "Maggie", null, false, 1, 5.1)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "insert five rows, all columns",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", "Simpson", false, 1, 5.1),
		//			(8, "Milhouse", "Van Houten", false, 8, 3.5),
		//			(9, "Jacqueline", "Bouvier", true, 80, 2),
		//			(10, "Patty", "Bouvier", false, 40, 7),
		//			(11, "Selma", "Bouvier", false, 40, 7)`,
		//	updatedValues: []row.Row{
		//		newRow(7, "Maggie", "Simpson", false, 1, 5.1),
		//		newRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
		//		newRow(9, "Jacqueline", "Bouvier", true, 80, 2),
		//		newRow(10, "Patty", "Bouvier", false, 40, 7),
		//		newRow(11, "Selma", "Bouvier", false, 40, 7),
		//	},
		//	expectedResult: InsertResult{NumRowsInserted: 5},
		//},
		//{
		//	name: "insert two rows, only required columns",
		//	query: `insert into people (id, first, last) values
		//			(7, "Maggie", "Simpson"),
		//			(8, "Milhouse", "Van Houten")`,
		//	updatedValues: []row.Row{
		//		row.New(testSch, row.TaggedValues{idTag: types.Int(7), firstTag: types.String("Maggie"), lastTag: types.String("Simpson")}),
		//		row.New(testSch, row.TaggedValues{idTag: types.Int(8), firstTag: types.String("Milhouse"), lastTag: types.String("Van Houten")}),
		//	},
		//	expectedResult: InsertResult{NumRowsInserted: 2},
		//},
		//
		//{
		//	name: "insert two rows, duplicate id",
		//	query: `insert into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", "Simpson", false, 1, 5.1),
		//			(7, "Milhouse", "Van Houten", false, 8, 3.5)`,
		//	expectedErr: true,
		//},
		//{
		//	name: "insert two rows, duplicate id with ignore",
		//	query: `insert ignore into people (id, first, last, is_married, age, rating) values
		//			(7, "Maggie", "Simpson", false, 1, 5.1),
		//			(7, "Milhouse", "Van Houten", false, 8, 3.5)`,
		//	updatedValues: []row.Row{
		//		newRow(7, "Maggie", "Simpson", false, 1, 5.1),
		//	},
		//	expectedResult: InsertResult{NumRowsInserted: 1, NumErrorsIgnored: 1},
		//},
		//{
		//	name:        "insert with missing required columns",
		//	query:       `insert into people (id) values (7)`,
		//	expectedErr: true,
		//},
		//{
		//	name:        "insert no primary keys",
		//	query:       `insert into people (age) values (7), (8)`,
		//	expectedErr: true,
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			createTestDatabase(dEnv, t)
			root, _ := dEnv.WorkingRoot()

			sqlStatement, _ := sqlparser.Parse(tt.query)
			s := sqlStatement.(*sqlparser.Update)

			result, err := ExecuteUpdate(dEnv.DoltDB, root, s, tt.query)
			if tt.expectedErr {
				assert.True(t, err != nil, "expected error")
				assert.Equal(t, InsertResult{}, tt.expectedResult, "incorrect test setup: cannot assert both an error and expected results")
				assert.Nil(t, tt.updatedValues, "incorrect test setup: cannot assert both an error and updated values")
				return
			} else {
				assert.Nil(t, err, "unexpected error")
			}

			assert.Equal(t, tt.expectedResult.NumRowsUpdated, result.NumRowsUpdated)
			assert.Equal(t, tt.expectedResult.NumErrorsIgnored, result.NumErrorsIgnored)

			table, ok := result.Root.GetTable(testTableName)
			assert.True(t, ok)

			for _, expectedRow := range tt.updatedValues {
				foundRow, ok := table.GetRow(expectedRow.NomsMapKey(testSch).(types.Tuple), testSch)
				assert.True(t, ok, "Row not found: %v", expectedRow)
				opts := cmp.Options{cmp.AllowUnexported(expectedRow), floatComparer}
				assert.True(t, cmp.Equal(expectedRow, foundRow, opts), "Rows not equals, found diff %v", cmp.Diff(expectedRow, foundRow, opts))
			}
		})
	}
}
