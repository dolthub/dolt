package sql

import (
	"context"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	. "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExecuteInsert(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		insertedValues []row.Row
		expectedResult InsertResult // root is not compared, but it used for other assertions
		expectedErr    string
	}{
		{
			name: "insert one row, all columns",
			query: `insert into people (id, first, last, is_married, age, rating, uuid, num_episodes) values
					(7, "Maggie", "Simpson", false, 1, 5.1, '00000000-0000-0000-0000-000000000005', 677)`,
			insertedValues: []row.Row{NewPeopleRowWithOptionalFields(7, "Maggie", "Simpson", false, 1, 5.1, uuid.MustParse("00000000-0000-0000-0000-000000000005"), 677)},
			expectedResult: InsertResult{NumRowsInserted: 1},
		},
		{
			name: "insert one row, all columns, negative values",
			query: `insert into people (id, first, last, is_married, age, rating, uuid, num_episodes) values
					(-7, "Maggie", "Simpson", false, -1, -5.1, '00000000-0000-0000-0000-000000000005', 677)`,
			insertedValues: []row.Row{NewPeopleRowWithOptionalFields(-7, "Maggie", "Simpson", false, -1, -5.1, uuid.MustParse("00000000-0000-0000-0000-000000000005"), 677)},
			expectedResult: InsertResult{NumRowsInserted: 1},
		},
		{
			name: "insert one row, no column list",
			query: `insert into people values
					(7, "Maggie", "Simpson", false, 1, 5.1, '00000000-0000-0000-0000-000000000005', 677)`,
			insertedValues: []row.Row{NewPeopleRowWithOptionalFields(7, "Maggie", "Simpson", false, 1, 5.1, uuid.MustParse("00000000-0000-0000-0000-000000000005"), 677)},
			expectedResult: InsertResult{NumRowsInserted: 1},
		},
		{
			name: "insert one row out of order",
			query: `insert into people (rating, first, id, last, age, is_married) values
					(5.1, "Maggie", 7, "Simpson", 1, false)`,
			insertedValues: []row.Row{NewPeopleRow(7, "Maggie", "Simpson", false, 1, 5.1)},
			expectedResult: InsertResult{NumRowsInserted: 1},
		},
		{
			name: "insert one row, null values",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", null, null, null)`,
			insertedValues: []row.Row{row.New(PeopleTestSchema, row.TaggedValues{IdTag: types.Int(7), FirstTag: types.String("Maggie"), LastTag: types.String("Simpson")})},
			expectedResult: InsertResult{NumRowsInserted: 1},
		},
		{
			name: "insert one row, null constraint failure",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", null, null, null, null)`,
			expectedErr: "Constraint failed for column 'last': Not null",
		},
		{
			name: "duplicate column list",
			query: `insert into people (id, first, last, is_married, first, age, rating) values
					(7, "Maggie", "Simpson", null, null, null, null)`,
			expectedErr: "Repeated column: 'first'",
		},
		{
			name: "insert two rows, all columns",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1),
					(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
			insertedValues: []row.Row{
				NewPeopleRow(7, "Maggie", "Simpson", false, 1, 5.1),
				NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
			},
			expectedResult: InsertResult{NumRowsInserted: 2},
		},
		{
			name: "insert two rows, one with null constraint failure",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1),
					(8, "Milhouse", null, false, 8, 3.5)`,
			expectedErr: "Constraint failed for column 'last': Not null",
		},
		{
			name: "type mismatch int -> string",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", 100, false, 1, 5.1)`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch int -> bool",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", 10, 1, 5.1)`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch int -> uuid",
			query: `insert into people (id, first, last, is_married, age, uuid) values
					(7, "Maggie", "Simpson", false, 1, 100)`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch string -> int",
			query: `insert into people (id, first, last, is_married, age, rating) values
					("7", "Maggie", "Simpson", false, 1, 5.1)`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch string -> float",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, "5.1")`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch string -> uint",
			query: `insert into people (id, first, last, is_married, age, num_episodes) values
					(7, "Maggie", "Simpson", false, 1, "100")`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch string -> uuid",
			query: `insert into people (id, first, last, is_married, age, uuid) values
					(7, "Maggie", "Simpson", false, 1, "a uuid but idk what im doing")`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch float -> string",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, 8.1, "Simpson", false, 1, 5.1)`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch float -> bool",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", 0.5, 1, 5.1)`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch float -> int",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1.0, 5.1)`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch bool -> int",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(true, "Maggie", "Simpson", false, 1, 5.1)`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch bool -> float",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, true)`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch bool -> string",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, true, "Simpson", false, 1, 5.1)`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch bool -> uuid",
			query: `insert into people (id, first, last, is_married, age, uuid) values
					(7, "Maggie", "Simpson", false, 1, true)`,
			expectedErr: "Type mismatch",
		},
		{
			name: "insert two rows with ignore, one with null constraint failure",
			query: `insert ignore into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", null, false, 1, 5.1),
					(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
			insertedValues: []row.Row{
				NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
			},
			expectedResult: InsertResult{NumRowsInserted: 1, NumErrorsIgnored: 1},
		},
		{
			name: "insert existing rows without ignore / replace",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100)`,
			expectedErr: "Duplicate primary key: 'id: 0'",
		},
		{
			name: "insert two rows with ignore, one existing in table",
			query: `insert ignore into people (id, first, last, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100),
					(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
			insertedValues: []row.Row{
				NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
				Homer, // verify that homer is unchanged by the insert
			},
			expectedResult: InsertResult{NumRowsInserted: 1, NumErrorsIgnored: 1},
		},
		{
			name: "insert two rows with replace, one existing in table",
			query: `replace into people (id, first, last, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100),
					(8, "Milhouse", "Van Houten", false, 8, 3.5)`,
			insertedValues: []row.Row{
				NewPeopleRow(0, "Homer", "Simpson", true, 45, 100),
				NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
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
				NewPeopleRow(0, "Homer", "Simpson", true, 45, 100),
				NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
			},
			expectedResult: InsertResult{NumRowsInserted: 1, NumRowsUpdated: 1, NumErrorsIgnored: 1},
		},
		{
			name: "insert two rows with replace, one with errors",
			query: `replace into people (id, first, last, is_married, age, rating) values
					(0, "Homer", "Simpson", true, 45, 100),
					(8, "Milhouse", "Van Houten", false, 8, 3.5),
					(7, "Maggie", null, false, 1, 5.1)`,
			expectedErr: "Constraint failed for column 'last': Not null",
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
				NewPeopleRow(7, "Maggie", "Simpson", false, 1, 5.1),
				NewPeopleRow(8, "Milhouse", "Van Houten", false, 8, 3.5),
				NewPeopleRow(9, "Jacqueline", "Bouvier", true, 80, 2),
				NewPeopleRow(10, "Patty", "Bouvier", false, 40, 7),
				NewPeopleRow(11, "Selma", "Bouvier", false, 40, 7),
			},
			expectedResult: InsertResult{NumRowsInserted: 5},
		},
		{
			name: "insert two rows, only required columns",
			query: `insert into people (id, first, last) values
					(7, "Maggie", "Simpson"),
					(8, "Milhouse", "Van Houten")`,
			insertedValues: []row.Row{
				row.New(PeopleTestSchema, row.TaggedValues{IdTag: types.Int(7), FirstTag: types.String("Maggie"), LastTag: types.String("Simpson")}),
				row.New(PeopleTestSchema, row.TaggedValues{IdTag: types.Int(8), FirstTag: types.String("Milhouse"), LastTag: types.String("Van Houten")}),
			},
			expectedResult: InsertResult{NumRowsInserted: 2},
		},

		{
			name: "insert two rows, duplicate id",
			query: `insert into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1),
					(7, "Milhouse", "Van Houten", false, 8, 3.5)`,
			expectedErr: "Duplicate primary key: 'id: 7'",
		},
		{
			name: "insert two rows, duplicate id with ignore",
			query: `insert ignore into people (id, first, last, is_married, age, rating) values
					(7, "Maggie", "Simpson", false, 1, 5.1),
					(7, "Milhouse", "Van Houten", false, 8, 3.5)`,
			insertedValues: []row.Row{
				NewPeopleRow(7, "Maggie", "Simpson", false, 1, 5.1),
			},
			expectedResult: InsertResult{NumRowsInserted: 1, NumErrorsIgnored: 1},
		},
		{
			name:        "insert no primary keys",
			query:       `insert into people (age) values (7), (8)`,
			expectedErr: "One or more primary key columns missing from insert statement",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if len(tt.expectedErr) > 0 {
				require.Equal(t, InsertResult{}, tt.expectedResult, "incorrect test setup: cannot assert both an error and expected results")
				require.Nil(t, tt.insertedValues, "incorrect test setup: cannot assert both an error and inserted values")
			}

			dEnv := dtestutils.CreateTestEnv()
			ctx := context.Background()

			CreateTestDatabase(dEnv, t)
			root, _ := dEnv.WorkingRoot(ctx)

			sqlStatement, _ := sqlparser.Parse(tt.query)
			s := sqlStatement.(*sqlparser.Insert)

			result, err := ExecuteInsert(ctx, dEnv.DoltDB, root, s, tt.query)

			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectedResult.NumRowsInserted, result.NumRowsInserted)
			assert.Equal(t, tt.expectedResult.NumErrorsIgnored, result.NumErrorsIgnored)
			assert.Equal(t, tt.expectedResult.NumRowsUpdated, result.NumRowsUpdated)

			table, ok := result.Root.GetTable(ctx, PeopleTableName)
			assert.True(t, ok)

			for _, expectedRow := range tt.insertedValues {
				foundRow, ok := table.GetRow(ctx, expectedRow.NomsMapKey(PeopleTestSchema).Value(ctx).(types.Tuple), PeopleTestSchema)
				assert.True(t, ok, "Row not found: %v", expectedRow)
				opts := cmp.Options{cmp.AllowUnexported(expectedRow), dtestutils.FloatComparer}
				assert.True(t, cmp.Equal(expectedRow, foundRow, opts), "Rows not equals, found diff %v", cmp.Diff(expectedRow, foundRow, opts))
			}
		})
	}
}
