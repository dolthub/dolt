package sql

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/xwb1989/sqlparser"
)

func TestExecuteUpdate(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		updatedRows    []row.Row
		expectedResult UpdateResult // root is not compared, but it's used for other assertions
		expectedErr    string
	}{
		{
			name:           "update one row, one col, primary key where clause",
			query:          `update people set first = "Domer" where id = 0`,
			updatedRows:    []row.Row{mutateRow(homer, firstTag, "Domer")},
			expectedResult: UpdateResult{NumRowsUpdated: 1},
		},
		{
			name:           "update one row, one col, non-primary key where clause",
			query:          `update people set first = "Domer" where first = "Homer"`,
			updatedRows:    []row.Row{mutateRow(homer, firstTag, "Domer")},
			expectedResult: UpdateResult{NumRowsUpdated: 1},
		},
		{
			name:           "update one row, two cols, primary key where clause",
			query:          `update people set first = "Ned", last = "Flanders" where id = 0`,
			updatedRows:    []row.Row{mutateRow(homer, firstTag, "Ned", lastTag, "Flanders")},
			expectedResult: UpdateResult{NumRowsUpdated: 1},
		},
		{
			name: "update one row, all cols, non-primary key where clause",
			query: `update people set first = "Ned", last = "Flanders", is_married = false, rating = 10,
				age = 45, num_episodes = 150, uuid = '00000000-0000-0000-0000-000000000050'
				where age = 38`,
			updatedRows: []row.Row{
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
			updatedRows:    []row.Row{},
			expectedResult: UpdateResult{NumRowsUpdated: 0, NumRowsUnchanged: 1},
		},
		{
			name: "update one row, null out existing values",
			query: `update people set first = "Homer", last = "Simpson", is_married = null, rating = null, age = null,
				num_episodes = null, uuid = null
				where first = "Homer"`,
			updatedRows:    []row.Row{mutateRow(homer, isMarriedTag, nil, ratingTag, nil, ageTag, nil)},
			expectedResult: UpdateResult{NumRowsUpdated: 1},
		},
		{
			name: "update multiple rows, set two columns",
			query: `update people set first = "Changed", rating = 0.0
				where last = "Simpson"`,
			updatedRows:   []row.Row{
				mutateRow(homer, firstTag, "Changed", ratingTag, 0.0),
				mutateRow(marge, firstTag, "Changed", ratingTag, 0.0),
				mutateRow(bart, firstTag, "Changed", ratingTag, 0.0),
				mutateRow(lisa, firstTag, "Changed", ratingTag, 0.0),
			},
			expectedResult: UpdateResult{NumRowsUpdated: 4, NumRowsUnchanged: 0},
		},
		{
			name: "update no matching rows",
			query: `update people set first = "Changed", rating = 0.0
				where last = "Flanders"`,
			updatedRows:   []row.Row{},
			expectedResult: UpdateResult{NumRowsUpdated: 0, NumRowsUnchanged: 0},
		},
		{
			name: "update without where clause",
			query: `update people set first = "Changed", rating = 0.0`,
			updatedRows:   []row.Row{
				mutateRow(homer, firstTag, "Changed", ratingTag, 0.0),
				mutateRow(marge, firstTag, "Changed", ratingTag, 0.0),
				mutateRow(bart, firstTag, "Changed", ratingTag, 0.0),
				mutateRow(lisa, firstTag, "Changed", ratingTag, 0.0),
				mutateRow(moe, firstTag, "Changed", ratingTag, 0.0),
				mutateRow(barney, firstTag, "Changed", ratingTag, 0.0),
			},
			expectedResult: UpdateResult{NumRowsUpdated: 6, NumRowsUnchanged: 0},
		},
		{
			name: "update set first = last",
			query: `update people set first = last`,
			updatedRows:   []row.Row{
				mutateRow(homer, firstTag, "Simpson"),
				mutateRow(marge, firstTag, "Simpson"),
				mutateRow(bart, firstTag, "Simpson"),
				mutateRow(lisa, firstTag, "Simpson"),
				mutateRow(moe, firstTag, "Szyslak"),
				mutateRow(barney, firstTag, "Gumble"),
			},
			expectedResult: UpdateResult{NumRowsUpdated: 6, NumRowsUnchanged: 0},
		},
		{
			name: "update increment age",
			query: `update people set age = age + 1`,
			updatedRows:   []row.Row{
				mutateRow(homer, ageTag, 41),
				mutateRow(marge, ageTag, 39),
				mutateRow(bart, ageTag, 11),
				mutateRow(lisa, ageTag, 9),
				mutateRow(moe, ageTag, 49),
				mutateRow(barney, ageTag, 41),
			},
			expectedResult: UpdateResult{NumRowsUpdated: 6, NumRowsUnchanged: 0},
		},
		{
			name: "update multiple rows, =",
			query: `update people set first = "Homer"
				where last = "Simpson"`,
			updatedRows:   []row.Row{
				mutateRow(marge, firstTag, "Homer"),
				mutateRow(bart, firstTag, "Homer"),
				mutateRow(lisa, firstTag, "Homer"),
			},
			expectedResult: UpdateResult{NumRowsUpdated: 3, NumRowsUnchanged: 1},
		},
		{
			name: "update multiple rows, <>",
			query: `update people set last = "Simpson"
				where last <> "Simpson"`,
			updatedRows:   []row.Row{
				mutateRow(moe, lastTag, "Simpson"),
				mutateRow(barney, lastTag, "Simpson"),
			},
			expectedResult: UpdateResult{NumRowsUpdated: 2, NumRowsUnchanged: 0},
		},
		{
			name: "update multiple rows, >",
			query: `update people set first = "Homer" where age > 10`,
			updatedRows:   []row.Row{
				mutateRow(marge, firstTag, "Homer"),
				mutateRow(moe, firstTag, "Homer"),
				mutateRow(barney, firstTag, "Homer"),
			},
			expectedResult: UpdateResult{NumRowsUpdated: 3, NumRowsUnchanged: 1},
		},
		{
			name: "update multiple rows, >=",
			query: `update people set first = "Homer" where age >= 10`,
			updatedRows:   []row.Row{
				mutateRow(marge, firstTag, "Homer"),
				mutateRow(bart, firstTag, "Homer"),
				mutateRow(moe, firstTag, "Homer"),
				mutateRow(barney, firstTag, "Homer"),
			},
			expectedResult: UpdateResult{NumRowsUpdated: 4, NumRowsUnchanged: 1},
		},
		{
			name: "update multiple rows, <",
			query: `update people set first = "Bart" where age < 40`,
			updatedRows:   []row.Row{
				mutateRow(marge, firstTag, "Bart"),
				mutateRow(lisa, firstTag, "Bart"),
			},
			expectedResult: UpdateResult{NumRowsUpdated: 2, NumRowsUnchanged: 1},
		},
		{
			name: "update multiple rows, <=",
			query: `update people set first = "Homer" where age <= 40`,
			updatedRows:   []row.Row{
				mutateRow(marge, firstTag, "Homer"),
				mutateRow(bart, firstTag, "Homer"),
				mutateRow(lisa, firstTag, "Homer"),
				mutateRow(barney, firstTag, "Homer"),
			},
			expectedResult: UpdateResult{NumRowsUpdated: 4, NumRowsUnchanged: 1},
		},
		{
			name: "existing row key collision",
			query: `update people set id = 0 where first = "Marge"`,
			expectedErr: "duplicate primary key",
		},
		{
			name: "duplicate primary keys in updated rows",
			query: `update people set id = 100 where last = "Simpson"`,
			expectedErr: "duplicate primary key",
		},
		{
			name: "duplicate column in update list",
			query: `update people set first = "Marge", first = "Homer", last = "Simpson"`,
			expectedErr: "Repeated column 'first'",
		},
		{
			name: "null constraint failure",
			query: `update people set first = null where id = 0`,
			expectedErr: "row constraint failed",
		},
		{
			name: "type mismatch int -> string",
			query: `update people set first = 1 where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch int -> bool",
			query: `update people set is_married = 0 where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch int -> uuid",
			query: `update people set uuid = 0 where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch string -> int",
			query: `update people set age = "pretty old" where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch string -> float",
			query: `update people set rating = "great" where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch string -> uint",
			query: `update people set num_episodes = "all of them" where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch string -> uuid",
			query: `update people set uuid = "not a uuid string" where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch float -> string",
			query: `update people set last = 1.0 where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch float -> bool",
			query: `update people set is_married = 1.0 where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch float -> int",
			query: `update people set num_episodes = 1.5 where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch bool -> int",
			query: `update people set age = true where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch bool -> float",
			query: `update people set rating = false where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch bool -> string",
			query: `update people set last = true where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch bool -> uuid",
			query: `update people set uuid = false where id = 0`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch in where clause",
			query: `update people set first = "Homer" where id = "id"`,
			expectedErr: "Type mismatch",
		},
		{
			name: "type mismatch in where clause",
			query: `update people set first = "Homer" where id = "0"`,
			expectedErr: "Type mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			createTestDatabase(dEnv, t)
			root, _ := dEnv.WorkingRoot(context.Background())

			sqlStatement, _ := sqlparser.Parse(tt.query)
			s := sqlStatement.(*sqlparser.Update)

			if len(tt.expectedErr) > 0 {
				require.Equal(t, UpdateResult{}, tt.expectedResult, "incorrect test setup: cannot assert both an error and expected results")
				require.Nil(t, tt.updatedRows, "incorrect test setup: cannot assert both an error and updated values")
			}

			result, err := ExecuteUpdate(context.Background(), dEnv.DoltDB, root, s, tt.query)

			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			}

			assert.Equal(t, tt.expectedResult.NumRowsUpdated, result.NumRowsUpdated)
			assert.Equal(t, tt.expectedResult.NumRowsUnchanged, result.NumRowsUnchanged)
			assert.Equal(t, tt.expectedResult.NumErrorsIgnored, result.NumErrorsIgnored)

			table, ok := result.Root.GetTable(context.Background(), peopleTableName)
			assert.True(t, ok)

			// make sure exactly the expected rows were updated
			for _, r := range allPeopleRows {
				updatedIdx := findRowIndex(r, tt.updatedRows)

				expectedRow := r
				if updatedIdx >= 0 {
					expectedRow = tt.updatedRows[updatedIdx]
				}

				foundRow, ok := table.GetRow(context.Background(), expectedRow.NomsMapKey(peopleTestSchema).(types.Tuple), peopleTestSchema)
				assert.True(t, ok, "Row not found: %v", expectedRow)
				opts := cmp.Options{cmp.AllowUnexported(expectedRow), floatComparer}
				assert.True(t, cmp.Equal(expectedRow, foundRow, opts), "Rows not equals, found diff %v", cmp.Diff(expectedRow, foundRow, opts))
			}
		})
	}
}
