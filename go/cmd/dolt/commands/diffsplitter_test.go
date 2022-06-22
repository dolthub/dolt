// Copyright 2022 Dolthub, Inc.
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

package commands

import (
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type splitRow struct {
	old, new rowDiff
}

func TestDiffSplitter(t *testing.T) {
	type testcase struct {
		name          string
		diffQuerySch  sql.Schema
		tableSch      sql.Schema
		diffQueryRows []sql.Row
		expectedRows  []splitRow
	}

	testcases := []testcase{
		{
			name: "changed rows",
			diffQuerySch: sql.Schema{
				{Name: "from_a"},
				{Name: "from_b"},
				{Name: "to_a"},
				{Name: "to_b"},
				{Name: "diff_type"},
			},
			tableSch: sql.Schema{
				{Name: "a"},
				{Name: "b"},
			},
			diffQueryRows: []sql.Row{
				{nil, nil, "1", "2", "added"},
				{"3", "4", nil, nil, "removed"},
				{"5", "6", "5", "100", "modified"},
			},
			expectedRows: []splitRow{
				{
					old: emptyRowDiff(2),
					new: rowDiff{
						row:      sql.Row{"1", "2"},
						rowDiff:  diff.Inserted,
						colDiffs: []diff.ChangeType{diff.Inserted, diff.Inserted},
					},
				},
				{
					old: rowDiff{
						row:      sql.Row{"3", "4"},
						rowDiff:  diff.Deleted,
						colDiffs: []diff.ChangeType{diff.Deleted, diff.Deleted},
					},
					new: emptyRowDiff(2),
				},
				{
					old: rowDiff{
						row:      sql.Row{"5", "6"},
						rowDiff:  diff.ModifiedOld,
						colDiffs: []diff.ChangeType{diff.None, diff.ModifiedOld},
					},
					new: rowDiff{
						row:      sql.Row{"5", "100"},
						rowDiff:  diff.ModifiedNew,
						colDiffs: []diff.ChangeType{diff.None, diff.ModifiedNew},
					},
				},
			},
		},
		{
			name: "added and removed column",
			diffQuerySch: sql.Schema{
				{Name: "from_a"},
				{Name: "from_b"},
				{Name: "to_b"},
				{Name: "to_c"},
				{Name: "diff_type"},
			},
			tableSch: sql.Schema{
				{Name: "a"},
				{Name: "b"},
				{Name: "c"},
			},
			diffQueryRows: []sql.Row{
				{nil, nil, "1", "2", "added"},
				{"3", "4", nil, nil, "removed"},
				{"5", "6", "6", "100", "modified"},
			},
			expectedRows: []splitRow{
				{
					old: emptyRowDiff(3),
					new: rowDiff{
						row:      sql.Row{nil, "1", "2"},
						rowDiff:  diff.Inserted,
						colDiffs: []diff.ChangeType{diff.None, diff.Inserted, diff.Inserted},
					},
				},
				{
					old: rowDiff{
						row:      sql.Row{"3", "4", nil},
						rowDiff:  diff.Deleted,
						colDiffs: []diff.ChangeType{diff.Deleted, diff.Deleted, diff.None},
					},
					new: emptyRowDiff(3),
				},
				{
					old: rowDiff{
						row:      sql.Row{"5", "6", nil},
						rowDiff:  diff.ModifiedOld,
						colDiffs: []diff.ChangeType{diff.None, diff.None, diff.None},
					},
					new: rowDiff{
						row:      sql.Row{nil, "6", "100"},
						rowDiff:  diff.ModifiedNew,
						colDiffs: []diff.ChangeType{diff.None, diff.None, diff.ModifiedNew},
					},
				},
			},
		},
		{
			name: "new table",
			diffQuerySch: sql.Schema{
				{Name: "to_a"},
				{Name: "to_b"},
				{Name: "diff_type"},
			},
			tableSch: sql.Schema{
				{Name: "a"},
				{Name: "b"},
			},
			diffQueryRows: []sql.Row{
				{"1", "2", "added"},
				{"3", "4", "added"},
			},
			expectedRows: []splitRow{
				{
					old: emptyRowDiff(2),
					new: rowDiff{
						row:      sql.Row{"1", "2"},
						rowDiff:  diff.Inserted,
						colDiffs: []diff.ChangeType{diff.Inserted, diff.Inserted},
					},
				},
				{
					old: emptyRowDiff(2),
					new: rowDiff{
						row:      sql.Row{"3", "4"},
						rowDiff:  diff.Inserted,
						colDiffs: []diff.ChangeType{diff.Inserted, diff.Inserted},
					},
				},
			},
		},
		{
			name: "dropped table",
			diffQuerySch: sql.Schema{
				{Name: "from_a"},
				{Name: "from_b"},
				{Name: "diff_type"},
			},
			tableSch: sql.Schema{
				{Name: "a"},
				{Name: "b"},
			},
			diffQueryRows: []sql.Row{
				{"1", "2", "removed"},
				{"3", "4", "removed"},
			},
			expectedRows: []splitRow{
				{
					new: emptyRowDiff(2),
					old: rowDiff{
						row:      sql.Row{"1", "2"},
						rowDiff:  diff.Deleted,
						colDiffs: []diff.ChangeType{diff.Deleted, diff.Deleted},
					},
				},
				{
					new: emptyRowDiff(2),
					old: rowDiff{
						row:      sql.Row{"3", "4"},
						rowDiff:  diff.Deleted,
						colDiffs: []diff.ChangeType{diff.Deleted, diff.Deleted},
					},
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ds, err := newDiffSplitter(tc.diffQuerySch, tc.tableSch)
			require.NoError(t, err)

			var splitRows []splitRow
			for _, row := range tc.diffQueryRows {
				old, new, err := ds.splitDiffResultRow(row)
				require.NoError(t, err)
				splitRows = append(splitRows, splitRow{old, new})
			}

			assert.Equal(t, tc.expectedRows, splitRows)
		})
	}
}

func emptyRowDiff(columns int) rowDiff {
	return rowDiff{

		colDiffs: make([]diff.ChangeType, columns),
	}
}
