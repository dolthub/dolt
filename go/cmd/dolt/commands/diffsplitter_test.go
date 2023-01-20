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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
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
				intCol("from_a"),
				intCol("from_b"),
				intCol("to_a"),
				intCol("to_b"),
				strCol("diff_type"),
			},
			tableSch: sql.Schema{
				intCol("a"),
				intCol("b"),
			},
			diffQueryRows: []sql.Row{
				{nil, nil, 1, 2, "added"},
				{3, 4, nil, nil, "removed"},
				{5, 6, 5, 100, "modified"},
			},
			expectedRows: []splitRow{
				{
					old: emptyRowDiff(2),
					new: rowDiff{
						row:      sql.Row{1, 2},
						rowDiff:  diff.Added,
						colDiffs: []diff.ChangeType{diff.Added, diff.Added},
					},
				},
				{
					old: rowDiff{
						row:      sql.Row{3, 4},
						rowDiff:  diff.Removed,
						colDiffs: []diff.ChangeType{diff.Removed, diff.Removed},
					},
					new: emptyRowDiff(2),
				},
				{
					old: rowDiff{
						row:      sql.Row{5, 6},
						rowDiff:  diff.ModifiedOld,
						colDiffs: []diff.ChangeType{diff.None, diff.ModifiedOld},
					},
					new: rowDiff{
						row:      sql.Row{5, 100},
						rowDiff:  diff.ModifiedNew,
						colDiffs: []diff.ChangeType{diff.None, diff.ModifiedNew},
					},
				},
			},
		},
		{
			name: "added and removed column",
			diffQuerySch: sql.Schema{
				intCol("from_a"),
				intCol("from_b"),
				intCol("to_b"),
				intCol("to_c"),
				strCol("diff_type"),
			},
			tableSch: sql.Schema{
				intCol("a"),
				intCol("b"),
				intCol("c"),
			},
			diffQueryRows: []sql.Row{
				{nil, nil, 1, 2, "added"},
				{3, 4, nil, nil, "removed"},
				{5, 6, 6, 100, "modified"},
			},
			expectedRows: []splitRow{
				{
					old: emptyRowDiff(3),
					new: rowDiff{
						row:      sql.Row{nil, 1, 2},
						rowDiff:  diff.Added,
						colDiffs: []diff.ChangeType{diff.None, diff.Added, diff.Added},
					},
				},
				{
					old: rowDiff{
						row:      sql.Row{3, 4, nil},
						rowDiff:  diff.Removed,
						colDiffs: []diff.ChangeType{diff.Removed, diff.Removed, diff.None},
					},
					new: emptyRowDiff(3),
				},
				{
					old: rowDiff{
						row:      sql.Row{5, 6, nil},
						rowDiff:  diff.ModifiedOld,
						colDiffs: []diff.ChangeType{diff.ModifiedOld, diff.None, diff.None},
					},
					new: rowDiff{
						row:      sql.Row{nil, 6, 100},
						rowDiff:  diff.ModifiedNew,
						colDiffs: []diff.ChangeType{diff.None, diff.None, diff.ModifiedNew},
					},
				},
			},
		},
		{
			name: "new table",
			diffQuerySch: sql.Schema{
				intCol("to_a"),
				intCol("to_b"),
				strCol("diff_type"),
			},
			tableSch: sql.Schema{
				intCol("a"),
				intCol("b"),
			},
			diffQueryRows: []sql.Row{
				{1, 2, "added"},
				{3, 4, "added"},
			},
			expectedRows: []splitRow{
				{
					old: emptyRowDiff(2),
					new: rowDiff{
						row:      sql.Row{1, 2},
						rowDiff:  diff.Added,
						colDiffs: []diff.ChangeType{diff.Added, diff.Added},
					},
				},
				{
					old: emptyRowDiff(2),
					new: rowDiff{
						row:      sql.Row{3, 4},
						rowDiff:  diff.Added,
						colDiffs: []diff.ChangeType{diff.Added, diff.Added},
					},
				},
			},
		},
		{
			name: "dropped table",
			diffQuerySch: sql.Schema{
				intCol("from_a"),
				intCol("from_b"),
				strCol("diff_type"),
			},
			tableSch: sql.Schema{
				intCol("a"),
				intCol("b"),
			},
			diffQueryRows: []sql.Row{
				{1, 2, "removed"},
				{3, 4, "removed"},
			},
			expectedRows: []splitRow{
				{
					new: emptyRowDiff(2),
					old: rowDiff{
						row:      sql.Row{1, 2},
						rowDiff:  diff.Removed,
						colDiffs: []diff.ChangeType{diff.Removed, diff.Removed},
					},
				},
				{
					new: emptyRowDiff(2),
					old: rowDiff{
						row:      sql.Row{3, 4},
						rowDiff:  diff.Removed,
						colDiffs: []diff.ChangeType{diff.Removed, diff.Removed},
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

func strCol(name string) *sql.Column {
	return &sql.Column{Name: name, Type: types.Text}
}

func intCol(name string) *sql.Column {
	return &sql.Column{Name: name, Type: types.Int64}
}
