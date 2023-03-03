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

package diff

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type splitRow struct {
	old, new RowDiff
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
					new: RowDiff{
						Row:      sql.Row{1, 2},
						RowDiff:  Added,
						ColDiffs: []ChangeType{Added, Added},
					},
				},
				{
					old: RowDiff{
						Row:      sql.Row{3, 4},
						RowDiff:  Removed,
						ColDiffs: []ChangeType{Removed, Removed},
					},
					new: emptyRowDiff(2),
				},
				{
					old: RowDiff{
						Row:      sql.Row{5, 6},
						RowDiff:  ModifiedOld,
						ColDiffs: []ChangeType{None, ModifiedOld},
					},
					new: RowDiff{
						Row:      sql.Row{5, 100},
						RowDiff:  ModifiedNew,
						ColDiffs: []ChangeType{None, ModifiedNew},
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
					new: RowDiff{
						Row:      sql.Row{nil, 1, 2},
						RowDiff:  Added,
						ColDiffs: []ChangeType{None, Added, Added},
					},
				},
				{
					old: RowDiff{
						Row:      sql.Row{3, 4, nil},
						RowDiff:  Removed,
						ColDiffs: []ChangeType{Removed, Removed, None},
					},
					new: emptyRowDiff(3),
				},
				{
					old: RowDiff{
						Row:      sql.Row{5, 6, nil},
						RowDiff:  ModifiedOld,
						ColDiffs: []ChangeType{ModifiedOld, None, None},
					},
					new: RowDiff{
						Row:      sql.Row{nil, 6, 100},
						RowDiff:  ModifiedNew,
						ColDiffs: []ChangeType{None, None, ModifiedNew},
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
					new: RowDiff{
						Row:      sql.Row{1, 2},
						RowDiff:  Added,
						ColDiffs: []ChangeType{Added, Added},
					},
				},
				{
					old: emptyRowDiff(2),
					new: RowDiff{
						Row:      sql.Row{3, 4},
						RowDiff:  Added,
						ColDiffs: []ChangeType{Added, Added},
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
					old: RowDiff{
						Row:      sql.Row{1, 2},
						RowDiff:  Removed,
						ColDiffs: []ChangeType{Removed, Removed},
					},
				},
				{
					new: emptyRowDiff(2),
					old: RowDiff{
						Row:      sql.Row{3, 4},
						RowDiff:  Removed,
						ColDiffs: []ChangeType{Removed, Removed},
					},
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ds, err := NewDiffSplitter(tc.diffQuerySch, tc.tableSch)
			require.NoError(t, err)

			var splitRows []splitRow
			for _, row := range tc.diffQueryRows {
				old, new, err := ds.SplitDiffResultRow(row)
				require.NoError(t, err)
				splitRows = append(splitRows, splitRow{old, new})
			}

			assert.Equal(t, tc.expectedRows, splitRows)
		})
	}
}

func emptyRowDiff(columns int) RowDiff {
	return RowDiff{
		ColDiffs: make([]ChangeType, columns),
	}
}

func strCol(name string) *sql.Column {
	return &sql.Column{Name: name, Type: types.Text}
}

func intCol(name string) *sql.Column {
	return &sql.Column{Name: name, Type: types.Int64}
}
