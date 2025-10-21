// Copyright 2023 Dolthub, Inc.
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

package merge_test

import (
	"context"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

type dataMergeTest struct {
	name                string
	schema              namedSchema
	conflict            bool
	skipNewFmt          bool
	skipOldFmt          bool
	skipFlipOnNewFormat bool
	skipFlipOnOldFormat bool
	dataTests           []dataTest
}

// TestDataMerge are data merge integration tests
func TestDataMerge(t *testing.T) {
	t.Run("column add/drop tests", func(t *testing.T) {
		testDataMerge(t, []dataMergeTest{shiftingNodeBoundariesTest})
	})
}

// shiftingNodeBoundariesTest is a regression test for a merge bug that could occur when one side of the merge adds a
// new prolly tree node, but none of the keys in that new node have changed from the common ancestor. The merge
// algorithm assumed that any new node on one side of the merge would necessarily contain a changed key-value pair, but
// this is not guaranteed. One example where this could cause an error is when all the following are true:
//   - An insert caused a shift in node boundaries on one side of the merge.
//   - The node immediately prior to this is unchanged on at least one branch.
//   - As a result of the shift, both sides of the merge now contain new nodes that have the same end key,
//     but different start keys.
//   - One of these two new nodes contains only key-value pairs from the common ancestor, but the other node does not.
//   - After this point, neither branch has any additional changes from the common ancestor.
//
// In this situation, the merge algorithm would hit an unexpected EOF error while attempting to diff the two nodes.
//
// The below table recreates these conditions: inserting the key 15 shifts the node boundaries, resulting in the right
// branch containing a node with keys ranging from _ to _, all of which match the ancestor. The left branch contains
// a node with keys ranging from _ to _, which also includes the key 40.
var shiftingNodeBoundariesTest = func() dataMergeTest {
	charString := strings.Repeat("1", 255)
	var rows []sql.Row
	for i := 0; i < 64; i++ {
		rows = append(rows, sql.NewRow(i, charString))
	}
	rowsWithHoles := func(holes ...int) []sql.Row {
		if len(holes) == 0 {
			return rows
		}
		var result []sql.Row
		result = append(result, rows[:holes[0]]...)
		previousHole := holes[0]
		for _, hole := range holes[1:] {
			result = append(result, rows[previousHole+1:hole]...)
			previousHole = hole
		}
		result = append(result, rows[previousHole+1:]...)
		return result
	}
	return dataMergeTest{
		name:   "insert rows that shift chunk boundaries",
		schema: sch("CREATE TABLE t (id int PRIMARY KEY, t char(255))"),
		dataTests: []dataTest{
			{
				name:     "left side adds column and assigns non-null value",
				ancestor: rowsWithHoles(15, 40),
				left:     rowsWithHoles(15),
				right:    rowsWithHoles(40),
				merged:   rows,
			},
		},
	}
}()

func testDataMerge(t *testing.T, tests []dataMergeTest) {
	t.Run("merge left to right", func(t *testing.T) {
		testDataMergeHelper(t, tests, false)
	})
	t.Run("merge right to left", func(t *testing.T) {
		testDataMergeHelper(t, tests, true)
	})
}

func testDataMergeHelper(t *testing.T, tests []dataMergeTest, flipSides bool) {
	for _, test := range tests {
		if flipSides {
			for i, _ := range test.dataTests {
				tmp := test.dataTests[i].left
				test.dataTests[i].left = test.dataTests[i].right
				test.dataTests[i].right = tmp
			}
		}

		t.Run(test.name, func(t *testing.T) {
			for _, data := range test.dataTests {
				t.Run(data.name, func(t *testing.T) {
					if data.skip {
						t.Skip()
					}
					ctx := context.Background()
					a, l, r, m := setupDataMergeTest(ctx, t, test.schema, data)
					ns := a.NodeStore()

					var mo merge.MergeOpts
					var eo editor.Options
					eo = eo.WithDeaf(editor.NewInMemDeaf(a.VRW()))
					// attempt merge before skipping to assert no panics
					result, err := merge.MergeRoots(sql.NewContext(ctx), l, r, a, rootish{r}, rootish{a}, eo, mo)

					if data.dataConflict {
						// TODO: Test the conflict error message more deeply
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						verifyMerge(t, ctx, m, result, ns, data.dataConflict, data.constraintViolations)
					}
				})
			}
		})
	}
}

func setupDataMergeTest(ctx context.Context, t *testing.T, schema namedSchema, test dataTest) (anc, left, right, merged doltdb.RootValue) {
	denv := dtestutils.CreateTestEnv()
	var eo editor.Options
	eo = eo.WithDeaf(editor.NewInMemDeaf(denv.DoltDB(ctx).ValueReadWriter()))

	ancestorTable := tbl(schema, test.ancestor...)
	anc = makeRootWithTable(t, denv.DoltDB(ctx), eo, *ancestorTable)
	assert.NotNil(t, anc)

	leftTable := tbl(schema, test.left...)
	left = makeRootWithTable(t, denv.DoltDB(ctx), eo, *leftTable)
	assert.NotNil(t, left)

	rightTable := tbl(schema, test.right...)
	right = makeRootWithTable(t, denv.DoltDB(ctx), eo, *rightTable)
	assert.NotNil(t, right)

	mergedTable := tbl(schema, test.merged...)
	merged = makeRootWithTable(t, denv.DoltDB(ctx), eo, *mergedTable)
	assert.NotNil(t, merged)

	return anc, left, right, merged
}
