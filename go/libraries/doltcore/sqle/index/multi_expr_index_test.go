// Copyright 2026 Dolthub, Inc.
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

package index_test

import (
	"context"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

// findDoltIndex returns the DoltIndex with the given ID among indexes, or nil if not found.
func findDoltIndex(indexes []sql.Index, id string) index.DoltIndex {
	for _, i := range indexes {
		if i.ID() == id {
			return i.(index.DoltIndex)
		}
	}
	return nil
}

// TestDoltIndexMultipleFunctionalExpressions verifies that Dolt's real storage stack (secondary key
// building on write, and index-backed lookups on read) correctly supports a single index containing
// more than one functional expression mixed with a plain column reference.
func TestDoltIndexMultipleFunctionalExpressions(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	root, err = sqle.ExecuteSql(ctx, dEnv, `
CREATE TABLE multiexpr (
  pk BIGINT PRIMARY KEY,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT
);
CREATE INDEX idx1 ON multiexpr ((c1 * 10), c2, (c3 * 10));
INSERT INTO multiexpr VALUES
  (1, 1, 2, 3),
  (2, 4, 5, 6),
  (3, 1, 2, 30);
`)
	require.NoError(t, err)

	tbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: "multiexpr"})
	require.NoError(t, err)
	require.True(t, ok)

	indexes, err := index.DoltIndexesFromTable(ctx, "dolt", "multiexpr", tbl)
	require.NoError(t, err)

	idx := findDoltIndex(indexes, "idx1")
	require.NotNil(t, idx, "expected to find idx1 among the table's indexes")

	// idx1 has three key parts: (c1*10), c2, (c3*10). Confirm all three are present and query each
	// combination of values to confirm the secondary index was built correctly from the mix of
	// virtual (expression-backed) and stored (plain column) fields.
	sqlCtx := sql.NewEmptyContext()
	exprs := idx.Expressions()
	require.Len(t, exprs, 3)

	tests := []struct {
		name     string
		keys     []interface{}
		expected []sql.Row
	}{
		{
			// idx.Schema() (and thus pkSch used by RowIterForIndexLookup) includes the two hidden
			// generated columns backing (c1*10) and (c3*10) as trailing fields; they aren't
			// projected out at this low level, so expected rows carry two trailing nils.
			name:     "matches first row",
			keys:     []interface{}{int64(10), int64(2), int64(30)},
			expected: []sql.Row{{int64(1), int64(1), int64(2), int64(3), nil, nil}},
		},
		{
			name:     "matches second row",
			keys:     []interface{}{int64(40), int64(5), int64(60)},
			expected: []sql.Row{{int64(2), int64(4), int64(5), int64(6), nil, nil}},
		},
		{
			name:     "matches third row despite sharing (c1*10, c2) with the first",
			keys:     []interface{}{int64(10), int64(2), int64(300)},
			expected: []sql.Row{{int64(3), int64(1), int64(2), int64(30), nil, nil}},
		},
		{
			name:     "no match when only a prefix of the key parts matches",
			keys:     []interface{}{int64(10), int64(2), int64(999)},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := sql.NewMySQLIndexBuilder(sqlCtx, idx)
			for i, key := range tt.keys {
				builder = builder.Equals(sqlCtx, exprs[i], nil, key)
			}
			indexLookup, err := builder.Build(sqlCtx)
			require.NoError(t, err)

			pkSch, err := sqlutil.FromDoltSchema(sqlCtx, "", "multiexpr", idx.Schema())
			require.NoError(t, err)

			indexIter, err := index.RowIterForIndexLookup(sqlCtx, NoCacheTableable{tbl}, indexLookup, pkSch, nil)
			require.NoError(t, err)

			var readRows []sql.Row
			for {
				row, err := indexIter.Next(sqlCtx)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				readRows = append(readRows, row)
			}

			require.ElementsMatch(t, tt.expected, readRows)
		})
	}
}

// TestDoltIndexMultipleFunctionalExpressionsDropOnlyTargetedIndex verifies that dropping one
// multi-expression functional index removes only its own hidden columns from Dolt's schema and
// leaves a second, unrelated functional index on the same table fully intact and queryable.
func TestDoltIndexMultipleFunctionalExpressionsDropOnlyTargetedIndex(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	root, err = sqle.ExecuteSql(ctx, dEnv, `
CREATE TABLE multiexpr2 (
  pk BIGINT PRIMARY KEY,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT
);
CREATE INDEX idx2 ON multiexpr2 ((c2 * 100));
CREATE INDEX idx1 ON multiexpr2 ((c1 * 10), c2, (c3 * 10));
INSERT INTO multiexpr2 VALUES (1, 10, 20, 30);
`)
	require.NoError(t, err)

	root, err = sqle.ExecuteSql(ctx, dEnv, `DROP INDEX idx1 ON multiexpr2;`)
	require.NoError(t, err)

	tbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: "multiexpr2"})
	require.NoError(t, err)
	require.True(t, ok)

	sch, err := tbl.GetSchema(ctx)
	require.NoError(t, err)

	// idx1's two hidden columns must be gone; idx2's single hidden column must remain.
	var hiddenColNames []string
	err = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Virtual {
			hiddenColNames = append(hiddenColNames, col.Name)
		}
		return false, nil
	})
	require.NoError(t, err)
	require.Len(t, hiddenColNames, 1, "expected only idx2's hidden column to remain: %v", hiddenColNames)

	indexes, err := index.DoltIndexesFromTable(ctx, "dolt", "multiexpr2", tbl)
	require.NoError(t, err)

	idx2 := findDoltIndex(indexes, "idx2")
	require.NotNil(t, idx2, "expected idx2 to survive dropping idx1")
	require.Len(t, idx2.Expressions(), 1)
}
