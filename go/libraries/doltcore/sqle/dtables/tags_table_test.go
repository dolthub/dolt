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

package dtables_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	gmsql "github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/store/datas"
)

// TestTagsTableIndexLookupIsLazy asserts that a WHERE tag_name = 'x' point
// lookup resolves only the matching tag, not all tags.
func TestTagsTableIndexLookupIsLazy(t *testing.T) {
	const numTags = 50

	ctx := context.Background()
	sqlCtx := gmsql.NewEmptyContext()

	dEnv := dtestutils.CreateTestEnv()
	t.Cleanup(func() { dEnv.DoltDB(ctx).Close() })

	db := dEnv.DoltDB(ctx)
	headCommit, err := db.ResolveCommitRef(ctx, ref.NewBranchRef(env.DefaultInitBranch))
	require.NoError(t, err)

	meta := datas.NewTagMeta("tester", "test@example.com", "")
	for i := 0; i < numTags; i++ {
		tagRef := ref.NewTagRef(fmt.Sprintf("tag-%04d", i))
		require.NoError(t, db.NewTagAtCommit(ctx, tagRef, headCommit, meta))
	}

	tbl := dtables.NewTagsTable(sqlCtx, "dolt_tags", db)

	indexes, err := tbl.(gmsql.IndexAddressable).GetIndexes(sqlCtx)
	require.NoError(t, err)
	require.Len(t, indexes, 1, "expected exactly one index on dolt_tags")

	idx := indexes[0]
	exprs := idx.Expressions()
	require.Len(t, exprs, 1, "expected one expression on the tag_name index")

	// Point lookup: WHERE tag_name = 'tag-0025'
	lookup, err := gmsql.NewMySQLIndexBuilder(idx).Equals(sqlCtx, exprs[0], gmstypes.Text, "tag-0025").Build(sqlCtx)
	require.NoError(t, err)

	drainTable := func(partItr gmsql.PartitionIter) {
		t.Helper()
		for {
			part, err := partItr.Next(sqlCtx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			rowItr, err := tbl.PartitionRows(sqlCtx, part)
			require.NoError(t, err)
			for {
				_, err := rowItr.Next(sqlCtx)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
			}
			require.NoError(t, rowItr.Close(sqlCtx))
		}
		require.NoError(t, partItr.Close(sqlCtx))
	}

	const runs = 20

	indexedAllocs := testing.AllocsPerRun(runs, func() {
		partItr, err := tbl.(gmsql.IndexedTable).LookupPartitions(sqlCtx, lookup)
		if err != nil {
			t.Fatal(err)
		}
		drainTable(partItr)
	})
	fullScanAllocs := testing.AllocsPerRun(runs, func() {
		partItr, err := tbl.Partitions(sqlCtx)
		if err != nil {
			t.Fatal(err)
		}
		drainTable(partItr)
	})
	allocRatio := fullScanAllocs / indexedAllocs
	t.Logf("indexed:   %.0f allocs/op", indexedAllocs)
	t.Logf("full-scan: %.0f allocs/op", fullScanAllocs)
	t.Logf("ratio:     %.1fx", allocRatio)

	// With lazy index resolution the ratio grows with numTags. A ratio below
	// numTags/10 indicates the index is not filtering lazily.
	const threshold = float64(numTags) / 10
	assert.Greater(t, allocRatio, threshold,
		"alloc ratio %.1fx below threshold %.1fx (indexed=%g full-scan=%g allocs/op)",
		allocRatio, threshold, indexedAllocs, fullScanAllocs)
}
