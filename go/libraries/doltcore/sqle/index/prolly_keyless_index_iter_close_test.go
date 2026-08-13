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
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

// TestProllyKeylessIndexIterCloseStopsBackgroundGoroutine is a regression test for a goroutine leak in
// prollyKeylessIndexIter (prolly_index_iter.go). Its Close() used to be a no-op, so a caller that stops reading
// before exhausting the iterator -- e.g. Subquery.HasResultRow, which an EXISTS check uses and which only reads
// the first matching row -- left the background queueRows goroutine permanently blocked trying to send the
// remaining rows into an 8-row buffered channel (indexLookupBufSize) that nothing would ever drain again. This
// affects any keyless table (no PRIMARY KEY, which plain SQL allows) with a secondary index: every indexed
// lookup against one that finds more matches than the buffer holds and isn't read to completion leaked a
// goroutine holding open storage cursors.
func TestProllyKeylessIndexIterCloseStopsBackgroundGoroutine(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	// A keyless table with a secondary index and more rows sharing the same indexed value than
	// indexLookupBufSize (8), so the background goroutine is guaranteed to still have unsent rows
	// buffered/pending when the caller stops reading after the first one.
	root, err = sqle.ExecuteSql(ctx, dEnv, `
CREATE TABLE keyless_dup (a BIGINT, b BIGINT);
CREATE INDEX idx_b ON keyless_dup(b);
INSERT INTO keyless_dup VALUES
	(1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1), (7, 1), (8, 1), (9, 1), (10, 1),
	(11, 1), (12, 1), (13, 1), (14, 1), (15, 1), (16, 1), (17, 1), (18, 1), (19, 1), (20, 1);
`)
	require.NoError(t, err)

	dt, ok, err := root.GetTable(ctx, doltdb.TableName{Name: "keyless_dup"})
	require.NoError(t, err)
	require.True(t, ok)

	indexes, err := index.DoltIndexesFromTable(ctx, "dolt", "keyless_dup", dt)
	require.NoError(t, err)

	var idx index.DoltIndex
	for _, candidate := range indexes {
		if candidate.ID() == "idx_b" {
			idx = candidate.(index.DoltIndex)
		}
	}
	require.NotNil(t, idx, "expected to find idx_b")

	sqlCtx := sql.NewEmptyContext()
	pkSch, err := sqlutil.FromDoltSchema(sqlCtx, "", "keyless_dup", idx.Schema())
	require.NoError(t, err)
	require.True(t, sql.IsKeyless(pkSch.Schema), "test table must be keyless to exercise prollyKeylessIndexIter")

	exprs := idx.Expressions()
	indexLookup, err := sql.NewMySQLIndexBuilder(sqlCtx, idx).Equals(sqlCtx, exprs[0], nil, 1).Build(sqlCtx)
	require.NoError(t, err)

	rowIter, err := index.RowIterForIndexLookup(sqlCtx, NoCacheTableable{dt}, indexLookup, pkSch, nil)
	require.NoError(t, err)

	// Read exactly one row and stop, exactly like an EXISTS check does: Subquery.HasResultRow calls Close()
	// after the first row, regardless of how many more rows remain unread.
	_, err = rowIter.Next(sqlCtx)
	require.NoError(t, err)

	require.NoError(t, rowIter.Close(sqlCtx))

	stopped := index.WaitForKeylessIndexIterGoroutine(rowIter, 2*time.Second)
	require.True(t, stopped, "background queueRows goroutine did not stop after Close(); it leaked instead of being cancelled")
}
