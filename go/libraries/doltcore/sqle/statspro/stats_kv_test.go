// Copyright 2025 Dolthub, Inc.
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

package statspro

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestProllyKv(t *testing.T) {
	prollyKv := newTestProllyKv(t)

	h := hash.Parse(strings.Repeat("a", hash.StringLen))
	h2 := hash.Parse(strings.Repeat("b", hash.StringLen))

	tupB := val.NewTupleBuilder(val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.StringEnc, Nullable: true},
	))

	t.Run("test bounds", func(t *testing.T) {
		exp := sql.Row{1, 1}
		prollyKv.PutBound(h, exp)
		cmp, ok := prollyKv.GetBound(h)
		require.True(t, ok)
		require.Equal(t, exp, cmp)

		_, ok = prollyKv.GetBound(h2)
		require.False(t, ok)
	})

	t.Run("test templates", func(t *testing.T) {
		exp := stats.Statistic{RowCnt: 50, Qual: sql.StatQualifier{Database: "mydb", Tab: "xy"}}
		key := templateCacheKey{
			h:       h,
			idxName: "PRIMARY",
		}
		prollyKv.PutTemplate(key, exp)
		cmp, ok := prollyKv.GetTemplate(key)
		require.True(t, ok)
		require.Equal(t, exp, cmp)

		key2 := templateCacheKey{
			h:       h2,
			idxName: "PRIMARY",
		}
		_, ok = prollyKv.GetTemplate(key2)
		require.False(t, ok)
	})

	t.Run("test buckets", func(t *testing.T) {
		exp := stats.NewHistogramBucket(15, 7, 3, 4, sql.Row{int64(1), "one"}, []uint64{5, 4, 3, 1}, []sql.Row{{int64(5), "six"}, {int64(4), "three"}, {int64(3), "seven"}, {int64(1), "one"}}).(*stats.Bucket)
		err := prollyKv.PutBucket(context.Background(), h, exp, tupB)
		require.NoError(t, err)
		cmp, ok, err := prollyKv.GetBucket(context.Background(), h, tupB)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, exp, cmp)

		_, ok, err = prollyKv.GetBucket(context.Background(), h2, tupB)
		require.NoError(t, err)
		require.False(t, ok)

		// delete from memory, should pull from disk when |tupB| supplied
		prollyKv.mem.buckets.Remove(h)

		cmp, ok, err = prollyKv.GetBucket(context.Background(), h, nil)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, (*stats.Bucket)(nil), cmp)

		cmp, ok, err = prollyKv.GetBucket(context.Background(), h, tupB)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, exp.RowCnt, cmp.RowCnt)
		require.Equal(t, exp.DistinctCnt, cmp.DistinctCnt)
		require.Equal(t, exp.NullCnt, cmp.NullCnt)
		require.Equal(t, exp.McvsCnt, cmp.McvsCnt)
		require.Equal(t, exp.McvVals[0], cmp.McvVals[0])
		require.Equal(t, exp.McvVals[1], cmp.McvVals[1])
		require.Equal(t, exp.McvVals[2], cmp.McvVals[2])
		require.Equal(t, exp.McvVals[3], cmp.McvVals[3])
		require.Equal(t, exp.BoundVal, cmp.BoundVal)
		require.Equal(t, exp.BoundCnt, cmp.BoundCnt)
	})

	t.Run("test GC", func(t *testing.T) {
		prollyKv.StartGc(context.Background(), 10)

		// if we delete from memory, no more fallback to disk
		prollyKv.mem.buckets.Remove(h)
		_, ok, err := prollyKv.GetBucket(context.Background(), h2, tupB)
		require.NoError(t, err)
		require.False(t, ok)

		exp := stats.NewHistogramBucket(15, 7, 3, 4, sql.Row{int64(1), "one"}, []uint64{5, 4, 3, 1}, []sql.Row{{int64(5), "six"}, {int64(4), "three"}, {int64(3), "seven"}, {int64(1), "one"}}).(*stats.Bucket)
		err = prollyKv.PutBucket(context.Background(), h, exp, tupB)
		require.NoError(t, err)

		exp2 := stats.NewHistogramBucket(10, 7, 3, 4, sql.Row{int64(1), "one"}, []uint64{5, 4, 3, 1}, []sql.Row{{int64(5), "six"}, {int64(4), "three"}, {int64(3), "seven"}, {int64(1), "one"}}).(*stats.Bucket)
		err = prollyKv.PutBucket(context.Background(), h2, exp2, tupB)
		require.NoError(t, err)

		prollyKv.FinishGc()

		prollyKv.StartGc(context.Background(), 10)
		cmp2, ok, err := prollyKv.GetBucket(context.Background(), h2, tupB)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, exp2.BoundCount(), cmp2.BoundCnt)
		prollyKv.FinishGc()
		// only tagged one bucket
		require.Equal(t, 1, prollyKv.Len())

	})
}

func newTestProllyKv(t *testing.T) *prollyStats {
	dEnv := dtestutils.CreateTestEnv()
	sqlEng, ctx := newTestEngine(context.Background(), dEnv)
	ctx.Session.SetClient(sql.Client{
		User:    "billy boy",
		Address: "bigbillie@fake.horse",
	})
	require.NoError(t, executeQuery(ctx, sqlEng, "create database mydb"))
	require.NoError(t, executeQuery(ctx, sqlEng, "use mydb"))

	startDbs := sqlEng.Analyzer.Catalog.DbProvider.AllDatabases(ctx)

	kv, err := NewProllyStats(ctx, startDbs[0].(dsess.SqlDatabase))
	require.NoError(t, err)

	return kv
}
