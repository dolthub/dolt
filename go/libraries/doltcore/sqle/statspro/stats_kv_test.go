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
	"strconv"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

func TestProllyKv(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	prollyKv := newTestProllyKv(t, threads)

	h := hash.Parse(strings.Repeat("a", hash.StringLen))
	h2 := hash.Parse(strings.Repeat("b", hash.StringLen))
	k := getBucketKey(h, 2)

	tupB := val.NewTupleBuilder(val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.StringEnc, Nullable: true},
	))

	t.Run("test bounds", func(t *testing.T) {
		exp := sql.Row{1, 1}
		prollyKv.PutBound(h, exp, 2)
		cmp, ok := prollyKv.GetBound(h, 2)
		require.True(t, ok)
		require.Equal(t, exp, cmp)

		_, ok = prollyKv.GetBound(h2, 2)
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
		delete(prollyKv.mem.buckets, k)

		cmp, ok, err = prollyKv.GetBucket(context.Background(), h, tupB)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, exp, cmp)

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

	t.Run("test bucket GC", func(t *testing.T) {
		exp := stats.NewHistogramBucket(15, 7, 3, 4, sql.Row{int64(1), "one"}, []uint64{5, 4, 3, 1}, []sql.Row{{int64(5), "six"}, {int64(4), "three"}, {int64(3), "seven"}, {int64(1), "one"}}).(*stats.Bucket)
		err := prollyKv.PutBucket(context.Background(), h, exp, tupB)
		require.NoError(t, err)

		exp2 := stats.NewHistogramBucket(10, 7, 3, 4, sql.Row{int64(1), "one"}, []uint64{5, 4, 3, 1}, []sql.Row{{int64(5), "six"}, {int64(4), "three"}, {int64(3), "seven"}, {int64(1), "one"}}).(*stats.Bucket)
		err = prollyKv.PutBucket(context.Background(), h2, exp2, tupB)
		require.NoError(t, err)

		prollyKv.StartGc(context.Background(), 10)
		err = prollyKv.MarkBucket(context.Background(), h, tupB)
		require.NoError(t, err)
		err = prollyKv.MarkBucket(context.Background(), h2, tupB)
		require.NoError(t, err)

		prollyKv.FinishGc(nil)

		m, _ := prollyKv.m.Map(context.Background())
		iter, _ := m.IterAll(context.Background())
		for i := range 2 {
			k, _, err := iter.Next(context.Background())
			if i == 0 {
				require.Equal(t, "( 2, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa )", prollyKv.kb.Desc.Format(k))
			} else if i == 1 {
				require.Equal(t, "( 2, bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb )", prollyKv.kb.Desc.Format(k))
			} else if i == 2 {
				require.Error(t, err)
			}
		}

		prollyKv.StartGc(context.Background(), 10)
		err = prollyKv.MarkBucket(context.Background(), h2, tupB)
		require.NoError(t, err)
		prollyKv.FinishGc(nil)

		cmp2, ok, err := prollyKv.GetBucket(context.Background(), h2, tupB)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, exp2.BoundCount(), cmp2.BoundCnt)
		// only tagged one bucket
		require.Equal(t, 1, prollyKv.Len())
	})

	t.Run("test overflow", func(t *testing.T) {
		prollyKv.StartGc(context.Background(), 10)
		prollyKv.FinishGc(nil)

		expLen := 2000
		var expected []hash.Hash
		for i := range expLen {
			exp := stats.NewHistogramBucket(uint64(i), 7, 3, 4, sql.Row{int64(1), "one"}, []uint64{5, 4, 3, 1}, []sql.Row{{int64(5), "six"}, {int64(4), "three"}, {int64(3), "seven"}, {int64(1), "one"}}).(*stats.Bucket)
			nh := strconv.AppendInt(nil, int64(i), 10)
			nh = append(nh, h[:hash.ByteLen-len(nh)]...)
			newH := hash.New(nh)
			expected = append(expected, newH)
			err := prollyKv.PutBucket(context.Background(), newH, exp, tupB)
			require.NoError(t, err)
		}

		for _, h := range expected {
			_, ok, err := prollyKv.GetBucket(context.Background(), h, tupB)
			require.NoError(t, err)
			require.True(t, ok)
		}

		require.Equal(t, expLen, prollyKv.Len())
	})

	t.Run("test bounds GC", func(t *testing.T) {
		exp := sql.Row{1, 1}
		prollyKv.PutBound(h, exp, 2)
		prollyKv.PutBound(h2, exp, 2)

		prollyKv.StartGc(context.Background(), 10)
		prollyKv.GetBound(h2, 2)
		prollyKv.FinishGc(nil)

		require.Equal(t, 1, len(prollyKv.mem.bounds))
	})

	t.Run("test templates GC", func(t *testing.T) {
		exp := stats.Statistic{RowCnt: 50, Qual: sql.StatQualifier{Database: "mydb", Tab: "xy"}}
		key := templateCacheKey{
			h:       h,
			idxName: "PRIMARY",
		}
		key2 := templateCacheKey{
			h:       h2,
			idxName: "PRIMARY",
		}
		prollyKv.PutTemplate(key, exp)
		prollyKv.PutTemplate(key2, exp)

		prollyKv.StartGc(context.Background(), 10)
		prollyKv.GetTemplate(key2)
		prollyKv.FinishGc(nil)

		require.Equal(t, 1, len(prollyKv.mem.templates))
	})

}

func newTestProllyKv(t *testing.T, threads *sql.BackgroundThreads) *prollyStats {
	dEnv := dtestutils.CreateTestEnv()

	sqlEng, ctx := newTestEngine(context.Background(), dEnv, threads)
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
