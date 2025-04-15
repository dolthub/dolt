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
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro/jobqueue"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
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
	), nil)

	t.Run("TestBoundsRoundTrip", func(t *testing.T) {
		exp := sql.Row{1, 1}
		prollyKv.PutBound(h, exp, 2)
		cmp, ok := prollyKv.GetBound(h, 2)
		require.True(t, ok)
		require.Equal(t, exp, cmp)

		_, ok = prollyKv.GetBound(h2, 2)
		require.False(t, ok)
	})

	t.Run("TestTemplatesRoundTrip", func(t *testing.T) {
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
	t.Run("TestBucketsRoundTrip", func(t *testing.T) {
		exp := stats.NewHistogramBucket(15, 7, 3, 4, sql.Row{int64(1), "one"}, []uint64{5, 4, 3, 1}, []sql.Row{{int64(5), "six"}, {int64(4), "three"}, {int64(3), "seven"}, {int64(1), "one"}}).(*stats.Bucket)
		err := prollyKv.PutBucket(context.Background(), h, exp, tupB)
		require.NoError(t, err)
		cmp, ok, err := prollyKv.GetBucket(context.Background(), h, tupB)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, exp, cmp)

		// delete from memory, should pull from disk when |tupB| supplied
		delete(prollyKv.mem.buckets, k)

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
	t.Run("TestNilMcvsRoundTrip", func(t *testing.T) {
		exp := stats.NewHistogramBucket(15, 7, 3, 4, sql.Row{int64(1), "one"}, []uint64{5, 4}, []sql.Row{{int64(5), "six"}, {int64(4), "three"}}).(*stats.Bucket)
		err := prollyKv.PutBucket(context.Background(), h, exp, tupB)

		delete(prollyKv.mem.buckets, k)

		cmp, ok, err := prollyKv.GetBucket(context.Background(), h, tupB)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, exp.RowCnt, cmp.RowCnt)
		require.Equal(t, exp.DistinctCnt, cmp.DistinctCnt)
		require.Equal(t, exp.NullCnt, cmp.NullCnt)
		require.Equal(t, exp.McvsCnt, cmp.McvsCnt)
		require.Equal(t, len(exp.McvVals), len(cmp.McvVals))
		require.Equal(t, exp.McvVals[0], cmp.McvVals[0])
		require.Equal(t, exp.McvVals[1], cmp.McvVals[1])
		require.Equal(t, exp.BoundVal, cmp.BoundVal)
		require.Equal(t, exp.BoundCnt, cmp.BoundCnt)
	})
	t.Run("TestGcGenBlocking", func(t *testing.T) {
		to := NewMemStats()
		from := NewMemStats()
		from.gcGen = 1
		require.False(t, to.GcMark(from, nil, nil, 0, nil))
	})
	t.Run("TestGcMarkFlush", func(t *testing.T) {
		ctx := context.Background()
		bthreads := sql.NewBackgroundThreads()
		defer bthreads.Shutdown()
		prev := NewMemStats()
		nodes1, bucks1 := testNodes(t, 10, 1)
		nodes2, bucks2 := testNodes(t, 10, 2)
		nodes3, bucks3 := testNodes(t, 10, 3)
		for i := range nodes1 {
			require.NoError(t, prev.PutBucket(ctx, nodes1[i].HashOf(), bucks1[i], tupB))
		}
		for i := range nodes2 {
			require.NoError(t, prev.PutBucket(ctx, nodes2[i].HashOf(), bucks2[i], tupB))
		}
		for i := range nodes3 {
			require.NoError(t, prev.PutBucket(ctx, nodes3[i].HashOf(), bucks3[i], tupB))
		}

		require.Equal(t, 30, prev.Len())

		to := NewMemStats()
		require.True(t, to.GcMark(prev, nodes1, bucks1, 2, tupB))
		require.True(t, to.GcMark(prev, nodes2, bucks2, 2, tupB))

		require.Equal(t, 1, len(to.gcFlusher))
		require.Equal(t, 20, len(to.gcFlusher[tupB]))
		require.Equal(t, 20, to.Len())

		sq := jobqueue.NewSerialQueue()
		sq.Run(ctx)
		defer sq.Stop()

		kv := newTestProllyKv(t, bthreads)
		kv.mem = to
		cnt, err := kv.Flush(ctx, sq)
		require.NoError(t, err)
		require.Equal(t, 20, cnt)
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

func testNodes(t *testing.T, cnt int, seed uint8) ([]tree.Node, []*stats.Bucket) {
	ts := &chunks.TestStorage{}
	ns := tree.NewNodeStore(ts.NewViewWithFormat(types.Format_DOLT.VersionString()))
	s := message.NewBlobSerializer(ns.Pool())

	var nodes []tree.Node
	var buckets []*stats.Bucket
	for i := range cnt {
		vals := [][]byte{{uint8(i), seed, 1, 1}}
		msg := s.Serialize([][]byte{{0}}, vals, []uint64{1}, 0)
		node, _, err := tree.NodeFromBytes(msg)
		require.NoError(t, err)
		nodes = append(nodes, node)
		buckets = append(buckets, &stats.Bucket{RowCnt: uint64(i), BoundVal: sql.Row{i, "col2"}})
	}
	return nodes, buckets
}
