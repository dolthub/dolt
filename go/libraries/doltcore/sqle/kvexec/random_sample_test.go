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

package kvexec

import (
	"context"
	"fmt"
	"io"
	"testing"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

func TestRandomSampleTopN(t *testing.T) {
	setup := []string{
		"create table xy (x int primary key, y int)",
		"insert into xy values (1, 10), (2, 20), (3, 30)",
		"create table cpk (x int, y int, primary key (x, y))",
		`insert into cpk values
(1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
(6, 60), (7, 70), (8, 80), (9, 90), (10, 100),
(11, 110), (12, 120), (13, 130), (14, 140), (15, 150),
(16, 160), (17, 170), (18, 180), (19, 190), (20, 200),
(21, 210), (22, 220), (23, 230), (24, 240), (25, 250),
(26, 260), (27, 270), (28, 280), (29, 290), (30, 300),
(31, 310), (32, 320), (33, 330), (34, 340), (35, 350),
(36, 360), (37, 370), (38, 380), (39, 390), (40, 400)`,
		"create table kl (x int, y int)",
		"insert into kl values (1, 10), (2, 20), (3, 30)",
		"create table empty_tbl (x int primary key, y int)",
	}
	engine, sqlCtx := newTestEngine(t, setup...)

	tests := []struct {
		name             string
		query            string
		expectSampleIter bool
		wantRows         int
	}{
		{
			"simple order by rand",
			"select * from xy order by rand() limit 1",
			true, 1,
		},
		{
			"composite pk within io bound",
			"select * from cpk order by rand() limit 2",
			true, 2,
		},
		{
			"reject cpk limit exceeding io bound",
			"select * from cpk order by rand() limit 3",
			false, 0,
		},
		{
			"reject limit exceeding small table io bound",
			"select * from xy order by rand() limit 2",
			false, 0,
		},
		{
			// Subtree counts do not track filter matches.
			"reject filter",
			"select * from xy where y > 10 order by rand() limit 1",
			false, 0,
		},
		{
			// Seeded RAND requires deterministic row order.
			"reject seeded",
			"select * from xy order by rand(42) limit 1",
			false, 0,
		},
		{
			// Keyless tables lack 1:1 ordinal row mapping.
			"reject keyless",
			"select * from kl order by rand() limit 1",
			false, 0,
		},
		{
			"reject empty",
			"select * from empty_tbl order by rand() limit 1",
			false, 0,
		},
		{
			"reject zero limit",
			"select * from xy order by rand() limit 0",
			false, 0,
		},
		{
			"reject calc found rows",
			"select sql_calc_found_rows * from xy order by rand() limit 1",
			false, 0,
		},
		{
			"reject multiple sort",
			"select * from xy order by rand(), x limit 1",
			false, 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topn := parseTopN(t, engine, sqlCtx, tt.query)
			iter, err := Builder{}.Build(sqlCtx, topn, nil)
			require.NoError(t, err)
			_, ok := iter.(*randomSampleIter)
			require.Equalf(t, tt.expectSampleIter, ok, "query: %s", tt.query)

			if tt.expectSampleIter {
				rows, err := sql.RowIterToRows(sqlCtx, iter)
				require.NoError(t, err)
				require.Len(t, rows, tt.wantRows)

				_, err = iter.Next(sqlCtx)
				require.Equal(t, io.EOF, err)
				require.NoError(t, iter.Close(sqlCtx))

				_, qIter, _, err := engine.Query(sqlCtx, tt.query)
				require.NoError(t, err)
				qRows, err := sql.RowIterToRows(sqlCtx, qIter)
				require.NoError(t, err)
				require.Len(t, qRows, tt.wantRows)
			}
		})
	}

	t.Run("reject non-empty input row", func(t *testing.T) {
		query := "select * from xy order by rand() limit 1"
		topn := parseTopN(t, engine, sqlCtx, query)
		iter, err := Builder{}.Build(sqlCtx, topn, sql.Row{1})
		require.NoError(t, err)
		require.Nil(t, iter)
	})
}

func TestRandomSampleUniformityOnGappedTable(t *testing.T) {
	setup := []string{
		"create table gapped (id int primary key, v varchar(10))",
		`insert into gapped values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (1000, 'gap_end')`,
	}
	engine, sqlCtx := newTestEngine(t, setup...)

	query := "select id from gapped order by rand() limit 1"
	topn := parseTopN(t, engine, sqlCtx, query)

	iter, err := Builder{}.Build(sqlCtx, topn, nil)
	require.NoError(t, err)
	_, ok := iter.(*randomSampleIter)
	require.True(t, ok)

	counts := make(map[int]int)
	const trials = 3000
	for i := 0; i < trials; i++ {
		_, rowIter, _, err := engine.Query(sqlCtx, query)
		require.NoError(t, err)
		rows, err := sql.RowIterToRows(sqlCtx, rowIter)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		id := rows[0][0].(int32)
		counts[int(id)]++
	}

	for _, id := range []int{1, 2, 3, 4, 5, 1000} {
		cnt := counts[id]
		require.GreaterOrEqualf(t, cnt, 300, "row %d: %d", id, cnt)
		require.LessOrEqualf(t, cnt, 700, "row %d: %d", id, cnt)
	}
}

func TestSampleBoundaries(t *testing.T) {
	ctx := sql.NewEmptyContext()
	require.Equal(t, int64(10922), maxMemorySampleLimit(ctx))

	// Context without session falls back to default 1024 limit.
	ctx.Session = nil
	require.Equal(t, int64(1024), maxMemorySampleLimit(ctx))

	// Dynamic sort_buffer_size scaling
	engine, sqlCtx := newTestEngine(t, "create table dummy (id int primary key)")
	_ = engine

	err := sqlCtx.SetSessionVariable(sqlCtx, "sort_buffer_size", uint64(32768))
	require.NoError(t, err)
	require.Equal(t, int64(1365), maxMemorySampleLimit(sqlCtx))

	err = sqlCtx.SetSessionVariable(sqlCtx, "sort_buffer_size", uint64(262144))
	require.NoError(t, err)
	require.Equal(t, int64(10922), maxMemorySampleLimit(sqlCtx))

	// I/O limit tests based on N / 20
	require.Equal(t, int64(1), maxIoSampleLimit(0))
	require.Equal(t, int64(1), maxIoSampleLimit(10))
	require.Equal(t, int64(1), maxIoSampleLimit(20))
	require.Equal(t, int64(2), maxIoSampleLimit(40))
	require.Equal(t, int64(5), maxIoSampleLimit(100))
	require.Equal(t, int64(50), maxIoSampleLimit(1000))
	require.Equal(t, int64(240000), maxIoSampleLimit(4800000))
}

func TestRandomSampleTopN_SequentialComparison(t *testing.T) {
	engine, sqlCtx := newTestEngine(t, newComparisonSetup(20, 50)...)
	topn := parseTopN(t, engine, sqlCtx, "select * from cmp_tbl order by rand() limit 1")
	fallbackBuilder := rowexec.NewBuilder(nil, sql.EngineOverrides{})

	sampleAllocs := testing.AllocsPerRun(10, func() {
		iter, err := Builder{}.Build(sqlCtx, topn, nil)
		require.NoError(t, err)
		for {
			if _, err = iter.Next(sqlCtx); err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
		require.NoError(t, iter.Close(sqlCtx))
	})

	scanAllocs := testing.AllocsPerRun(10, func() {
		iter, err := fallbackBuilder.Build(sqlCtx, topn, nil)
		require.NoError(t, err)
		for {
			if _, err = iter.Next(sqlCtx); err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
		require.NoError(t, iter.Close(sqlCtx))
	})

	// Sampling 1 row must use 10x fewer allocations than a scan.
	require.Less(t, sampleAllocs*10, scanAllocs)
}

func newComparisonSetup(batches, batchSize int) []string {
	setup := make([]string, 0, batches+1)
	setup = append(setup, "create table cmp_tbl (id int primary key, v varchar(20))")
	for i := 0; i < batches; i++ {
		vals := ""
		for j := 0; j < batchSize; j++ {
			id := i*batchSize + j
			if j > 0 {
				vals += ", "
			}
			vals += fmt.Sprintf("(%d, 'val-%d')", id, id)
		}
		setup = append(setup, "insert into cmp_tbl values "+vals)
	}
	return setup
}

func newTestEngine(tb testing.TB, setup ...string) (*gms.Engine, *sql.Context) {
	tb.Helper()
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	tb.Cleanup(func() { dEnv.Close() })

	db, err := sqle.NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(tb, err)

	engine, sqlCtx, err := sqle.NewTestEngine(dEnv, ctx, db)
	require.NoError(tb, err)

	err = sqlCtx.Session.SetSessionVariable(sqlCtx, sql.AutoCommitSessionVar, false)
	require.NoError(tb, err)

	for _, q := range setup {
		_, iter, _, err := engine.Query(sqlCtx, q)
		require.NoError(tb, err)
		_, err = sql.RowIterToRows(sqlCtx, iter)
		require.NoError(tb, err)
	}
	return engine, sqlCtx
}

func parseTopN(tb testing.TB, engine *gms.Engine, ctx *sql.Context, query string) *plan.TopN {
	tb.Helper()
	node, err := engine.AnalyzeQuery(ctx, query)
	require.NoError(tb, err)
	var ret *plan.TopN
	transform.InspectWithOpaque(ctx, node, func(_ *sql.Context, n sql.Node) bool {
		if topn, ok := n.(*plan.TopN); ok {
			ret = topn
			return false
		}
		return true
	})
	require.NotNil(tb, ret)
	return ret
}
