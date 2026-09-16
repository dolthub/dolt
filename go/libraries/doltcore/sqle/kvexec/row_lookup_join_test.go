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
	"testing"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// rowLookupJoinSetup is shared by the tests below.
//
// The joins under test need a left side that is not a KV source, so that the
// KV-to-KV lookup join cannot apply. A projection-only derived table does not
// qualify, because the analyzer inlines it back into a table scan; DISTINCT,
// LIMIT, an aggregation or a computed column all keep the subquery alias in the
// plan.
var rowLookupJoinSetup = []string{
	"create table xy (x int primary key, y int)",
	"insert into xy values (0,0), (1,1), (2,2), (3,null)",
	"create table ab (a int primary key, b int)",
	"insert into ab values (0,10), (1,11), (2,12)",
	"create table big (g bigint primary key, h int)",
	"insert into big values (0,20), (1,21), (2,22)",
	"create table sec (s int primary key, k int, p varchar(10), key k_idx(k))",
	"insert into sec values (1,100,'p1'), (2,100,'p2'), (3,200,'p3'), (4,null,'p4')",
	"create table keyless (k int, v varchar(10), key k_idx(k))",
	"insert into keyless values (0,'k0'), (0,'k1'), (1,'k2')",
	"create table str (s varchar(20) primary key, n int)",
	"insert into str values ('alpha',1), ('beta',2)",
	"create table comp (c1 int, c2 int, c3 int, primary key (c1,c2))",
	"insert into comp values (0,0,1), (0,1,2), (1,0,3)",
}

// TestRowLookupJoin ensures we trigger the row source lookup join operator for
// the expected query patterns.
func TestRowLookupJoin(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		doKvex bool
	}{
		{
			name:   "accept cte on left",
			query:  "with d as (select distinct x, y from xy) select /*+ LOOKUP_JOIN(d,ab) */ d.x, ab.b from d join ab on ab.a = d.x",
			doKvex: true,
		},
		{
			name:   "accept derived table on left",
			query:  "select /*+ LOOKUP_JOIN(d,ab) */ d.x, ab.b from (select x, y from xy limit 10) d join ab on ab.a = d.x",
			doKvex: true,
		},
		{
			name:   "accept aggregation on left",
			query:  "select /*+ LOOKUP_JOIN(d,ab) */ d.x, ab.b from (select x, sum(y) sy from xy group by x) d join ab on ab.a = d.x",
			doKvex: true,
		},
		{
			name:   "accept left outer join",
			query:  "select /*+ LOOKUP_JOIN(d,ab) */ d.x, ab.b from (select distinct x, y from xy) d left join ab on ab.a = d.x",
			doKvex: true,
		},
		{
			name:   "accept keyless right side",
			query:  "select /*+ LOOKUP_JOIN(d,keyless) */ d.x, keyless.v from (select distinct x, y from xy) d join keyless on keyless.k = d.x",
			doKvex: true,
		},
		{
			name:   "accept non-covering secondary index on right side",
			query:  "select /*+ LOOKUP_JOIN(d,sec) */ d.y, sec.p from (select x, y*100 y from xy) d join sec on sec.k = d.y",
			doKvex: true,
		},
		{
			name:   "accept index prefix lookup",
			query:  "select /*+ LOOKUP_JOIN(d,comp) */ d.x, comp.c3 from (select distinct x, y from xy) d join comp on comp.c1 = d.x",
			doKvex: true,
		},
		{
			name:   "accept key encodings the kv-to-kv path rejects",
			query:  "select /*+ LOOKUP_JOIN(d,big) */ d.x, big.h from (select distinct x, y from xy) d join big on big.g = d.x",
			doKvex: true,
		},
		{
			name:   "reject complex key expression",
			query:  "select /*+ LOOKUP_JOIN(d,ab) */ d.x, ab.b from (select distinct x, y from xy) d join ab on ab.a = d.x+1",
			doKvex: false,
		},
		{
			name:   "reject semi join",
			query:  "select /*+ LOOKUP_JOIN(d,ab) */ d.x from (select distinct x, y from xy) d where d.x in (select a from ab)",
			doKvex: false,
		},
		{
			name:   "reject kv-to-kv shape, which the other operator handles",
			query:  "select /*+ LOOKUP_JOIN(xy,ab) */ * from xy join ab on ab.a = xy.x",
			doKvex: false,
		},
	}

	engine, sqlCtx := setupRowLookupJoinEngine(t, rowLookupJoinSetup)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := buildJoinWithKvexec(t, engine, sqlCtx, tt.query)
			_, ok := iter.(*rowLookupJoinKvIter)
			require.Equalf(t, tt.doKvex, ok, "expected row lookup kvexec: %t, got %T", tt.doKvex, iter)
		})
	}
}

// TestRowLookupJoinResults runs queries the row source lookup join operator
// handles through both execution paths and requires the results to match.
func TestRowLookupJoinResults(t *testing.T) {
	queries := []struct {
		query string
		// multiJoin queries have more than one join, so we cannot assert which
		// operator a single join resolved to
		multiJoin bool
	}{
		// inner and outer joins against a unique index
		{query: "select /*+ LOOKUP_JOIN(d,ab) */ d.x, d.y, ab.a, ab.b from (select distinct x, y from xy) d join ab on ab.a = d.x order by d.x"},
		{query: "select /*+ LOOKUP_JOIN(d,ab) */ d.x, d.y, ab.a, ab.b from (select distinct x, y from xy) d left join ab on ab.a = d.x order by d.x"},
		// null keys never match a regular equality, and do match a null safe one
		{query: "select /*+ LOOKUP_JOIN(d,ab) */ d.x, ab.a from (select distinct x, y from xy) d join ab on ab.a = d.y order by d.x"},
		{query: "select /*+ LOOKUP_JOIN(d,ab) */ d.x, ab.a from (select distinct x, y from xy) d left join ab on ab.a = d.y order by d.x"},
		{query: "select /*+ LOOKUP_JOIN(d,sec) */ d.x, sec.s from (select distinct x, y from xy) d join sec on sec.k <=> d.y order by d.x, sec.s"},
		// keys outside the range of the index column match nothing
		{query: "select /*+ LOOKUP_JOIN(d,ab) */ d.k, ab.a from (select 1 k union all select -1 union all select 4294967296) d left join ab on ab.a = d.k order by d.k"},
		// key encodings the kv-to-kv path rejects
		{query: "select /*+ LOOKUP_JOIN(d,big) */ d.x, big.h from (select distinct x, y from xy) d left join big on big.g = d.x order by d.x"},
		// several right rows per left row
		{query: "select /*+ LOOKUP_JOIN(d,sec) */ d.k, sec.s, sec.p from (select 100 k union all select 200 union all select 300) d join sec on sec.k = d.k order by d.k, sec.s"},
		{query: "select /*+ LOOKUP_JOIN(d,keyless) */ d.x, keyless.v from (select distinct x, y from xy) d join keyless on keyless.k = d.x order by d.x, keyless.v"},
		{query: "select /*+ LOOKUP_JOIN(d,keyless) */ d.x, keyless.v from (select distinct x, y from xy) d left join keyless on keyless.k = d.x order by d.x, keyless.v"},
		// index prefix lookups
		{query: "select /*+ LOOKUP_JOIN(d,comp) */ d.x, comp.c2, comp.c3 from (select distinct x, y from xy) d join comp on comp.c1 = d.x order by d.x, comp.c2"},
		// filters on the right side and on the join condition
		{query: "select /*+ LOOKUP_JOIN(d,ab) */ d.x, ab.b from (select distinct x, y from xy) d join ab on ab.a = d.x and ab.b > 10 order by d.x"},
		{query: "select /*+ LOOKUP_JOIN(d,ab) */ d.x, ab.b from (select distinct x, y from xy) d left join ab on ab.a = d.x and ab.b > 10 order by d.x"},
		{query: "select /*+ LOOKUP_JOIN(d,sec) */ d.k, sec.s from (select 100 k union all select 200) d join sec on sec.k = d.k and sec.p <> 'p1' order by d.k, sec.s"},
		// string keys
		{query: "select /*+ LOOKUP_JOIN(d,str) */ d.s, str.n from (select 'alpha' s union all select 'beta' union all select 'gamma') d left join str on str.s = d.s order by d.s"},
		// aggregation feeding the join
		{query: "select /*+ LOOKUP_JOIN(d,ab) */ d.x, d.sy, ab.b from (select x, sum(y) sy from xy group by x) d join ab on ab.a = d.x order by d.x"},
		// a join feeding a join, the shape from dolthub/dolt#11636
		{query: "with i as (select x, sum(y) sy from xy group by x), m as (select i.x, ab.b from i join ab on ab.a = i.x) select m.x, m.b, sec.p from m left join sec on sec.s = m.x order by m.x", multiJoin: true},
		// early termination
		{query: "select /*+ LOOKUP_JOIN(d,sec) */ d.k, sec.s from (select 100 k union all select 200) d join sec on sec.k = d.k order by d.k, sec.s limit 2"},
	}

	engine, sqlCtx := setupRowLookupJoinEngine(t, rowLookupJoinSetup)
	analyzer := engine.EngineAnalyzer()

	gmsBuilder := rowexec.NewBuilder(nil, sql.EngineOverrides{})
	gmsBuilder.Runner = analyzer.Runner
	kvBuilder := NewExecBuilder(sql.EngineOverrides{})
	kvBuilder.Runner = analyzer.Runner

	for _, tt := range queries {
		t.Run(tt.query, func(t *testing.T) {
			if !tt.multiJoin {
				iter := buildJoinWithKvexec(t, engine, sqlCtx, tt.query)
				require.IsTypef(t, &rowLookupJoinKvIter{}, iter, "query does not exercise the operator under test, got %T", iter)
			}

			// analyze separately for each builder, plan nodes can hold
			// execution state
			gmsIter, err := gmsBuilder.Build(sqlCtx, analyzeQuery(t, engine, sqlCtx, tt.query), nil)
			require.NoError(t, err)
			expected, err := sql.RowIterToRows(sqlCtx, gmsIter)
			require.NoError(t, err)

			kvIter, err := kvBuilder.Build(sqlCtx, analyzeQuery(t, engine, sqlCtx, tt.query), nil)
			require.NoError(t, err)
			actual, err := sql.RowIterToRows(sqlCtx, kvIter)
			require.NoError(t, err)

			require.Equal(t, expected, actual)
		})
	}
}

// buildJoinWithKvexec analyzes |query| and returns the iterator kvexec builds
// for its join, or nil when kvexec declines the join.
func buildJoinWithKvexec(t *testing.T, engine *gms.Engine, sqlCtx *sql.Context, query string) sql.RowIter {
	t.Helper()
	j := getJoin(sqlCtx, analyzeQuery(t, engine, sqlCtx, query))
	require.NotNil(t, j)

	kvb := &Builder{}
	kvb.SetFallbackBuilder(rowexec.NewBuilder(kvb, sql.EngineOverrides{}))
	iter, err := kvb.Build(sqlCtx, j, nil)
	require.NoError(t, err)
	if iter != nil {
		require.NoError(t, iter.Close(sqlCtx))
	}
	return iter
}

func setupRowLookupJoinEngine(t *testing.T, setup []string) (*gms.Engine, *sql.Context) {
	t.Helper()
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	t.Cleanup(func() { dEnv.Close() })

	db, err := sqle.NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)

	engine, sqlCtx, err := sqle.NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)

	err = sqlCtx.Session.SetSessionVariable(sqlCtx, sql.AutoCommitSessionVar, false)
	require.NoError(t, err)

	for _, q := range setup {
		_, iter, _, err := engine.Query(sqlCtx, q)
		require.NoError(t, err)
		_, err = sql.RowIterToRows(sqlCtx, iter)
		require.NoError(t, err)
	}
	return engine, sqlCtx
}

func analyzeQuery(t *testing.T, engine *gms.Engine, sqlCtx *sql.Context, query string) sql.Node {
	t.Helper()
	binder := planbuilder.New(sqlCtx, engine.EngineAnalyzer().Catalog, engine.EventScheduler)
	node, _, _, qFlags, err := binder.Parse(query, nil, false)
	require.NoError(t, err)
	node, err = engine.EngineAnalyzer().Analyze(sqlCtx, node, nil, qFlags)
	require.NoError(t, err)
	return node
}
