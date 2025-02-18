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
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
)

func TestScheduleLoop(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)

	{
		// add more data
		b := strings.Repeat("b", 100)
		require.NoError(t, executeQuery(ctx, sqlEng, "create table ab (a int primary key, b varchar(100), key (b,a))"))
		abIns := strings.Builder{}
		abIns.WriteString("insert into ab values")
		for i := range 200 {
			if i > 0 {
				abIns.WriteString(", ")
			}
			abIns.WriteString(fmt.Sprintf("(%d, '%s')", i, b))
		}
		require.NoError(t, executeQuery(ctx, sqlEng, abIns.String()))

		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_restart()"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_stop()"))

		// 4 old + 2*7 new ab
		kv := sc.kv.(*memStats)
		require.Equal(t, 18, len(kv.buckets))
		require.Equal(t, 4, len(kv.bounds))
		require.Equal(t, 4, len(kv.templates))
		require.Equal(t, 2, len(sc.Stats.stats))
		stat := sc.Stats.stats[tableIndexesKey{"mydb", "main", "ab", ""}]
		require.Equal(t, 7, len(stat[0].Hist))
		require.Equal(t, 7, len(stat[1].Hist))
	}

	require.NoError(t, executeQuery(ctx, sqlEng, "drop table xy"))

	//doGcCycle(t, ctx, sc)
	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_restart()"))
	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_gc()"))
	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))
	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_stop()"))

	kv := sc.kv.(*memStats)
	require.Equal(t, 14, len(kv.buckets))
	require.Equal(t, 2, len(kv.bounds))
	require.Equal(t, 2, len(kv.templates))
	require.Equal(t, 1, len(sc.Stats.stats))
	stat := sc.Stats.stats[tableIndexesKey{"mydb", "main", "ab", ""}]
	require.Equal(t, 2, len(stat))
	require.Equal(t, 7, len(stat[0].Hist))
	require.Equal(t, 7, len(stat[1].Hist))
}

func TestAnalyze(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)

	require.NoError(t, executeQuery(ctx, sqlEng, "insert into xy values (-1,-1)"))

	//require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_restart()"))
	require.NoError(t, executeQuery(ctx, sqlEng, "analyze table xy"))
	//require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))
	//require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_stop()"))

	si, err := sc.Info(ctx)
	require.NoError(t, err)
	kv := sc.kv.(*memStats)
	require.Equal(t, 0, si.GcCnt)
	require.Equal(t, 1, si.DbCnt)
	require.Equal(t, false, si.Active)
	require.Equal(t, 6, len(kv.buckets))
	require.Equal(t, 4, len(kv.bounds))
	require.Equal(t, 2, len(kv.templates))
	require.Equal(t, 1, len(sc.Stats.stats))
	for _, tableStats := range sc.Stats.stats {
		require.Equal(t, 2, len(tableStats))
	}
}

func TestModifyColumn(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)
	sc.enableGc = false
	{
		runBlock(t, ctx, sqlEng, "alter table xy modify column y bigint")

		kv := sc.kv.(*memStats)
		require.Equal(t, 10, len(kv.buckets))
		require.Equal(t, 4, len(kv.bounds))
		require.Equal(t, 4, len(kv.templates))
		require.Equal(t, 1, len(sc.Stats.stats))
		stat := sc.Stats.stats[tableIndexesKey{"mydb", "main", "xy", ""}]
		require.Equal(t, 4, len(stat[0].Hist))
		require.Equal(t, 2, len(stat[1].Hist))

		runBlock(t, ctx, sqlEng, "call dolt_stats_gc()")
		require.Equal(t, 6, sc.Len())
	}
}

func TestAddColumn(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)
	sc.enableGc = false

	runBlock(t, ctx, sqlEng,
		"alter table xy add column z int",
	)

	kv := sc.kv.(*memStats)
	require.Equal(t, 4, len(kv.buckets))
	require.Equal(t, 2, len(kv.bounds))
	require.Equal(t, 4, len(kv.templates)) // +2 for new schema
	require.Equal(t, 1, len(sc.Stats.stats))
	stat := sc.Stats.stats[tableIndexesKey{"mydb", "main", "xy", ""}]
	require.Equal(t, 2, len(stat[0].Hist))
	require.Equal(t, 2, len(stat[1].Hist))
}

func TestDropIndex(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)
	sc.enableGc = false

	runBlock(t, ctx, sqlEng,
		"alter table xy drop index y",
	)

	kv := sc.kv.(*memStats)
	require.Equal(t, 4, len(kv.buckets))
	require.Equal(t, 2, len(kv.bounds))
	require.Equal(t, 3, len(kv.templates))
	require.Equal(t, 1, len(sc.Stats.stats))
	stat := sc.Stats.stats[tableIndexesKey{"mydb", "main", "xy", ""}]
	require.Equal(t, 1, len(stat))
	require.Equal(t, 2, len(stat[0].Hist))

	runBlock(t, ctx, sqlEng, "call dolt_stats_gc()")

	kv = sc.kv.(*memStats)
	require.Equal(t, 2, len(kv.buckets))
	require.Equal(t, 1, len(kv.bounds))
	require.Equal(t, 1, len(kv.templates))
	require.Equal(t, 1, len(sc.Stats.stats))
	stat = sc.Stats.stats[tableIndexesKey{"mydb", "main", "xy", ""}]
	require.Equal(t, 1, len(stat))
	require.Equal(t, 2, len(stat[0].Hist))
}

func TestDropTable(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)
	sc.enableGc = false

	runBlock(t, ctx, sqlEng,
		"create table ab (a int primary key, b int)",
		"insert into ab values (0,0)",
		"drop table xy",
	)

	kv := sc.kv.(*memStats)
	require.Equal(t, 5, len(kv.buckets))
	require.Equal(t, 3, len(kv.bounds))
	require.Equal(t, 3, len(kv.templates))
	require.Equal(t, 1, len(sc.Stats.stats))
	stat := sc.Stats.stats[tableIndexesKey{"mydb", "main", "ab", ""}]
	require.Equal(t, 1, len(stat))
	require.Equal(t, 1, len(stat[0].Hist))

	runBlock(t, ctx, sqlEng, "call dolt_stats_gc()")

	kv = sc.kv.(*memStats)
	require.Equal(t, 1, len(kv.buckets))
	require.Equal(t, 1, len(kv.bounds))
	require.Equal(t, 1, len(kv.templates))
	require.Equal(t, 1, len(sc.Stats.stats))
	stat = sc.Stats.stats[tableIndexesKey{"mydb", "main", "ab", ""}]
	require.Equal(t, 1, len(stat))
	require.Equal(t, 1, len(stat[0].Hist))
}

func TestDeleteAboveBoundary(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)
	sc.enableGc = false

	runBlock(t, ctx, sqlEng,
		"alter table xy drop index y",
		"delete from xy where x > 498",
		"call dolt_stats_wait()",
	)

	kv := sc.kv.(*memStats)
	require.Equal(t, 5, len(kv.buckets)) // 1 for new chunk
	require.Equal(t, 2, len(kv.bounds))
	require.Equal(t, 3, len(kv.templates)) // +1 for schema change
	require.Equal(t, 1, len(sc.Stats.stats))
	stat := sc.Stats.stats[tableIndexesKey{db: "mydb", branch: "main", table: "xy"}]
	require.Equal(t, 2, len(stat[0].Hist))

	runBlock(t, ctx, sqlEng, "call dolt_stats_gc()")

	require.Equal(t, 2, sc.Len())
}

func TestDeleteBelowBoundary(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)
	sc.enableGc = false

	runBlock(t, ctx, sqlEng,
		"alter table xy drop index y",
		"delete from xy where x > 410",
		"call dolt_stats_wait()",
	)

	kv := sc.kv.(*memStats)

	require.Equal(t, 5, len(kv.buckets)) // +1 rewrite partial chunk
	require.Equal(t, 3, len(kv.bounds))  // +1 rewrite first chunk
	require.Equal(t, 3, len(kv.templates))
	require.Equal(t, 1, len(sc.Stats.stats))
	stat := sc.Stats.stats[tableIndexesKey{db: "mydb", branch: "main", table: "xy"}]
	require.Equal(t, 1, len(stat[0].Hist))

	runBlock(t, ctx, sqlEng, "call dolt_stats_gc()")

	require.Equal(t, 1, sc.Len())

}

func TestDeleteOnBoundary(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)
	sc.enableGc = false

	runBlock(t, ctx, sqlEng,
		"alter table xy drop index y",
		// PRIMARY boundary chunk -> rewrite y_idx's second
		"delete from xy where x > 414",
	)

	kv := sc.kv.(*memStats)
	require.Equal(t, 4, len(kv.buckets))
	require.Equal(t, 2, len(kv.bounds))
	require.Equal(t, 3, len(kv.templates)) // +1 schema change
	require.Equal(t, 1, len(sc.Stats.stats))
	stat := sc.Stats.stats[tableIndexesKey{db: "mydb", branch: "main", table: "xy"}]
	require.Equal(t, 1, len(stat[0].Hist))

	runBlock(t, ctx, sqlEng, "call dolt_stats_gc()")

	require.Equal(t, 1, sc.Len())
}

func TestAddDropDatabases(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)
	sc.enableGc = false

	{
		runBlock(t, ctx, sqlEng,
			"create database otherdb",
			"use otherdb",
			"create table t (i int primary key)",
			"insert into t values (0), (1)",
			"call dolt_stats_wait()",
		)

		// xy and t
		kv := sc.kv.(*memStats)
		require.Equal(t, 5, len(kv.buckets))
		require.Equal(t, 3, len(kv.bounds))
		require.Equal(t, 3, len(kv.templates))
		require.Equal(t, 2, len(sc.Stats.stats))
		stat := sc.Stats.stats[tableIndexesKey{db: "otherdb", branch: "main", table: "t"}]
		require.Equal(t, 1, len(stat))
	}

	{
		runBlock(t, ctx, sqlEng, "drop database otherdb")
		_, ok := sc.Stats.stats[tableIndexesKey{db: "otherdb", branch: "main", table: "t"}]
		require.False(t, ok)
	}
}

func TestGC(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)

	{
		runBlock(t, ctx, sqlEng,
			"create database otherdb",
			"use otherdb",
			"create table t (i int primary key)",
			"insert into t values (0), (1)",

			"create database thirddb",
			"use thirddb",
			"create table s (i int primary key, j int, key (j))",
			"insert into s values (0,0), (1,1), (2,2)",
		)

		kv := sc.kv.(*memStats)
		require.Equal(t, 3, sc.Stats.dbCnt)

		runBlock(t, ctx, sqlEng,
			"drop database otherdb",
			"alter table s drop index j",
			"call dolt_stats_gc()",
		)

		// test for cleanup
		require.Equal(t, sc.Stats.dbCnt, 2)

		kv = sc.kv.(*memStats)
		require.Equal(t, 5, len(kv.buckets))
		require.Equal(t, 3, len(kv.bounds))
		require.Equal(t, 3, len(kv.templates))
		require.Equal(t, 2, len(sc.Stats.stats))
	}
}

func TestBranches(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)
	sc.enableGc = true

	{
		runBlock(t, ctx, sqlEng,
			"call dolt_commit('-Am', 'add xy')",
			"create database otherdb",
			"use otherdb",
			"create table t (i int primary key)",
			"insert into t values (0), (1)",
			"call dolt_commit('-Am', 'add t')",

			"create database thirddb",
			"use thirddb",
			"create table s (i int primary key, j int, key (j))",
			"insert into s values (0,0), (1,1), (2,2)",
			"call dolt_commit('-Am', 'add s')",
		)

		require.Equal(t, sc.Stats.dbCnt, 3)

		stat, ok := sc.Stats.stats[tableIndexesKey{"otherdb", "feat2", "t", ""}]
		require.False(t, ok)
		stat, ok = sc.Stats.stats[tableIndexesKey{"otherdb", "feat3", "t", ""}]
		require.False(t, ok)
		stat, ok = sc.Stats.stats[tableIndexesKey{"otherdb", "main", "t", ""}]
		require.Equal(t, 1, len(stat))
		stat = sc.Stats.stats[tableIndexesKey{"thirddb", "main", "s", ""}]
		require.Equal(t, 2, len(stat))

		runBlock(t, ctx, sqlEng,
			"use mydb",
			"call dolt_checkout('-b', 'feat1')",

			"use otherdb",
			"call dolt_checkout('-b', 'feat2')",
			"insert into t values (2), (3)",
			"call dolt_commit('-Am', 'insert into t')",
			"call dolt_checkout('-b', 'feat3')",
			"drop table t",
			"call dolt_commit('-Am', 'drop t')",

			"use thirddb",
			"call dolt_checkout('-b', 'feat1')",
			"alter table s drop index j",
			"call dolt_commit('-Am', 'drop index j')",
		)

		require.Equal(t, sc.Stats.dbCnt, 7)

		stat, ok = sc.Stats.stats[tableIndexesKey{"mydb", "feat1", "xy", ""}]
		require.True(t, ok)
		require.Equal(t, 2, len(stat))
		stat, ok = sc.Stats.stats[tableIndexesKey{"otherdb", "feat2", "t", ""}]
		require.True(t, ok)
		require.Equal(t, 1, len(stat))
		stat, ok = sc.Stats.stats[tableIndexesKey{"otherdb", "feat3", "t", ""}]
		require.False(t, ok)
		stat, ok = sc.Stats.stats[tableIndexesKey{"thirddb", "feat1", "s", ""}]
		require.True(t, ok)
		require.Equal(t, 1, len(stat))

		// mydb: 4 shared
		// otherdb: 1 + 1
		// thirddb: 2 + shared
		kv := sc.kv.(*memStats)
		require.Equal(t, 4+2+2, len(kv.buckets))
		require.Equal(t, 2+(1+1)+2, len(kv.bounds))
		require.Equal(t, 2+1+(2+1), len(kv.templates))
		require.Equal(t, 7-1, len(sc.Stats.stats))

		runBlock(t, ctx, sqlEng,
			"drop database otherdb",
		)

		require.Equal(t, sc.Stats.dbCnt, 4)

		stat, ok = sc.Stats.stats[tableIndexesKey{"otherdb", "feat2", "t", ""}]
		require.False(t, ok)
		stat, ok = sc.Stats.stats[tableIndexesKey{"otherdb", "main", "t", ""}]
		require.False(t, ok)

		runBlock(t, ctx, sqlEng,
			"use mydb",
			"call dolt_checkout('main')",
			"call dolt_branch('-D', 'feat1')",
		)

		require.Equal(t, sc.Stats.dbCnt, 3)

		stat, ok = sc.Stats.stats[tableIndexesKey{"mydb", "feat1", "xy", ""}]
		require.False(t, ok)
		stat, ok = sc.Stats.stats[tableIndexesKey{"mydb", "main", "xy", ""}]
		require.True(t, ok)

		runBlock(t, ctx, sqlEng, "call dolt_stats_gc()")

		// 3 dbs remaining, mydb/main, thirddb/feat1, thirddb/main
		kv = sc.kv.(*memStats)
		require.Equal(t, 4+2, len(kv.buckets))
		require.Equal(t, 4, len(kv.bounds))
		require.Equal(t, 5, len(kv.templates))
		require.Equal(t, 3, len(sc.Stats.stats))
	}
}

func runBlock(t *testing.T, ctx *sql.Context, sqlEng *gms.Engine, qs ...string) {
	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_restart()"))
	for _, q := range qs {
		require.NoError(t, executeQuery(ctx, sqlEng, q))
	}
	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))
	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_stop()"))
}

func TestBucketCounting(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, true)
	sc.enableGc = false

	// add more data
	b := strings.Repeat("b", 100)
	require.NoError(t, executeQuery(ctx, sqlEng, "create table ab (a int primary key, b varchar(100), key (b,a))"))
	abIns := strings.Builder{}
	abIns.WriteString("insert into ab values")
	for i := range 200 {
		if i > 0 {
			abIns.WriteString(", ")
		}
		abIns.WriteString(fmt.Sprintf("(%d, '%s')", i, b))
	}
	require.NoError(t, executeQuery(ctx, sqlEng, abIns.String()))

	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))

	// 4 old + 2*7 new ab
	kv := sc.kv.(*memStats)
	require.Equal(t, 18, len(kv.buckets))
	require.Equal(t, 2, len(sc.Stats.stats))

	require.NoError(t, executeQuery(ctx, sqlEng, "create table cd (c int primary key, d varchar(200), key (d,c))"))
	require.NoError(t, executeQuery(ctx, sqlEng, "insert into cd select a,b from ab"))

	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))

	// no new buckets
	kv = sc.kv.(*memStats)
	require.Equal(t, 18, len(kv.buckets))
	require.Equal(t, 3, len(sc.Stats.stats))
}

func TestDropOnlyDb(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, false)

	require.NoError(t, sc.Restart(ctx))

	_, ok := sc.kv.(*prollyStats)
	require.True(t, ok)
	require.Equal(t, "mydb", sc.statsBackingDb)

	// what happens when we drop the only database? swap to memory?
	// add first database, switch to prolly?
	require.NoError(t, executeQuery(ctx, sqlEng, "drop database mydb"))

	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))

	require.NoError(t, sc.Stop(context.Background()))

	// empty memory KV
	_, ok = sc.kv.(*memStats)
	require.True(t, ok)
	require.Equal(t, "", sc.statsBackingDb)

	require.NoError(t, executeQuery(ctx, sqlEng, "create database otherdb"))

	// empty prollyKv
	_, ok = sc.kv.(*prollyStats)
	require.True(t, ok)
	require.Equal(t, "otherdb", sc.statsBackingDb)
}

func TestRotateBackingDb(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := defaultSetup(t, threads, false)

	require.NoError(t, executeQuery(ctx, sqlEng, "create database backupdb"))

	require.NoError(t, executeQuery(ctx, sqlEng, "use backupdb"))
	require.NoError(t, executeQuery(ctx, sqlEng, "create table xy (x int primary key, y int)"))
	require.NoError(t, executeQuery(ctx, sqlEng, "insert into xy values (0,0), (1,1), (2,2)"))

	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))

	require.Equal(t, 5, sc.kv.Len())
	require.Equal(t, 2, len(sc.Stats.stats))

	require.NoError(t, executeQuery(ctx, sqlEng, "drop database mydb"))

	_, ok := sc.kv.(*prollyStats)
	require.True(t, ok)
	require.Equal(t, "backupdb", sc.statsBackingDb)

	// lost the backing storage, previous in-memory moves into new kv
	require.Equal(t, 5, sc.kv.Len())
	require.Equal(t, 1, len(sc.Stats.stats))

}

func TestPanic(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := emptySetup(t, threads, false)
	sc.SetEnableGc(true)

	require.NoError(t, sc.Restart(ctx))

	sc.sq.DoSync(ctx, func() error {
		panic("test panic")
	})

	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))
}

func emptySetup(t *testing.T, threads *sql.BackgroundThreads, memOnly bool) (*sql.Context, *gms.Engine, *StatsCoord) {
	dEnv := dtestutils.CreateTestEnv()
	sqlEng, ctx := newTestEngine(context.Background(), dEnv, threads)
	ctx.Session.SetClient(sql.Client{
		User:    "billy boy",
		Address: "bigbillie@fake.horse",
	})

	sql.SystemVariables.AssignValues(map[string]interface{}{
		dsess.DoltStatsGCInterval:     100,
		dsess.DoltStatsBranchInterval: 100,
		dsess.DoltStatsJobInterval:    1,
	})

	sc := sqlEng.Analyzer.Catalog.StatsProvider.(*StatsCoord)
	sc.SetEnableGc(false)
	sc.SetMemOnly(memOnly)
	sc.JobInterval = time.Nanosecond

	require.NoError(t, sc.Restart(ctx))

	ctx, _ = sc.ctxGen(ctx)
	ctx.Session.SetClient(sql.Client{
		User:    "billy boy",
		Address: "bigbillie@fake.horse",
	})
	require.NoError(t, executeQuery(ctx, sqlEng, "create database mydb"))
	require.NoError(t, executeQuery(ctx, sqlEng, "use mydb"))

	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))
	require.NoError(t, sc.Stop(context.Background()))

	var sqlDbs []sqle.Database
	for _, db := range sqlEng.Analyzer.Catalog.DbProvider.AllDatabases(ctx) {
		if sqlDb, ok := db.(sqle.Database); ok {
			branch := ref.NewBranchRef("main")
			db, err := sqle.RevisionDbForBranch(ctx, sqlDb, branch.GetPath(), branch.GetPath()+"/"+sqlDb.AliasedName())
			require.NoError(t, err)
			sqlDbs = append(sqlDbs, db.(sqle.Database))
		}
	}

	if memOnly {
		statsKv := NewMemStats()
		sc.kv = statsKv
	}

	return ctx, sqlEng, sc
}

func defaultSetup(t *testing.T, threads *sql.BackgroundThreads, memOnly bool) (*sql.Context, *gms.Engine, *StatsCoord) {
	ctx, sqlEng, sc := emptySetup(t, threads, memOnly)
	//sc.Debug = true

	require.NoError(t, executeQuery(ctx, sqlEng, "create table xy (x int primary key, y int, key (y,x))"))

	xyIns := strings.Builder{}
	xyIns.WriteString("insert into xy values")
	for i := range 500 {
		if i > 0 {
			xyIns.WriteString(", ")
		}
		xyIns.WriteString(fmt.Sprintf("(%d, %d)", i, i%25))
	}
	require.NoError(t, executeQuery(ctx, sqlEng, xyIns.String()))

	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_restart()"))
	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))
	require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_stop()"))

	var kv *memStats
	switch s := sc.kv.(type) {
	case *memStats:
		kv = s
	case *prollyStats:
		kv = s.mem
	}
	require.Equal(t, 4, len(kv.buckets))
	require.Equal(t, 2, len(kv.bounds))
	require.Equal(t, 2, len(kv.templates))
	require.Equal(t, 1, len(sc.Stats.stats))
	for _, tableStats := range sc.Stats.stats {
		require.Equal(t, 2, len(tableStats))
	}

	switch s := sc.kv.(type) {
	case *memStats:
		kv = s
	case *prollyStats:
		kv = s.mem
	}
	require.Equal(t, 4, len(kv.buckets))
	require.Equal(t, 2, len(kv.bounds))
	require.Equal(t, 2, len(kv.templates))
	require.Equal(t, 1, len(sc.Stats.stats))
	for _, tableStats := range sc.Stats.stats {
		require.Equal(t, 2, len(tableStats))
	}

	return ctx, sqlEng, sc
}

func executeQuery(ctx *sql.Context, eng *gms.Engine, query string) error {
	_, iter, _, err := eng.Query(ctx, query)
	if err != nil {
		return err
	}
	for {
		_, err = iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return iter.Close(ctx) // tx commit
}

func executeQueryResults(ctx *sql.Context, eng *gms.Engine, query string) ([]sql.Row, error) {
	_, iter, _, err := eng.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	var ret []sql.Row
	for {
		r, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ret = append(ret, r)
	}
	return ret, iter.Close(ctx) // tx commit
}

func newTestEngine(ctx context.Context, dEnv *env.DoltEnv, threads *sql.BackgroundThreads) (*gms.Engine, *sql.Context) {
	pro, err := sqle.NewDoltDatabaseProviderWithDatabases("main", dEnv.FS, nil, nil, threads)
	if err != nil {
		panic(err)
	}

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	if err != nil {
		panic(err)
	}

	sc := NewStatsCoord(ctx, pro, nil, logrus.StandardLogger(), threads, dEnv)

	gcSafepointController := dsess.NewGCSafepointController()

	doltSession, err := dsess.NewDoltSession(sql.NewBaseSession(), pro, dEnv.Config.WriteableConfig(), branch_control.CreateDefaultController(ctx), sc, writer.NewWriteSession, gcSafepointController)
	if err != nil {
		panic(err)
	}

	sqlCtx := sql.NewContext(ctx, sql.WithSession(doltSession))
	sqlCtx.SetCurrentDatabase(mrEnv.GetFirstDatabase())

	sc.ctxGen = func(ctx context.Context) (*sql.Context, error) {
		doltSession, err := dsess.NewDoltSession(sql.NewBaseSession(), pro, dEnv.Config.WriteableConfig(), branch_control.CreateDefaultController(ctx), sc, writer.NewWriteSession, gcSafepointController)
		if err != nil {
			return nil, err
		}
		return sql.NewContext(ctx, sql.WithSession(doltSession)), nil
	}

	pro.InitDatabaseHooks = append(pro.InitDatabaseHooks, NewInitDatabaseHook(sc))
	pro.DropDatabaseHooks = append(pro.DropDatabaseHooks, NewDropDatabaseHook(sc))

	sqlEng := gms.New(analyzer.NewBuilder(pro).Build(), &gms.Config{
		IsReadOnly:     false,
		IsServerLocked: false,
	})

	if err := sc.Init(sqlCtx, pro.AllDatabases(sqlCtx), false); err != nil {
		log.Fatal(err)
	}
	sqlEng.Analyzer.Catalog.StatsProvider = sc
	return sqlEng, sqlCtx
}

func TestStatsGcConcurrency(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := emptySetup(t, threads, false)
	sc.SetEnableGc(true)
	sc.JobInterval = 1 * time.Nanosecond
	sc.gcInterval = 100 * time.Nanosecond
	sc.branchInterval = 50 * time.Nanosecond
	require.NoError(t, sc.Restart(ctx))

	addDb := func(ctx *sql.Context, dbName string) {
		require.NoError(t, executeQuery(ctx, sqlEng, "create database "+dbName))
	}

	addData := func(ctx *sql.Context, dbName string, i int) {
		//log.Println("add ", dbName)
		require.NoError(t, executeQuery(ctx, sqlEng, "use "+dbName))
		require.NoError(t, executeQuery(ctx, sqlEng, "create table xy (x int primary key, y int)"))
		require.NoError(t, executeQuery(ctx, sqlEng, "insert into xy values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5), (6,"+strconv.Itoa(i)+")"))
	}

	dropDb := func(dropCtx *sql.Context, dbName string) {
		//log.Println("drop ", dbName)
		require.NoError(t, executeQuery(ctx, sqlEng, "use mydb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "drop database "+dbName))
	}

	// it is important to use new sessions for this test, to avoid working root conflicts
	addCtx, _ := sc.ctxGen(context.Background())
	writeCtx, _ := sc.ctxGen(context.Background())
	dropCtx, _ := sc.ctxGen(context.Background())

	iters := 200
	dbs := make(chan string, iters)

	{
		wg := sync.WaitGroup{}
		wg.Add(2)

		addCnt := 0
		go func() {
			for i := range iters {
				addCnt++
				dbName := "db" + strconv.Itoa(i)
				addDb(addCtx, dbName)
				addData(writeCtx, dbName, i)
				dbs <- dbName
			}
			close(dbs)
			wg.Done()
		}()

		dropCnt := 0
		go func() {
			i := 0
			for db := range dbs {
				if i%2 == 0 {
					time.Sleep(50 * time.Millisecond)
					dropCnt++
					dropDb(dropCtx, db)
				}
				i++
			}
			wg.Done()
		}()

		wg.Wait()

		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_gc()"))

		require.NoError(t, sc.Stop(context.Background()))

		// 101 dbs, 100 with stats (not main)
		require.Equal(t, iters/2, len(sc.Stats.stats))
		//require.NoError(t, sc.ValidateState(ctx))
		require.Equal(t, iters/2, sc.kv.Len())
	}
}

func TestStatsBranchConcurrency(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := emptySetup(t, threads, false)
	sc.SetEnableGc(true)

	sc.JobInterval = 10
	sc.gcInterval = time.Hour
	sc.branchInterval = time.Hour
	require.NoError(t, sc.Restart(ctx))

	addBranch := func(ctx *sql.Context, i int) {
		branchName := "branch" + strconv.Itoa(i)
		require.NoError(t, executeQuery(ctx, sqlEng, "use mydb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_checkout('main')"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_checkout('-b', '"+branchName+"')"))
	}

	addData := func(ctx *sql.Context, i int) {
		branchName := "branch" + strconv.Itoa(i)
		require.NoError(t, executeQuery(ctx, sqlEng, "use mydb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_checkout('"+branchName+"')"))
		require.NoError(t, executeQuery(ctx, sqlEng, "create table xy (x int primary key, y int)"))
		require.NoError(t, executeQuery(ctx, sqlEng, "insert into xy values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5), (6,"+strconv.Itoa(i)+")"))
		//require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_wait()"))
	}

	dropBranch := func(dropCtx *sql.Context, branchName string) {
		//log.Println("delete branch: ", branchName)
		require.NoError(t, executeQuery(ctx, sqlEng, "use mydb"))
		del := "call dolt_branch('-d', '" + branchName + "')"
		require.NoError(t, executeQuery(ctx, sqlEng, del))
	}

	// it is important to use new sessions for this test, to avoid working root conflicts
	addCtx, _ := sc.ctxGen(context.Background())
	dropCtx, _ := sc.ctxGen(context.Background())

	iters := 100
	{
		branches := make(chan string, iters)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			for i := range iters {
				addBranch(addCtx, i)
				addData(addCtx, i)
				branches <- "branch" + strconv.Itoa(i)
			}
			close(branches)
			wg.Done()
		}()

		go func() {
			i := 0
			for br := range branches {
				if i%2 == 0 {
					dropBranch(dropCtx, br)
					time.Sleep(50 * time.Millisecond)
				}
				i++
			}
			wg.Done()
		}()

		wg.Wait()

		err := executeQuery(ctx, sqlEng, "call dolt_stats_gc()")
		for err != nil {
			log.Println("waiting on final Gc", err)
			err = executeQuery(ctx, sqlEng, "call dolt_stats_gc()")
		}
		require.NoError(t, sc.Stop(context.Background()))

		// at the end we should still have |iters/2| databases
		require.Equal(t, iters/2, len(sc.Stats.stats))
		//require.NoError(t, sc.ValidateState(ctx))
		require.Equal(t, iters/2, sc.kv.Len())
	}
}

func TestStatsCacheGrowth(t *testing.T) {
	//t.Skip("expensive test")

	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc := emptySetup(t, threads, false)
	sc.SetEnableGc(true)

	sc.JobInterval = 10
	sc.gcInterval = time.Hour
	sc.branchInterval = time.Hour
	require.NoError(t, sc.Restart(ctx))

	addBranch := func(ctx *sql.Context, i int) {
		branchName := "branch" + strconv.Itoa(i)
		require.NoError(t, executeQuery(ctx, sqlEng, "use mydb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_checkout('main')"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_checkout('-b', '"+branchName+"')"))
	}

	addData := func(ctx *sql.Context, i int) {
		branchName := "branch" + strconv.Itoa(i)
		require.NoError(t, executeQuery(ctx, sqlEng, "use mydb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_checkout('"+branchName+"')"))
		require.NoError(t, executeQuery(ctx, sqlEng, "create table xy (x int primary key, y int)"))
		require.NoError(t, executeQuery(ctx, sqlEng, "insert into xy values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5), (6,"+strconv.Itoa(i)+")"))

	}

	iters := 2000
	if os.Getenv("CI") != "" {
		iters = 1025
	}
	{
		branches := make(chan string, iters)

		go func() {
			addCtx, _ := sc.ctxGen(context.Background())
			for i := range iters {
				addBranch(addCtx, i)
				addData(addCtx, i)
				branches <- "branch" + strconv.Itoa(i)
				if i%500 == 0 {
					log.Println("branches: ", strconv.Itoa(i))
					require.NoError(t, executeQuery(addCtx, sqlEng, "call dolt_stats_wait()"))
				}
			}
			close(branches)
		}()

		//waitCtx, _ := sc.ctxGen(context.Background())
		i := 0
		for _ = range branches {
			//if i%50 == 0 {
			//	log.Println("branches: ", strconv.Itoa(i))
			//	require.NoError(t, executeQuery(waitCtx, sqlEng, "call dolt_stats_wait()"))
			//}
			i++
		}

		executeQuery(ctx, sqlEng, "call dolt_stats_wait()")
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_stats_gc()"))

		require.NoError(t, sc.Stop(context.Background()))

		// at the end we should still have |iters/2| databases
		require.Equal(t, iters, len(sc.Stats.stats))
		//require.NoError(t, sc.ValidateState(ctx))
		require.Equal(t, iters, sc.kv.Len())
	}
}
