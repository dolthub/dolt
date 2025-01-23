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
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/stats"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestScheduleLoop(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, sqlDbs := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

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

		// run two cycles -> (1) seed, (2) populate
		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			ReadJob{
				db: sqlDbs[0], table: "ab",
				ordinals: []updateOrdinal{{0, 47}, {47, 59}, {59, 94}, {94, 125}, {125, 159}, {159, 191}, {191, 200}},
			},
			ReadJob{
				db: sqlDbs[0], table: "ab",
				ordinals: []updateOrdinal{{0, 26}, {26, 55}, {55, 92}, {92, 110}, {110, 147}, {147, 189}, {189, 200}},
			},
			FinalizeJob{
				tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "ab"},
				indexes: map[templateCacheKey]finalizeStruct{
					templateCacheKey{idxName: "PRIMARY"}: {},
					templateCacheKey{idxName: "b"}:       {},
				}},
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "ab"}, {name: "xy"}}},
		})

		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "ab"}, {name: "xy"}}},
		})

		// 4 old + 2*7 new ab
		kv := sc.kv.(*memStats)
		require.Equal(t, 18, kv.buckets.Len())
		require.Equal(t, 4, len(kv.bounds))
		require.Equal(t, 4, len(kv.templates))
		require.Equal(t, 2, len(sc.Stats))
		stat := sc.Stats[tableIndexesKey{"mydb", "main", "ab", ""}]
		require.Equal(t, 7, len(stat[0].Hist))
		require.Equal(t, 7, len(stat[1].Hist))
	}

	require.NoError(t, executeQuery(ctx, sqlEng, "drop table xy"))
	runAndPause(ctx, sc, &wg)
	runAndPause(ctx, sc, &wg)

	doGcCycle(t, ctx, sc)

	kv := sc.kv.(*memStats)
	require.Equal(t, 14, kv.buckets.Len())
	require.Equal(t, 2, len(kv.bounds))
	require.Equal(t, 2, len(kv.templates))
	require.Equal(t, 1, len(sc.Stats))
	stat := sc.Stats[tableIndexesKey{"mydb", "main", "ab", ""}]
	require.Equal(t, 2, len(stat))
	require.Equal(t, 7, len(stat[0].Hist))
	require.Equal(t, 7, len(stat[1].Hist))
}

func TestAnalyze(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, sqlDbs := defaultSetup(t, threads)

	sc.flushQueue(ctx)

	wg := sync.WaitGroup{}

	require.NoError(t, executeQuery(ctx, sqlEng, "insert into xy values (-1,-1)"))

	analyze := NewAnalyzeJob(ctx, sqlDbs[0], []string{"xy"}, ControlJob{})
	sc.Jobs <- analyze

	validateJobState(t, ctx, sc, []StatsJob{
		AnalyzeJob{
			sqlDb:  sqlDbs[0],
			tables: []string{"xy"},
		},
	})

	runAndPause(ctx, sc, &wg)
	validateJobState(t, ctx, sc, []StatsJob{
		ReadJob{db: sqlDbs[0], table: "xy", nodes: []tree.Node{{}}, ordinals: []updateOrdinal{{0, 416}}},
		ReadJob{db: sqlDbs[0], table: "xy", nodes: []tree.Node{{}}, ordinals: []updateOrdinal{{0, 241}}},
		FinalizeJob{
			tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "xy"},
			indexes: map[templateCacheKey]finalizeStruct{
				templateCacheKey{idxName: "PRIMARY"}: {},
				templateCacheKey{idxName: "y"}:       {},
			}},
	})

	runAndPause(ctx, sc, &wg)
	validateJobState(t, ctx, sc, []StatsJob{})
	kv := sc.kv.(*memStats)
	require.Equal(t, 6, kv.buckets.Len())
	require.Equal(t, 4, len(kv.bounds))
	require.Equal(t, 2, len(kv.templates))
	require.Equal(t, 1, len(sc.Stats))
	for _, tableStats := range sc.Stats {
		require.Equal(t, 2, len(tableStats))
	}
}

func TestModifyColumn(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, sqlDbs := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	{
		require.NoError(t, executeQuery(ctx, sqlEng, "alter table xy modify column y bigint"))

		// expect finalize, no GC
		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			ReadJob{db: sqlDbs[0], table: "xy", ordinals: []updateOrdinal{{0, 210}, {210, 415}, {415, 470}, {470, 500}}},
			ReadJob{db: sqlDbs[0], table: "xy", ordinals: []updateOrdinal{{0, 267}, {267, 500}}},
			FinalizeJob{
				tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "xy"},
				indexes: map[templateCacheKey]finalizeStruct{
					templateCacheKey{idxName: "PRIMARY"}: {},
					templateCacheKey{idxName: "y"}:       {},
				}},
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		kv := sc.kv.(*memStats)
		require.Equal(t, 10, kv.buckets.Len())
		require.Equal(t, 4, len(kv.bounds))
		require.Equal(t, 4, len(kv.templates))
		require.Equal(t, 1, len(sc.Stats))
		stat := sc.Stats[tableIndexesKey{"mydb", "main", "xy", ""}]
		require.Equal(t, 4, len(stat[0].Hist))
		require.Equal(t, 2, len(stat[1].Hist))
		require.Equal(t, int64(6), sc.bucketCnt.Load())

		doGcCycle(t, ctx, sc)
		require.Equal(t, int64(6), sc.bucketCnt.Load())
		require.Equal(t, 6, kv.buckets.Len())
	}
}

func TestAddColumn(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, sqlDbs := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	{
		require.NoError(t, executeQuery(ctx, sqlEng, "alter table xy add column z int"))

		// schema but no data change
		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			FinalizeJob{
				tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "xy"},
				indexes: map[templateCacheKey]finalizeStruct{
					templateCacheKey{idxName: "PRIMARY"}: {},
				},
			},
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		kv := sc.kv.(*memStats)
		require.Equal(t, 4, kv.buckets.Len())
		require.Equal(t, 2, len(kv.bounds))
		require.Equal(t, 4, len(kv.templates)) // +2 for new schema
		require.Equal(t, 1, len(sc.Stats))
		stat := sc.Stats[tableIndexesKey{"mydb", "main", "xy", ""}]
		require.Equal(t, 2, len(stat[0].Hist))
		require.Equal(t, 2, len(stat[1].Hist))
		require.Equal(t, int64(4), sc.bucketCnt.Load())
	}
}

func TestDropIndex(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, sqlDbs := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	{
		require.NoError(t, executeQuery(ctx, sqlEng, "alter table xy drop index y"))

		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			FinalizeJob{
				tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "xy"},
				indexes: map[templateCacheKey]finalizeStruct{
					templateCacheKey{idxName: "PRIMARY"}: {},
				},
			},
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		kv := sc.kv.(*memStats)
		require.Equal(t, 4, kv.buckets.Len())
		require.Equal(t, 2, len(kv.bounds))
		require.Equal(t, 3, len(kv.templates))
		require.Equal(t, 1, len(sc.Stats))
		stat := sc.Stats[tableIndexesKey{"mydb", "main", "xy", ""}]
		require.Equal(t, 1, len(stat))
		require.Equal(t, 2, len(stat[0].Hist))
		require.Equal(t, int64(2), sc.bucketCnt.Load())

		doGcCycle(t, ctx, sc)

		kv = sc.kv.(*memStats)
		require.Equal(t, 2, kv.buckets.Len())
		require.Equal(t, 1, len(kv.bounds))
		require.Equal(t, 1, len(kv.templates))
		require.Equal(t, 1, len(sc.Stats))
		stat = sc.Stats[tableIndexesKey{"mydb", "main", "xy", ""}]
		require.Equal(t, 1, len(stat))
		require.Equal(t, 2, len(stat[0].Hist))
		require.Equal(t, int64(2), sc.bucketCnt.Load())
	}
}

func TestDropTable(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, sqlDbs := defaultSetup(t, threads)
	wg := sync.WaitGroup{}
	{
		require.NoError(t, executeQuery(ctx, sqlEng, "create table ab (a int primary key, b int)"))
		require.NoError(t, executeQuery(ctx, sqlEng, "insert into ab values (0,0)"))
		require.NoError(t, executeQuery(ctx, sqlEng, "drop table xy"))

		runAndPause(ctx, sc, &wg)

		validateJobState(t, ctx, sc, []StatsJob{
			ReadJob{db: sqlDbs[0], table: "ab", ordinals: []updateOrdinal{{0, 1}}},
			FinalizeJob{
				tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "ab"},
				indexes: map[templateCacheKey]finalizeStruct{
					templateCacheKey{idxName: "PRIMARY"}: {},
				},
			},
			FinalizeJob{
				tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "xy"},
				indexes:  nil,
			},
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "ab"}}},
		})

		runAndPause(ctx, sc, &wg)

		kv := sc.kv.(*memStats)
		require.Equal(t, 5, kv.buckets.Len())
		require.Equal(t, 3, len(kv.bounds))
		require.Equal(t, 3, len(kv.templates))
		require.Equal(t, 1, len(sc.Stats))
		stat := sc.Stats[tableIndexesKey{"mydb", "main", "ab", ""}]
		require.Equal(t, 1, len(stat))
		require.Equal(t, 1, len(stat[0].Hist))

		doGcCycle(t, ctx, sc)

		select {
		case <-sc.gcDone:
			break
		default:
			require.Fail(t, "failed to finish GC")
		}

		kv = sc.kv.(*memStats)
		require.Equal(t, 1, kv.buckets.Len())
		require.Equal(t, 1, len(kv.bounds))
		require.Equal(t, 1, len(kv.templates))
		require.Equal(t, 1, len(sc.Stats))
		stat = sc.Stats[tableIndexesKey{"mydb", "main", "ab", ""}]
		require.Equal(t, 1, len(stat))
		require.Equal(t, 1, len(stat[0].Hist))
		require.Equal(t, int64(1), sc.bucketCnt.Load())
	}
}

func TestDeleteAboveBoundary(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, _ := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	require.NoError(t, executeQuery(ctx, sqlEng, "alter table xy drop index y"))

	{
		require.NoError(t, executeQuery(ctx, sqlEng, "delete from xy where x > 498"))

		runAndPause(ctx, sc, &wg) // seed
		runAndPause(ctx, sc, &wg) // finalize

		kv := sc.kv.(*memStats)
		require.Equal(t, 5, kv.buckets.Len()) // 1 for new chunk
		require.Equal(t, 2, len(kv.bounds))
		require.Equal(t, 3, len(kv.templates)) // +1 for schema change
		require.Equal(t, 1, len(sc.Stats))
		stat := sc.Stats[tableIndexesKey{db: "mydb", branch: "main", table: "xy"}]
		require.Equal(t, 2, len(stat[0].Hist))
		require.Equal(t, int64(2), sc.bucketCnt.Load())

		doGcCycle(t, ctx, sc)
		require.Equal(t, 2, kv.buckets.Len())
		require.Equal(t, int64(2), sc.bucketCnt.Load())
	}
}

func TestDeleteBelowBoundary(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, _ := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	require.NoError(t, executeQuery(ctx, sqlEng, "alter table xy drop index y"))

	{
		require.NoError(t, executeQuery(ctx, sqlEng, "delete from xy where x > 410"))

		runAndPause(ctx, sc, &wg) // seed
		runAndPause(ctx, sc, &wg) // finalize

		kv := sc.kv.(*memStats)

		require.Equal(t, 5, kv.buckets.Len()) // +1 rewrite partial chunk
		require.Equal(t, 3, len(kv.bounds))   // +1 rewrite first chunk
		require.Equal(t, 3, len(kv.templates))
		require.Equal(t, 1, len(sc.Stats))
		stat := sc.Stats[tableIndexesKey{db: "mydb", branch: "main", table: "xy"}]
		require.Equal(t, 1, len(stat[0].Hist))
		require.Equal(t, int64(1), sc.bucketCnt.Load())

		doGcCycle(t, ctx, sc)
		require.Equal(t, 1, kv.buckets.Len())
		require.Equal(t, int64(1), sc.bucketCnt.Load())
	}
}

func TestDeleteOnBoundary(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, _ := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	require.NoError(t, executeQuery(ctx, sqlEng, "alter table xy drop index y"))

	{
		// PRIMARY boundary chunk -> rewrite y_idx's second
		require.NoError(t, executeQuery(ctx, sqlEng, "delete from xy where x > 414"))

		runAndPause(ctx, sc, &wg) // seed
		runAndPause(ctx, sc, &wg) // finalize

		kv := sc.kv.(*memStats)
		require.Equal(t, 4, kv.buckets.Len())
		require.Equal(t, 2, len(kv.bounds))
		require.Equal(t, 3, len(kv.templates)) // +1 schema change
		require.Equal(t, 1, len(sc.Stats))
		stat := sc.Stats[tableIndexesKey{db: "mydb", branch: "main", table: "xy"}]
		require.Equal(t, 1, len(stat[0].Hist))
		require.Equal(t, int64(1), sc.bucketCnt.Load())

		doGcCycle(t, ctx, sc)
		require.Equal(t, 1, kv.buckets.Len())
		require.Equal(t, int64(1), sc.bucketCnt.Load())
	}
}

func TestAddDropDatabases(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, sqlDbs := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	addHook := NewStatsInitDatabaseHook2(sc, nil, threads)
	var otherDb sqle.Database
	{
		require.NoError(t, executeQuery(ctx, sqlEng, "create database otherdb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "use otherdb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "create table t (i int primary key)"))
		require.NoError(t, executeQuery(ctx, sqlEng, "insert into t values (0), (1)"))

		for _, db := range sqlEng.Analyzer.Catalog.DbProvider.AllDatabases(ctx) {
			if db.Name() == "otherdb" {
				dsessDb, err := sqle.RevisionDbForBranch(ctx, db.(dsess.SqlDatabase), "main", "main/"+db.Name())
				require.NoError(t, err)
				otherDb = dsessDb.(sqle.Database)
				addHook(ctx, nil, "otherdb", nil, otherDb)
			}
		}

		// finish queue of read/finalize
		runAndPause(ctx, sc, &wg)

		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
			ReadJob{db: otherDb, table: "t", ordinals: []updateOrdinal{{0, 2}}},
			FinalizeJob{
				tableKey: tableIndexesKey{db: "otherdb", branch: "main", table: "t"},
				indexes: map[templateCacheKey]finalizeStruct{
					templateCacheKey{idxName: "PRIMARY"}: {},
				}},
			SeedDbTablesJob{sqlDb: otherDb, tables: []tableStatsInfo{{name: "t"}}},
		})

		runAndPause(ctx, sc, &wg)

		// xy and t
		kv := sc.kv.(*memStats)
		require.Equal(t, 5, kv.buckets.Len())
		require.Equal(t, 3, len(kv.bounds))
		require.Equal(t, 3, len(kv.templates))
		require.Equal(t, 2, len(sc.Stats))
		stat := sc.Stats[tableIndexesKey{db: "otherdb", branch: "main", table: "t"}]
		require.Equal(t, 1, len(stat))
	}

	dropHook := NewStatsDropDatabaseHook2(sc)
	{
		require.NoError(t, executeQuery(ctx, sqlEng, "drop database otherdb"))
		dropHook(ctx, "otherdb")

		_, ok := sc.Stats[tableIndexesKey{db: "otherdb", branch: "main", table: "t"}]
		require.False(t, ok)
	}
}

func TestGC(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, _ := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	addHook := NewStatsInitDatabaseHook2(sc, nil, threads)

	{
		require.NoError(t, executeQuery(ctx, sqlEng, "create database otherdb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "use otherdb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "create table t (i int primary key)"))
		require.NoError(t, executeQuery(ctx, sqlEng, "insert into t values (0), (1)"))

		require.NoError(t, executeQuery(ctx, sqlEng, "create database thirddb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "use thirddb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "create table s (i int primary key, j int, key (j))"))
		require.NoError(t, executeQuery(ctx, sqlEng, "insert into s values (0,0), (1,1), (2,2)"))

		var otherDb sqle.Database
		var thirdDb sqle.Database
		for _, db := range sqlEng.Analyzer.Catalog.DbProvider.AllDatabases(ctx) {
			if db.Name() == "otherdb" {
				dsessDb, err := sqle.RevisionDbForBranch(ctx, db.(dsess.SqlDatabase), "main", "main/"+db.Name())
				require.NoError(t, err)
				otherDb = dsessDb.(sqle.Database)
				addHook(ctx, nil, "otherdb", nil, otherDb)
			}
			if db.Name() == "thirddb" {
				dsessDb, err := sqle.RevisionDbForBranch(ctx, db.(dsess.SqlDatabase), "main", "main/"+db.Name())
				require.NoError(t, err)
				thirdDb = dsessDb.(sqle.Database)
				addHook(ctx, nil, "thirddb", nil, thirdDb)
			}
		}

		runAndPause(ctx, sc, &wg) // read jobs
		runAndPause(ctx, sc, &wg) // finalize

		dropHook := NewStatsDropDatabaseHook2(sc)
		require.NoError(t, executeQuery(ctx, sqlEng, "drop database otherdb"))
		dropHook(ctx, "otherdb")

		require.NoError(t, executeQuery(ctx, sqlEng, "alter table s drop index j"))

		runAndPause(ctx, sc, &wg) // pick up table drop
		runAndPause(ctx, sc, &wg) // finalize

		doGcCycle(t, ctx, sc)

		// test for cleanup
		kv := sc.kv.(*memStats)
		require.Equal(t, 5, kv.buckets.Len())
		require.Equal(t, 3, len(kv.bounds))
		require.Equal(t, 3, len(kv.templates))
		require.Equal(t, 2, len(sc.Stats))
	}
}

func TestBranches(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, _ := defaultSetup(t, threads)
	wg := sync.WaitGroup{}
	sc.disableGc.Store(true)

	addHook := NewStatsInitDatabaseHook2(sc, nil, threads)

	{
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_commit('-Am', 'add xy')"))

		require.NoError(t, executeQuery(ctx, sqlEng, "create database otherdb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "use otherdb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "create table t (i int primary key)"))
		require.NoError(t, executeQuery(ctx, sqlEng, "insert into t values (0), (1)"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_commit('-Am', 'add t')"))

		require.NoError(t, executeQuery(ctx, sqlEng, "create database thirddb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "use thirddb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "create table s (i int primary key, j int, key (j))"))
		require.NoError(t, executeQuery(ctx, sqlEng, "insert into s values (0,0), (1,1), (2,2)"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_commit('-Am', 'add s')"))

		var otherDb sqle.Database
		var thirdDb sqle.Database
		for _, db := range sqlEng.Analyzer.Catalog.DbProvider.AllDatabases(ctx) {
			if db.Name() == "otherdb" {
				dsessDb, err := sqle.RevisionDbForBranch(ctx, db.(dsess.SqlDatabase), "main", "main/"+db.Name())
				require.NoError(t, err)
				otherDb = dsessDb.(sqle.Database)
				addHook(ctx, nil, "otherdb", nil, otherDb)
			}
			if db.Name() == "thirddb" {
				dsessDb, err := sqle.RevisionDbForBranch(ctx, db.(dsess.SqlDatabase), "main", "main/"+db.Name())
				require.NoError(t, err)
				thirdDb = dsessDb.(sqle.Database)
				addHook(ctx, nil, "thirddb", nil, thirdDb)
			}
		}

		runAndPause(ctx, sc, &wg) // read jobs
		runAndPause(ctx, sc, &wg) // finalize

		require.NoError(t, executeQuery(ctx, sqlEng, "use mydb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_checkout('-b', 'feat1')"))

		require.NoError(t, executeQuery(ctx, sqlEng, "use otherdb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_checkout('-b', 'feat2')"))
		require.NoError(t, executeQuery(ctx, sqlEng, "insert into t values (2), (3)"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_commit('-Am', 'insert into t')"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_checkout('-b', 'feat3')"))
		require.NoError(t, executeQuery(ctx, sqlEng, "drop table t"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_commit('-Am', 'drop t')"))

		require.NoError(t, executeQuery(ctx, sqlEng, "use thirddb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_checkout('-b', 'feat1')"))
		require.NoError(t, executeQuery(ctx, sqlEng, "alter table s drop index j"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_commit('-Am', 'drop index j')"))

		runAndPause(ctx, sc, &wg) // pick up table changes
		runAndPause(ctx, sc, &wg) // finalize

		sc.doBranchCheck.Store(true)
		runAndPause(ctx, sc, &wg) // new branches

		require.Equal(t, 7, len(sc.dbs))
		stat, ok := sc.Stats[tableIndexesKey{"otherdb", "feat2", "t", ""}]
		require.False(t, ok)
		stat, ok = sc.Stats[tableIndexesKey{"otherdb", "feat3", "t", ""}]
		require.False(t, ok)
		stat, ok = sc.Stats[tableIndexesKey{"thirddb", "feat1", "s", ""}]
		require.False(t, ok)
		stat, ok = sc.Stats[tableIndexesKey{"otherdb", "main", "t", ""}]
		require.Equal(t, 1, len(stat))
		stat = sc.Stats[tableIndexesKey{"thirddb", "main", "s", ""}]
		require.Equal(t, 2, len(stat))

		runAndPause(ctx, sc, &wg) // seed new branches
		runAndPause(ctx, sc, &wg) // finalize branches

		require.Equal(t, 7, len(sc.dbs))

		stat, ok = sc.Stats[tableIndexesKey{"mydb", "feat1", "xy", ""}]
		require.True(t, ok)
		require.Equal(t, 2, len(stat))
		stat, ok = sc.Stats[tableIndexesKey{"otherdb", "feat2", "t", ""}]
		require.True(t, ok)
		require.Equal(t, 1, len(stat))
		stat, ok = sc.Stats[tableIndexesKey{"otherdb", "feat3", "t", ""}]
		require.False(t, ok)
		stat, ok = sc.Stats[tableIndexesKey{"thirddb", "feat1", "s", ""}]
		require.True(t, ok)
		require.Equal(t, 1, len(stat))

		// mydb: 4 shared
		// otherdb: 1 + 1
		// thirddb: 2 + shared
		kv := sc.kv.(*memStats)
		require.Equal(t, 4+2+2, kv.buckets.Len())
		require.Equal(t, 2+(1+1)+2, len(kv.bounds))
		require.Equal(t, 2+1+(2+1), len(kv.templates))
		require.Equal(t, 7-1, len(sc.Stats))

		dropHook := NewStatsDropDatabaseHook2(sc)
		require.NoError(t, executeQuery(ctx, sqlEng, "drop database otherdb"))
		dropHook(ctx, "otherdb")

		runAndPause(ctx, sc, &wg) // finalize drop otherdb

		require.Equal(t, 4, len(sc.dbs))
		stat, ok = sc.Stats[tableIndexesKey{"otherdb", "feat2", "t", ""}]
		require.False(t, ok)
		stat, ok = sc.Stats[tableIndexesKey{"otherdb", "main", "t", ""}]
		require.False(t, ok)

		require.NoError(t, executeQuery(ctx, sqlEng, "use mydb"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_checkout('main')"))
		require.NoError(t, executeQuery(ctx, sqlEng, "call dolt_branch('-D', 'feat1')"))

		sc.doBranchCheck.Store(true)
		runAndPause(ctx, sc, &wg) // detect deleted branch
		runAndPause(ctx, sc, &wg) // finalize branch delete

		require.Equal(t, 3, len(sc.dbs))
		stat, ok = sc.Stats[tableIndexesKey{"mydb", "feat1", "xy", ""}]
		require.False(t, ok)
		stat, ok = sc.Stats[tableIndexesKey{"mydb", "main", "xy", ""}]
		require.True(t, ok)

		doGcCycle(t, ctx, sc)

		// 3 dbs remaining, mydb/main, thirddb/feat1, thirddb/main
		kv = sc.kv.(*memStats)
		require.Equal(t, 4+2, kv.buckets.Len())
		require.Equal(t, 4, len(kv.bounds))
		require.Equal(t, 5, len(kv.templates))
		require.Equal(t, 3, len(sc.Stats))
	}
}

func TestBucketDoubling(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, _ := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	cur := sc.kv.(*memStats).buckets
	newB, _ := lru.New[hash.Hash, *stats.Bucket](4)
	for _, k := range cur.Keys() {
		v, _ := cur.Get(k)
		newB.Add(k, v)
	}
	sc.kv.(*memStats).buckets = newB
	sc.bucketCap = 4

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

	sc.disableGc.Store(false)

	runAndPause(ctx, sc, &wg) // track ab
	runAndPause(ctx, sc, &wg) // finalize ab

	// 4 old + 2*7 new ab
	kv := sc.kv.(*memStats)
	require.Equal(t, 18, kv.buckets.Len())
	require.Equal(t, 4, len(kv.bounds))
	require.Equal(t, 4, len(kv.templates))
	require.Equal(t, 2, len(sc.Stats))
	stat := sc.Stats[tableIndexesKey{"mydb", "main", "ab", ""}]
	require.Equal(t, 7, len(stat[0].Hist))
	require.Equal(t, 7, len(stat[1].Hist))
}

func TestBucketCounting(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, _ := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

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

	sc.disableGc.Store(false)

	runAndPause(ctx, sc, &wg) // track ab
	runAndPause(ctx, sc, &wg) // finalize ab

	// 4 old + 2*7 new ab
	kv := sc.kv.(*memStats)
	require.Equal(t, 18, kv.buckets.Len())
	require.Equal(t, 2, len(sc.Stats))

	require.NoError(t, executeQuery(ctx, sqlEng, "create table cd (c int primary key, d varchar(200), key (d,c))"))
	require.NoError(t, executeQuery(ctx, sqlEng, "insert into cd select a,b from ab"))

	runAndPause(ctx, sc, &wg) // track ab
	runAndPause(ctx, sc, &wg) // finalize ab

	// no new buckets
	kv = sc.kv.(*memStats)
	require.Equal(t, 18, kv.buckets.Len())
	require.Equal(t, 3, len(sc.Stats))
}

func TestDropOnlyDb(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, startDbs := defaultSetup(t, threads)

	addHook := NewStatsInitDatabaseHook2(sc, nil, threads)
	dropHook := NewStatsDropDatabaseHook2(sc)

	prollyKv, err := NewProllyStats(ctx, startDbs[0])
	require.NoError(t, err)
	prollyKv.mem = sc.kv.(*memStats)
	sc.kv = prollyKv
	sc.statsBackingDb = "mydb"

	// what happens when we drop the only database? swap to memory?
	// add first database, switch to prolly?
	require.NoError(t, executeQuery(ctx, sqlEng, "drop database mydb"))
	dropHook(ctx, "mydb")

	// empty memory KV
	_, ok := sc.kv.(*memStats)
	require.True(t, ok)
	require.Equal(t, "", sc.statsBackingDb)

	require.NoError(t, executeQuery(ctx, sqlEng, "create database mydb"))

	for _, db := range sqlEng.Analyzer.Catalog.DbProvider.AllDatabases(ctx) {
		if db.Name() == "mydb" {
			dsessDb, err := sqle.RevisionDbForBranch(ctx, db.(dsess.SqlDatabase), "main", "main/"+db.Name())
			require.NoError(t, err)
			addHook(ctx, nil, "mydb", nil, dsessDb)
		}
	}

	// empty prollyKv
	prollyKv, ok = sc.kv.(*prollyStats)
	require.True(t, ok)
	require.Equal(t, "mydb", sc.statsBackingDb)
}

func TestRotateBackingDb(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, startDbs := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	addHook := NewStatsInitDatabaseHook2(sc, nil, threads)
	dropHook := NewStatsDropDatabaseHook2(sc)

	prollyKv, err := NewProllyStats(ctx, startDbs[0])
	require.NoError(t, err)
	prollyKv.mem = sc.kv.(*memStats)

	require.NoError(t, executeQuery(ctx, sqlEng, "create database backupdb"))
	for _, db := range sqlEng.Analyzer.Catalog.DbProvider.AllDatabases(ctx) {
		if db.Name() == "backupdb" {
			dsessDb, err := sqle.RevisionDbForBranch(ctx, db.(dsess.SqlDatabase), "main", "main/"+db.Name())
			require.NoError(t, err)
			addHook(ctx, nil, "backupdb", nil, dsessDb)
		}
	}

	require.NoError(t, executeQuery(ctx, sqlEng, "use backupdb"))
	require.NoError(t, executeQuery(ctx, sqlEng, "create table xy (x int primary key, y int)"))
	require.NoError(t, executeQuery(ctx, sqlEng, "insert into xy values (0,0), (1,1), (2,2)"))

	runAndPause(ctx, sc, &wg) // track xy
	runAndPause(ctx, sc, &wg) // finalize xy

	require.Equal(t, 5, sc.kv.Len())
	require.Equal(t, 2, len(sc.Stats))

	require.NoError(t, executeQuery(ctx, sqlEng, "drop database mydb"))
	dropHook(ctx, "mydb")

	prollyKv, ok := sc.kv.(*prollyStats)
	require.True(t, ok)
	require.Equal(t, "backupdb", sc.statsBackingDb)

	// lost the backing storage, in-memory switches to new kv
	require.Equal(t, 5, sc.kv.Len())
	require.Equal(t, 1, len(sc.Stats))

}

func TestReadCounter(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, _ := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	{
		require.Equal(t, 0, sc.Info().ReadCnt)

		require.NoError(t, executeQuery(ctx, sqlEng, "insert into xy values (501, 0)"))
		runAndPause(ctx, sc, &wg)

		require.Equal(t, 2, sc.Info().ReadCnt)
	}
}

func TestJobQueueDoubling(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	dEnv := dtestutils.CreateTestEnv()
	sqlEng, ctx := newTestEngine(context.Background(), dEnv)
	defer sqlEng.Close()

	sc := NewStatsCoord(time.Nanosecond, sqlEng.Analyzer.Catalog.DbProvider.(*sqle.DoltDatabaseProvider), ctx.GetLogger().Logger, threads, dEnv)

	sc.Jobs = make(chan StatsJob, 1)

	var jobs []StatsJob
	for _ = range 1025 {
		jobs = append(jobs, ControlJob{})
	}
	require.NoError(t, sc.sendJobs(ctx, jobs...))
	require.Equal(t, 1025, len(sc.Jobs))
	require.Equal(t, 2048, cap(sc.Jobs))
}

func defaultSetup(t *testing.T, threads *sql.BackgroundThreads) (*sql.Context, *gms.Engine, *StatsCoord, []sqle.Database) {
	dEnv := dtestutils.CreateTestEnv()
	sqlEng, ctx := newTestEngine(context.Background(), dEnv)
	ctx.Session.SetClient(sql.Client{
		User:    "billy boy",
		Address: "bigbillie@fake.horse",
	})
	require.NoError(t, executeQuery(ctx, sqlEng, "create database mydb"))
	require.NoError(t, executeQuery(ctx, sqlEng, "use mydb"))
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

	startDbs := sqlEng.Analyzer.Catalog.DbProvider.AllDatabases(ctx)

	sc := NewStatsCoord(time.Nanosecond, sqlEng.Analyzer.Catalog.DbProvider.(*sqle.DoltDatabaseProvider), ctx.GetLogger().Logger, threads, dEnv)
	sc.pro = sqlEng.Analyzer.Catalog.DbProvider.(*sqle.DoltDatabaseProvider)
	sc.disableGc.Store(true)

	wg := sync.WaitGroup{}

	var sqlDbs []sqle.Database

	{
		// initialize seed jobs

		for _, db := range startDbs {
			if sqlDb, ok := db.(sqle.Database); ok {
				br, err := sqlDb.DbData().Ddb.GetBranches(ctx)
				require.NoError(t, err)
				for _, b := range br {
					sqlDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, b.GetPath(), b.GetPath()+"/"+sqlDb.AliasedName())
					require.NoError(t, err)
					sqlDbs = append(sqlDbs, sqlDb.(sqle.Database))
					done := sc.Add(ctx, sqlDb.(sqle.Database))
					waitOnJob(&wg, done)
				}
			}
		}

		validateJobState(t, ctx, sc, []StatsJob{
			// first job doesn't have tracked tables
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: nil},
		})
	}

	statsKv := NewMemStats()
	sc.kv = statsKv

	{
		// seed creates read jobs
		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			ReadJob{db: sqlDbs[0], table: "xy", ordinals: []updateOrdinal{{0, 415}, {415, 500}}},
			ReadJob{db: sqlDbs[0], table: "xy", ordinals: []updateOrdinal{{0, 240}, {240, 500}}},
			FinalizeJob{
				tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "xy"},
				indexes: map[templateCacheKey]finalizeStruct{
					templateCacheKey{idxName: "PRIMARY"}: {},
					templateCacheKey{idxName: "y"}:       {},
				}},
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})
	}

	{
		// read jobs populate cache
		runAndPause(ctx, sc, &wg)

		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		kv := sc.kv.(*memStats)
		require.Equal(t, 4, kv.buckets.Len())
		require.Equal(t, 2, len(kv.bounds))
		require.Equal(t, 2, len(kv.templates))
		require.Equal(t, 1, len(sc.Stats))
		for _, tableStats := range sc.Stats {
			require.Equal(t, 2, len(tableStats))
		}
	}

	{
		// seed with no changes yields no new jobs
		runAndPause(ctx, sc, &wg)

		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		kv := sc.kv.(*memStats)
		require.Equal(t, 4, kv.buckets.Len())
		require.Equal(t, 2, len(kv.bounds))
		require.Equal(t, 2, len(kv.templates))
		require.Equal(t, 1, len(sc.Stats))
		for _, tableStats := range sc.Stats {
			require.Equal(t, 2, len(tableStats))
		}
	}
	return ctx, sqlEng, sc, sqlDbs
}

// validateJobs compares the current event loop and launches a background thread
// that will repopulate the queue in-order
func validateJobState(t *testing.T, ctx context.Context, sc *StatsCoord, expected []StatsJob) {
	jobs, err := sc.flushQueue(ctx)
	require.NoError(t, err)

	require.Equal(t, len(expected), len(jobs), fmt.Sprintf("expected: %s; found: %s", expected, jobs))
	for i, j := range jobs {
		switch j := j.(type) {
		case SeedDbTablesJob:
			ej, ok := expected[i].(SeedDbTablesJob)
			require.True(t, ok)
			for i := range ej.tables {
				require.Equal(t, ej.tables[i].name, j.tables[i].name)
			}
			require.Equal(t, ej.sqlDb.Name(), j.sqlDb.Name())
			require.Equal(t, ej.sqlDb.Revision(), j.sqlDb.Revision())
		case ReadJob:
			ej, ok := expected[i].(ReadJob)
			require.True(t, ok)
			require.Equal(t, ej.table, j.table)
			require.Equal(t, ej.ordinals, j.ordinals)
			require.Equal(t, ej.db.AliasedName(), j.db.AliasedName())
			require.Equal(t, ej.db.Revision(), j.db.Revision())
		case FinalizeJob:
			ej, ok := expected[i].(FinalizeJob)
			require.True(t, ok)
			require.Equal(t, ej.tableKey, j.tableKey)
			idx := make(map[string]bool)
			for k, _ := range j.indexes {
				idx[k.idxName] = true
			}
			for k, _ := range ej.indexes {
				if _, ok := idx[k.idxName]; !ok {
					require.Fail(t, "missing index: "+k.idxName)
				}
			}
		case ControlJob:
			ej, ok := expected[i].(ControlJob)
			require.True(t, ok)
			require.Equal(t, ej.desc, j.desc)
		case AnalyzeJob:
			ej, ok := expected[i].(AnalyzeJob)
			require.True(t, ok)
			require.Equal(t, ej.tables, j.tables)
			require.Equal(t, ej.sqlDb, j.sqlDb)
		}
	}

	// expect queue to fit all jobs, otherwise this deadlocks
	// since we stopped accepting before running this, it should just roundtrip
	// to/from the same buf
	for _, j := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
			sc.Jobs <- j
		}
	}
}

func waitOnJob(wg *sync.WaitGroup, done chan struct{}) {
	wg.Add(1)
	go func() {
		select {
		case <-context.Background().Done():
			return
		case <-done:
			wg.Add(-1)
		}
	}()
}

func doGcCycle(t *testing.T, ctx *sql.Context, sc *StatsCoord) {
	sc.disableGc.Store(false)
	sc.doGc.Store(true)
	defer sc.disableGc.Store(true)

	wg := sync.WaitGroup{}
	runAndPause(ctx, sc, &wg) // do GC
	runAndPause(ctx, sc, &wg) // pick up finish GC job

	select {
	case <-sc.gcDone:
		break
	default:
		require.Fail(t, "failed to finish GC")
	}

	sc.gcMu.Lock()
	defer sc.gcMu.Unlock()
	require.False(t, sc.doGc.Load())
	require.False(t, sc.activeGc.Load())
	if sc.gcCancel != nil {
		t.Errorf("gc cancel non-nil")
	}
}

func runAndPause(ctx *sql.Context, sc *StatsCoord, wg *sync.WaitGroup) {
	// The stop job closes the controller's done channel before the job
	// is finished. The done channel is closed before the next run loop,
	// making the loop effectively inactive even if the goroutine is still
	// in the process of closing by the time we are flushing/validating
	// the queue.
	pauseDone := sc.Control("pause", func(sc *StatsCoord) error {
		sc.Stop()
		return nil
	})
	waitOnJob(wg, pauseDone)
	sc.Restart(ctx)
	wg.Wait()
	return
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

func newTestEngine(ctx context.Context, dEnv *env.DoltEnv) (*gms.Engine, *sql.Context) {
	pro, err := sqle.NewDoltDatabaseProviderWithDatabases("main", dEnv.FS, nil, nil)
	if err != nil {
		panic(err)
	}

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	if err != nil {
		panic(err)
	}

	doltSession, err := dsess.NewDoltSession(sql.NewBaseSession(), pro, dEnv.Config.WriteableConfig(), branch_control.CreateDefaultController(ctx), nil, writer.NewWriteSession)
	if err != nil {
		panic(err)
	}

	sqlCtx := sql.NewContext(ctx, sql.WithSession(doltSession))
	sqlCtx.SetCurrentDatabase(mrEnv.GetFirstDatabase())

	return gms.New(analyzer.NewBuilder(pro).Build(), &gms.Config{
		IsReadOnly:     false,
		IsServerLocked: false,
	}), sqlCtx
}
