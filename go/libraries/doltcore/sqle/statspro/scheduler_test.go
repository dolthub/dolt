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
		xyIns := strings.Builder{}
		abIns.WriteString("insert into ab values")
		xyIns.WriteString("insert into xy values")
		for i := range 200 {
			if i > 0 {
				abIns.WriteString(", ")
				xyIns.WriteString(", ")
			}
			abIns.WriteString(fmt.Sprintf("(%d, '%s')", i, b))
			xyIns.WriteString(fmt.Sprintf("(%d, %d)", i+5, i%25))
		}
		require.NoError(t, executeQuery(ctx, sqlEng, abIns.String()))
		require.NoError(t, executeQuery(ctx, sqlEng, xyIns.String()))

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
				indexes: map[templateCacheKey][]hash.Hash{
					templateCacheKey{idxName: "PRIMARY"}: nil,
					templateCacheKey{idxName: "b"}:       nil,
				}},
			ReadJob{db: sqlDbs[0], table: "xy", nodes: []tree.Node{{}}, ordinals: []updateOrdinal{{0, 205}}},
			ReadJob{db: sqlDbs[0], table: "xy", nodes: []tree.Node{{}}, ordinals: []updateOrdinal{{0, 205}}},
			FinalizeJob{
				tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "xy"},
				indexes: map[templateCacheKey][]hash.Hash{
					templateCacheKey{idxName: "PRIMARY"}: nil,
					templateCacheKey{idxName: "y"}:       nil,
				}},
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "ab"}, {name: "xy"}}},
		})

		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "ab"}, {name: "xy"}}},
		})

		// 2 old + 2 new xy + 7 new ab
		require.Equal(t, 11, len(sc.BucketCache))
		require.Equal(t, 4, len(sc.LowerBoundCache))
		require.Equal(t, 4, len(sc.TemplateCache))
		require.Equal(t, 2, len(sc.Stats))
		for _, tableStats := range sc.Stats {
			require.Equal(t, 2, len(tableStats))
		}
	}
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
			indexes: map[templateCacheKey][]hash.Hash{
				templateCacheKey{idxName: "PRIMARY"}: nil,
				templateCacheKey{idxName: "y"}:       nil,
			}},
	})

	runAndPause(ctx, sc, &wg)
	validateJobState(t, ctx, sc, []StatsJob{})
	require.Equal(t, 6, len(sc.BucketCache))
	require.Equal(t, 4, len(sc.LowerBoundCache))
	require.Equal(t, 2, len(sc.TemplateCache))
	require.Equal(t, 1, len(sc.Stats))
	for _, tableStats := range sc.Stats {
		require.Equal(t, 2, len(tableStats))
	}
}

func TestAlterIndex(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, sqlDbs := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	{
		// drop index
		// TODO detect schema change?
		require.NoError(t, executeQuery(ctx, sqlEng, "alter table xy modify column y varchar(200)"))

		// expect finalize, no GC
		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			FinalizeJob{
				tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "xy"},
				indexes: map[templateCacheKey][]hash.Hash{
					templateCacheKey{idxName: "PRIMARY"}: nil,
					templateCacheKey{idxName: "y"}:       nil,
				}},
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		// 2 old + 2 new xy
		require.Equal(t, 4, len(sc.BucketCache))
		require.Equal(t, 4, len(sc.LowerBoundCache))
		require.Equal(t, 4, len(sc.TemplateCache))
		require.Equal(t, 1, len(sc.Stats))
		for _, tableStats := range sc.Stats {
			require.Equal(t, 2, len(tableStats))
		}
	}
}

func TestDropIndex(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, sqlDbs := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	{
		// alter index
		// TODO detect schema change?
		// TODO disable GC?
		require.NoError(t, executeQuery(ctx, sqlEng, "alter table xy drop index y"))

		// finalize and GC
		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			FinalizeJob{
				tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "xy"},
				indexes: map[templateCacheKey][]hash.Hash{
					templateCacheKey{idxName: "PRIMARY"}: nil,
				}},
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		// 2 old + 2 new xy
		require.Equal(t, 2, len(sc.BucketCache))
		require.Equal(t, 2, len(sc.LowerBoundCache))
		require.Equal(t, 2, len(sc.TemplateCache))
		require.Equal(t, 1, len(sc.Stats))
		for _, tableStats := range sc.Stats {
			require.Equal(t, 1, len(tableStats))
		}
	}
}

func TestDropIndexGC(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, sqlDbs := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	{
		require.NoError(t, executeQuery(ctx, sqlEng, "alter table xy drop index y"))

		// finalize and GC
		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			FinalizeJob{
				tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "xy"},
				indexes: map[templateCacheKey][]hash.Hash{
					templateCacheKey{idxName: "PRIMARY"}: nil,
				}},
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
			GCJob{},
		})

		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []tableStatsInfo{{name: "xy"}}},
		})

		// 2 old + 2 new xy
		require.Equal(t, 1, len(sc.BucketCache))
		require.Equal(t, 1, len(sc.LowerBoundCache))
		require.Equal(t, 1, len(sc.TemplateCache))
		require.Equal(t, 1, len(sc.Stats))
		for _, tableStats := range sc.Stats {
			require.Equal(t, 1, len(tableStats))
		}
	}
}

func TestDropTable(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, sqlDbs := defaultSetup(t, threads)
	wg := sync.WaitGroup{}
	{
		sc.disableGc.Store(true)
		sc.gcInterval = time.Nanosecond
		// alter index
		// TODO detect schema change?
		require.NoError(t, executeQuery(ctx, sqlEng, "drop table xy"))
		runAndPause(ctx, sc, &wg)

		// no finalize, just GC
		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: nil},
		})

	}
}

func TestDropTableGC(t *testing.T) {
	threads := sql.NewBackgroundThreads()
	defer threads.Shutdown()
	ctx, sqlEng, sc, sqlDbs := defaultSetup(t, threads)
	wg := sync.WaitGroup{}

	{
		require.NoError(t, executeQuery(ctx, sqlEng, "drop table xy"))
		runAndPause(ctx, sc, &wg)

		// no finalize, just GC
		validateJobState(t, ctx, sc, []StatsJob{
			SeedDbTablesJob{sqlDb: sqlDbs[0], tables: nil},
		})

		// check for clean slate
		runAndPause(ctx, sc, &wg)

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

		require.Equal(t, 5, len(sc.BucketCache)) // +1 for new chunk
		require.Equal(t, 2, len(sc.LowerBoundCache))
		require.Equal(t, 3, len(sc.TemplateCache)) // +1 for schema change
		require.Equal(t, 1, len(sc.Stats))
		stat := sc.Stats[tableIndexesKey{db: "mydb", branch: "main", table: "xy"}]
		require.Equal(t, 2, len(stat[0].Hist))
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

		require.Equal(t, 5, len(sc.BucketCache))     // +1 rewrite partial chunk
		require.Equal(t, 3, len(sc.LowerBoundCache)) // +1 rewrite first chunk
		require.Equal(t, 3, len(sc.TemplateCache))
		require.Equal(t, 1, len(sc.Stats))
		stat := sc.Stats[tableIndexesKey{db: "mydb", branch: "main", table: "xy"}]
		require.Equal(t, 1, len(stat[0].Hist))
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

		require.Equal(t, 4, len(sc.BucketCache))
		require.Equal(t, 2, len(sc.LowerBoundCache))
		require.Equal(t, 3, len(sc.TemplateCache)) // +1 schema change
		require.Equal(t, 1, len(sc.Stats))
		stat := sc.Stats[tableIndexesKey{db: "mydb", branch: "main", table: "xy"}]
		require.Equal(t, 1, len(stat[0].Hist))
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
				indexes: map[templateCacheKey][]hash.Hash{
					templateCacheKey{idxName: "PRIMARY"}: nil,
				}},
			SeedDbTablesJob{sqlDb: otherDb, tables: []tableStatsInfo{{name: "t"}}},
		})

		runAndPause(ctx, sc, &wg)

		// xy and t
		require.Equal(t, 5, len(sc.BucketCache))
		require.Equal(t, 3, len(sc.LowerBoundCache))
		require.Equal(t, 3, len(sc.TemplateCache))
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

		sc.gcInterval = time.Nanosecond
		sc.SleepMult = time.Hour
		runAndPause(ctx, sc, &wg) // GC

		// test for cleanup
		require.Equal(t, 5, len(sc.BucketCache))
		require.Equal(t, 3, len(sc.LowerBoundCache))
		require.Equal(t, 3, len(sc.TemplateCache))
		require.Equal(t, 2, len(sc.Stats))
	}
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

func defaultSetup(t *testing.T, threads *sql.BackgroundThreads) (*sql.Context, *gms.Engine, *StatsCoord, []sqle.Database) {
	dEnv := dtestutils.CreateTestEnv()
	sqlEng, ctx := newTestEngine(context.Background(), dEnv)

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

	sc := NewStatsCoord(time.Nanosecond, ctx.GetLogger().Logger, threads)

	startDbs := sqlEng.Analyzer.Catalog.DbProvider.AllDatabases(ctx)
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

	{
		// seed creates read jobs
		runAndPause(ctx, sc, &wg)
		validateJobState(t, ctx, sc, []StatsJob{
			ReadJob{db: sqlDbs[0], table: "xy", ordinals: []updateOrdinal{{0, 415}, {415, 500}}},
			ReadJob{db: sqlDbs[0], table: "xy", ordinals: []updateOrdinal{{0, 240}, {240, 500}}},
			FinalizeJob{
				tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "xy"},
				indexes: map[templateCacheKey][]hash.Hash{
					templateCacheKey{idxName: "PRIMARY"}: nil,
					templateCacheKey{idxName: "y"}:       nil,
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

		require.Equal(t, 4, len(sc.BucketCache))
		require.Equal(t, 2, len(sc.LowerBoundCache))
		require.Equal(t, 2, len(sc.TemplateCache))
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

		require.Equal(t, 4, len(sc.BucketCache))
		require.Equal(t, 2, len(sc.LowerBoundCache))
		require.Equal(t, 2, len(sc.TemplateCache))
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

	require.Equal(t, len(expected), len(jobs))
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
			fmt.Println(j.indexes)
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
