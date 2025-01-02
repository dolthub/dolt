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
	"sync"
	"testing"
	"time"
)

func TestScheduler(t *testing.T) {
	dEnv := dtestutils.CreateTestEnv()

	sqlEng, ctx := newTestEngine(context.Background(), dEnv)

	require.NoError(t, executeQuery(ctx, sqlEng, "create database mydb"))
	require.NoError(t, executeQuery(ctx, sqlEng, "use mydb"))
	require.NoError(t, executeQuery(ctx, sqlEng, "create table xy (x int primary key, y int, key (y,x))"))
	require.NoError(t, executeQuery(ctx, sqlEng, "insert into xy values (0,0), (1,0), (2,0), (3,0), (4,1)"))

	sc := NewStatsCoord(time.Nanosecond, ctx.GetLogger().Logger)

	startDbs := sqlEng.Analyzer.Catalog.DbProvider.AllDatabases(ctx)
	wg := sync.WaitGroup{}

	var sqlDbs []sqle.Database

	for _, db := range startDbs {
		if sqlDb, ok := db.(sqle.Database); ok {
			br, err := sqlDb.DbData().Ddb.GetBranches(ctx)
			require.NoError(t, err)
			for _, b := range br {
				sqlDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, b.GetPath(), b.GetPath()+"/"+sqlDb.AliasedName())
				require.NoError(t, err)
				sqlDbs = append(sqlDbs, sqlDb.(sqle.Database))
				done := sc.Seed(ctx, sqlDb.(sqle.Database))
				waitOnJob(&wg, done)
			}
		}
	}

	validateJobState(t, ctx, sc, []StatsJob{
		// first job doesn't have tracked tables
		SeedDbTablesJob{sqlDb: sqlDbs[0], tables: nil},
	})

	// The stop job closes the controller's done channel before the job
	// is finished. The done channel is closed before the next run loop,
	// making the loop effectively inactive even if the goroutine is still
	// in the process of closing by the time we are flushing/validating
	// the queue.
	pauseDone := sc.Control("pause", func(sc *StatsCoord) error {
		sc.Stop()
		return nil
	})
	waitOnJob(&wg, pauseDone)
	sc.Start(ctx)
	wg.Wait()

	validateJobState(t, ctx, sc, []StatsJob{
		ReadJob{db: sqlDbs[0], table: "xy", nodes: []tree.Node{{}}, ordinals: []updateOrdinal{{0, 5}}},
		ReadJob{db: sqlDbs[0], table: "xy", nodes: []tree.Node{{}}, ordinals: []updateOrdinal{{0, 5}}},
		FinalizeJob{
			tableKey: tableIndexesKey{db: "mydb", branch: "main", table: "xy"},
			indexes: map[templateCacheKey][]hash.Hash{
				templateCacheKey{idxName: "PRIMARY"}: []hash.Hash{{}, {}},
				templateCacheKey{idxName: "y"}:       []hash.Hash{{}, {}},
			}},
		SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []string{"xy"}},
	})

	// run the read/finalize jobs then pause
	pauseDone = sc.Control("pause", func(sc *StatsCoord) error {
		sc.Stop()
		return nil
	})
	waitOnJob(&wg, pauseDone)
	sc.Start(ctx)
	wg.Wait()

	validateJobState(t, ctx, sc, []StatsJob{
		SeedDbTablesJob{sqlDb: sqlDbs[0], tables: []string{"xy"}},
	})

	require.Equal(t, 2, len(sc.BucketCache))
	require.Equal(t, 2, len(sc.LowerBoundCache))
	require.Equal(t, 2, len(sc.TemplateCache))
	require.Equal(t, 1, len(sc.Stats))
	for _, tableStats := range sc.Stats {
		require.Equal(t, 2, len(tableStats))
	}
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
			require.Equal(t, ej.tables, j.tables)
			require.Equal(t, ej.sqlDb.Name(), j.sqlDb.Name())
			require.Equal(t, ej.sqlDb.Revision(), j.sqlDb.Revision())
		case ReadJob:
			ej, ok := expected[i].(ReadJob)
			require.True(t, ok)
			require.Equal(t, ej.table, j.table)
			require.Equal(t, ej.ordinals, j.ordinals)
			require.Equal(t, len(ej.nodes), len(j.nodes))
			require.Equal(t, ej.db.Name(), j.db.Name())
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
