package statspro

import (
	"context"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"io"
	"sync"
	"testing"
)

func TestScheduler(t *testing.T) {
	// setup for channel and background control
	ctx := sql.NewEmptyContext()
	sc := NewStatsCoord(0, ctx.GetLogger().Logger)

	//setup db
	dEnv := dtestutils.CreateTestEnv()

	sqlEng, _, err := engine.NewSqlEngineForEnv(context.Background(), dEnv)
	require.NoError(t, err)

	require.NoError(t, executeQuery(ctx, sqlEng, "create table xy (x int primary key, y int, key (y,x)"))
	require.NoError(t, executeQuery(ctx, sqlEng, "insert into xy values (0,0), (1,0), (2,0), (3,0), (4,1)"))

	startDbs := sqlEng.Databases(ctx)
	wg := sync.WaitGroup{}

	for _, db := range startDbs {
		if sqlDb, ok := db.(sqle.Database); ok {
			done := sc.Seed(ctx, sqlDb)
			waitOnJob(&wg, done)
		}
	}

	validateJobState(t, ctx, sc, []StatsJob{
		SeedDbTablesJob{sqlDb: startDbs[0], tables: []string{"xy"}},
	})

	// run the seed job and then stop
	sc.Start()
	wg.Wait()
	sc.Stop()

	validateJobState(t, ctx, sc, []StatsJob{
		ReadJob{db: startDbs[0], branch: "main", table: "xy"},
		ReadJob{db: startDbs[0], branch: "main", table: "xy"},
		FinalizeJob{indexes: nil},
	})

	// run the read/finalize jobs then stop
	sc.Start()
	wg.Wait()
	sc.Stop()

	validateJobState(t, ctx, sc, []StatsJob{
		SeedDbTablesJob{sqlDb: startDbs[0], tables: []string{"xy"}},
	})

	// bucket cache has 2 new buckets
	require.Equal(t, 2, len(sc.BucketCache))
	// stats state has two new indexes
	require.Equal(t, 2, len(sc.StatsState))
	for _, hist := range sc.StatsState {
		// each hist has one bucket
		require.Equal(t, 1, len(hist))
	}
}

// validateJobs compares the current event loop and launches a background thread
// that will repopulate the queue in-order
func validateJobState(t *testing.T, ctx context.Context, sc *StatsCoord, expected []StatsJob) {
	jobs, err := sc.flushQueue(ctx)
	require.NoError(t, err)

	require.Len(t, jobs, len(expected))
	for i, j := range jobs {
		// todo more specific equality comparison
		require.Equal(t, expected[i], j)
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

func executeQuery(ctx *sql.Context, eng *engine.SqlEngine, query string) error {
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
