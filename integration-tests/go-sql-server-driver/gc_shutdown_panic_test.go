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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// TestGCShutdownPanic reproduces a reported failure where a `dolt_gc()` that is
// finalizing when the sql-server process is shut down (NomsBlockStore.Close())
// leaves the on-disk store in an inconsistent state instead of shutting down
// cleanly.
//
// The observed failure mode is store corruption: GC updates the manifest to a
// new root but the chunks backing that root are not durably persisted before
// the process goes away (or the old table files are removed too early). The
// next time the server starts and tries to load the database it fails with
//
//	root hash doesn't exist: <hash>
//
// and exits non-zero, because the manifest's root chunk cannot be found in the
// table files. (A crash during Close() itself, i.e. a panic, is the other
// possible symptom of the same race.)
//
// The test spins up a single sql-server hosting several databases, drives
// continual writes (with commits, to produce garbage) against every database,
// runs serial `call dolt_gc()` invocations against every database in a tight
// loop, and periodically restarts the sql-server process. Each restart is a
// SIGTERM-driven graceful shutdown; Restart()/GracefulStop() surfaces the error
// from cmd.Wait() on the stopped process, so a non-zero exit (whether the
// corrupt process failed on the way down or the freshly-started process failed
// to load the corrupted store) fails the test. We additionally scan the merged
// server output for the corruption message and for panic stack traces so that a
// failure produces an actionable diagnostic.
//
// This is a stress/race reproduction: a clean run does not prove the bug is
// absent, but a failing run demonstrates it. Set
// DOLT_GC_SHUTDOWN_PANIC_STRESS to a Go duration (e.g. "5m") to run each
// variant longer when hunting for the race locally.
func TestGCShutdownPanic(t *testing.T) {
	t.Parallel()

	duration := 25 * time.Second
	if v := os.Getenv("DOLT_GC_SHUTDOWN_PANIC_STRESS"); v != "" {
		d, err := time.ParseDuration(v)
		require.NoError(t, err)
		duration = d
	}

	// The safepoint controller governs how in-flight sessions are handled
	// while GC runs; the shutdown-vs-finalize race is independent of it, but
	// the two implementations reach NomsBlockStore.Close() along different
	// paths, so we exercise both.
	for _, controller := range []string{"session_aware", "kill_connections"} {
		t.Run(controller, func(t *testing.T) {
			t.Parallel()
			gcShutdownPanicTest{
				numDatabases:    3,
				writersPerDB:    2,
				duration:        duration,
				minRestartWait:  150 * time.Millisecond,
				maxRestartWait:  600 * time.Millisecond,
				safepointChoice: controller,
			}.run(t)
		})
	}
}

type gcShutdownPanicTest struct {
	numDatabases    int
	writersPerDB    int
	duration        time.Duration
	minRestartWait  time.Duration
	maxRestartWait  time.Duration
	safepointChoice string
}

func (gct gcShutdownPanicTest) dbNames() []string {
	names := make([]string, gct.numDatabases)
	for i := range names {
		names[i] = fmt.Sprintf("gcdb%d", i)
	}
	return names
}

func (gct gcShutdownPanicTest) run(t *testing.T) {
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)

	// Create each database as its own dolt repo under a single repo store.
	// The sql-server started on the repo store hosts them all as sibling
	// databases.
	for _, name := range gct.dbNames() {
		_, err := rs.MakeRepo(name)
		require.NoError(t, err)
	}

	// Watch the server output for evidence that a GC left the store on-disk in
	// a bad state. The signature is a (re)started server that cannot find the
	// manifest's root chunk in the table files ("root hash doesn't exist"), or a
	// GC that fails while dropping the journal file. Either indicates the store
	// was corrupted by an interrupted GC finalize, so we fail the test.
	//
	// A hard process crash (e.g. the FatalBehaviorCrash panic that used to fire
	// from dropJournalWriter) makes the process exit non-zero, which is caught
	// separately by asserting on the Restart/GracefulStop error, so we do not
	// scan for it here.
	var sawCorruption atomic.Bool
	var corruptionLine atomic.Value
	corruptionLine.Store("")
	// Recovered per-query panics (go-mysql-server catches these and logs
	// "caught panic") do not crash the process or corrupt the store, so they are
	// not what this test targets. We count them for diagnostics only.
	var recoveredPanics atomic.Int32
	var firstRecoveredPanic atomic.Value
	firstRecoveredPanic.Store("")
	visitor := func(line string) {
		if strings.Contains(line, "root hash doesn't exist") ||
			strings.Contains(line, "error dropping journal writer") {
			if sawCorruption.CompareAndSwap(false, true) {
				corruptionLine.Store(line)
			}
		}
		if strings.Contains(line, "caught panic") {
			if recoveredPanics.Add(1) == 1 {
				firstRecoveredPanic.Store(line)
			}
		}
	}

	srvSettings := &driver.Server{
		Args:        []string{"-P", `{{get_port "server_port"}}`},
		DynamicPort: "server_port",
		Envs:        []string{"DOLT_GC_SAFEPOINT_CONTROLLER_CHOICE=" + gct.safepointChoice},
	}
	server := MakeServer(t, rs, srvSettings, &ports, driver.WithOutputVisitor(visitor))
	require.NotNil(t, server)

	// Open a connection pool per database. Because the server is repeatedly
	// restarted on the same port, these pools transparently reconnect; we
	// disable idle connection reuse so we don't hand out connections that were
	// severed by a restart.
	pass, err := driver.Connection{User: "root"}.Password()
	require.NoError(t, err)
	dbs := make(map[string]*sql.DB, gct.numDatabases)
	for _, name := range gct.dbNames() {
		db, err := driver.ConnectDB("root", pass, name, "127.0.0.1", server.Port, nil)
		require.NoError(t, err)
		db.SetMaxIdleConns(0)
		dbs[name] = db
		t.Cleanup(func() {
			db.Close()
		})
	}

	// Bootstrap a table with some rows in every database.
	for _, name := range gct.dbNames() {
		gct.createTable(t, dbs[name])
	}

	// Workload goroutines run until their context is cancelled. They swallow
	// all query errors: connections are expected to break on every restart,
	// and only the process exit status is a signal of the bug. They must never
	// fail the test off the main goroutine.
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, egCtx := errgroup.WithContext(baseCtx)

	for _, name := range gct.dbNames() {
		db := dbs[name]
		for w := 0; w < gct.writersPerDB; w++ {
			row := w
			eg.Go(func() error {
				gct.writeLoop(egCtx, db, row)
				return nil
			})
		}
	}

	// One goroutine issuing serial dolt_gc() calls, cycling across databases.
	eg.Go(func() error {
		for egCtx.Err() == nil {
			for _, name := range gct.dbNames() {
				if egCtx.Err() != nil {
					break
				}
				gct.gcOnce(egCtx, dbs[name])
			}
		}
		return nil
	})

	// Main loop: periodically restart the server. Restart() gracefully stops
	// the currently running process (SIGTERM + wait) and returns the error from
	// cmd.Wait() on that process. A clean shutdown exits 0 (nil error); a panic
	// on the way down exits non-zero and shows up here.
	restarts := 0
	start := time.Now()
	for time.Since(start) < gct.duration {
		gct.sleepJitter(egCtx)
		if egCtx.Err() != nil {
			break
		}
		err := server.Restart(nil, nil)
		restarts++
		require.NoErrorf(t, err, "server exited non-zero across restart #%d (corruption log line: %q)",
			restarts, corruptionLine.Load())
		require.Falsef(t, sawCorruption.Load(), "server logged store corruption across restart #%d: %q",
			restarts, corruptionLine.Load())
		// Wait for the freshly-started process to come up before doing anything
		// else, so that it has installed its signal handler and a subsequent
		// SIGTERM exercises a graceful shutdown rather than hard-killing a
		// still-booting process.
		gct.waitReady(egCtx, dbs[gct.dbNames()[0]])
	}

	// Stop the workload and drain the goroutines before the final shutdown so
	// the last GracefulStop (run by MakeServer's cleanup) is asserted cleanly.
	cancel()
	require.NoError(t, eg.Wait())

	// Explicitly perform a final graceful stop while asserting a clean exit,
	// rather than relying solely on the cleanup's assertion.
	err = server.GracefulStop()
	require.NoErrorf(t, err, "server exited non-zero on final shutdown (corruption log line: %q)", corruptionLine.Load())
	require.Falsef(t, sawCorruption.Load(), "server logged store corruption: %q", corruptionLine.Load())

	if n := recoveredPanics.Load(); n > 0 {
		// These are recovered by the server (no crash, no corruption) and are
		// out of scope for this test, but worth surfacing. The first was:
		// firstRecoveredPanic.
		t.Logf("note: server logged %d recovered per-query panic(s) during the run (first: %q)",
			n, firstRecoveredPanic.Load())
	}
	t.Logf("completed %d restarts over %v with no shutdown corruption", restarts, time.Since(start).Round(time.Millisecond))
}

func (gct gcShutdownPanicTest) createTable(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "create table vals (id int primary key, val int)")
	require.NoError(t, err)
	vals := make([]string, gct.writersPerDB)
	for i := range vals {
		vals[i] = fmt.Sprintf("(%d,0)", i)
	}
	_, err = conn.ExecContext(ctx, "insert into vals values "+strings.Join(vals, ","))
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "call dolt_commit('-Am', 'create vals table')")
	require.NoError(t, err)
}

// writeLoop performs update+commit against a single row until ctx is cancelled.
// All errors are ignored: restarts routinely sever connections and GC with the
// kill_connections controller intentionally kills sessions.
func (gct gcShutdownPanicTest) writeLoop(ctx context.Context, db *sql.DB, row int) {
	for ctx.Err() == nil {
		func() {
			conn, err := db.Conn(ctx)
			if err != nil {
				return
			}
			defer conn.Close()
			_, err = conn.ExecContext(ctx, "update vals set val = val+1 where id = ?", row)
			if err != nil {
				return
			}
			_, _ = conn.ExecContext(ctx, fmt.Sprintf("call dolt_commit('-am', 'increment id %d')", row))
		}()
	}
}

// gcOnce runs a single dolt_gc() against db, ignoring all errors.
func (gct gcShutdownPanicTest) gcOnce(ctx context.Context, db *sql.DB) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	_, _ = conn.ExecContext(ctx, "call dolt_gc()")
}

// waitReady pings db until the server responds or ctx is cancelled, so callers
// can be sure a just-(re)started server is fully up.
func (gct gcShutdownPanicTest) waitReady(ctx context.Context, db *sql.DB) {
	for i := 0; i < 200 && ctx.Err() == nil; i++ {
		pingCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (gct gcShutdownPanicTest) sleepJitter(ctx context.Context) {
	span := gct.maxRestartWait - gct.minRestartWait
	wait := gct.minRestartWait
	if span > 0 {
		wait += time.Duration(rand.Int64N(int64(span)))
	}
	select {
	case <-ctx.Done():
	case <-time.After(wait):
	}
}
