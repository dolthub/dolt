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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
)

// This test asserts the behavior of the fast-fail file-lock optimization for
// dolt CLI commands which run against a data dir while a `dolt sql-server` is
// running in that data dir.
//
// When opening a directory of databases, we don't want to wait for the
// lock-acquire timeout once we've failed to get one.
//
// This test creates many databases and runs a server in the data dir. It then
// spawns a CLI command in the data dir root and times it. The command must
// finish well under the un-optimized cost, which is ~N * 100ms.

const (
	// perDatabaseLockTimeout mirrors lockFileTimeout in nbs/journal.go
	perDatabaseLockTimeout = 100 * time.Millisecond

	// numReadOnlyDatabases is chosen large enough that the un-optimized,
	// serial lock-wait cost is unmistakably larger than the optimized cost
	numReadOnlyDatabases = 32

	// We will try up to this many times to see good behavior. If we have
	// lots of scheduling contention, we can still be slow enough that we
	// think we are waiting on the lock files but we are actually just
	// running slowly.
	maxTrials = 3
)

// makeEmptyDatabases creates n freshly initialized, empty databases as
// subdirectories of the data dir root |rs|. Empty databases still need
// locking, and so meet our use case.
func makeEmptyDatabases(t *testing.T, rs driver.RepoStore, n int) {
	for i := 0; i < n; i++ {
		_, err := rs.MakeRepo(fmt.Sprintf("db_%02d", i))
		require.NoError(t, err)
	}
}

// timeShowDatabases runs `dolt sql -q "show databases"` from the data dir root
// |rs| up to maxTrials times, returning the best time seen. If the time is
// ever < maxAcceptable, it returns that immediately.
func timeShowDatabases(t *testing.T, rs driver.RepoStore, maxAcceptable time.Duration) time.Duration {
	var best time.Duration
	for trial := 0; trial < maxTrials; trial++ {
		cmd := rs.DoltCmd("sql", "-q", "show databases")
		start := time.Now()
		out, err := cmd.CombinedOutput()
		elapsed := time.Since(start)
		require.NoError(t, err, "show databases failed, output:\n%s", string(out))
		require.Regexp(t, "db_00", string(out))
		if trial == 0 || elapsed < best {
			best = elapsed
		}
		if best < maxAcceptable {
			// We take the very first run. Serially waiting for
			// the timeouts would have definitely taken longer
			// than this.
			return best
		}
	}
	return best
}

// TestReadOnlyDatabaseLoadSkipsLockTimeout asserts that loading many read-only
// databases behind a running sql-server does not serially pay the file-lock
// timeout for every database.
func TestReadOnlyDatabaseLoadSkipsLockTimeout(t *testing.T) {
	// No Parallel because it's just a bit sensitive to wall-clock time.
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	makeEmptyDatabases(t, rs, numReadOnlyDatabases)

	// Start a sql-server in the data dir root. It holds the file lock on every
	// database, forcing the CLI below to open them all read-only. The helper
	// blocks until the server is up and serving, so server startup time is not
	// included in the measurement below.
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t
	RunServerUntilEndOfTest(t, rs, &driver.Server{
		Args:        []string{"--port", `{{get_port "server"}}`},
		DynamicPort: "server",
	}, &ports)

	unoptimizedCost := numReadOnlyDatabases * perDatabaseLockTimeout
	maxAcceptable := unoptimizedCost / 2

	best := timeShowDatabases(t, rs, maxAcceptable)

	require.Lessf(t, best, maxAcceptable,
		"loading %d read-only databases behind a running sql-server took %s; "+
			"without the fast-fail lock optimization this would have taken about "+
			"%s (%d serial %s file-lock waits)",
		numReadOnlyDatabases, best, unoptimizedCost, numReadOnlyDatabases, perDatabaseLockTimeout)
}
