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
	"io/fs"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"
	"github.com/dolthub/dolt/go/store/hash"
)

// This test asserts that running a full gc into the old gen at the
// point that it is at its conjoin limit will not generate a conjoin
// in the middle of the GC.
//
// It first generates empty commits and runs GC in a loop. After every
// GC, it checks how many table files are in the oldgen. When it sees
// them get conjoined, it assumes max table files is the observed high
// water mark.
//
// It then creates exactly that many table files in oldgen by creating
// empty commits and running gc however many times it needs to.
//
// Then it needs to create one more commit and run a
// dolt_gc(--full). Currently dolt sql-server logs whenever it beings
// a conjoin, and that determination is made synchronously on the
// write path. So, no real attempt is made to get a theoretically
// started conjoin to win the race against the newgen collection, for
// example.
//
// As described above, this test is tightly coupled with
// GenerationalNBS, NomsBlockStore and the file persisters, with
// conjoin strategy behavior.
//
// At the end of the test, there should be one file in the oldgen, the
// results of the --full. There should be exactly one "beginning
// conjoin of database" in the server logs and it should correspond to
// when we measured the high water mark. After shutting down the
// server should happily start again.
func TestFullGCNoOldgenConjoin(t *testing.T) {
	t.Parallel()
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t
	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	dbname := "full_gc_no_oldgen_conjoin_test"

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	repo, err := rs.MakeRepo(dbname)
	require.NoError(t, err)
	srvSettings := &driver.Server{
		Args:        []string{"--port", `{{get_port "server"}}`},
		DynamicPort: "server",
	}
	var conjoinStarted, conjoinFinished atomic.Bool
	upstreamLenOnConjoin := 0
	server := MakeServer(t, repo, srvSettings, &ports, driver.WithOutputVisitor(func(out string) {
		// Matching a line like:
		// time="2026-03-28T11:57:31-07:00" level=info msg="beginning conjoin of database" database=full_gc_no_oldgen_conjoin_test generation=old pkg=store.noms upstream_len=257
		// Extracting its upstream_len to see exactly when the conjoin was triggered.
		// This line is logged as part of the conjoin strategy deciding to do the conjoin,
		// and so reflects the state of the tableSet which triggers the conjoin.
		if upstreamLenOnConjoin <= 0 && strings.Contains(out, "beginning conjoin of database") {
			if i := strings.Index(out, "upstream_len="); i != -1 {
				i += len("upstream_len=")
				if _, err := fmt.Sscanf(out[i:], "%d", &upstreamLenOnConjoin); err != nil {
					upstreamLenOnConjoin = -1
				}
			}
			conjoinStarted.Store(true)
		}
		// Matching the line:
		// time="2026-03-28T11:57:32-07:00" level=info msg="conjoin completed successfully" database=full_gc_no_oldgen_conjoin_test generation=old new_upstream_len=2 pkg=store.noms
		// So we can block on further operations until conjoin is done.
		if strings.Contains(out, "conjoin completed successfully") {
			conjoinFinished.Store(true)
		}
	}))
	server.DBName = dbname

	oldgendir := filepath.Join(repo.Dir, "/.dolt/noms/oldgen")

	CommitAndGCUntilConjoin(t, server, &conjoinStarted, oldgendir)
	require.Greater(t, upstreamLenOnConjoin, 0)
	require.Eventually(t, func() bool {
		return conjoinFinished.Load()
	}, 5 * time.Second, 32 * time.Millisecond)

	CreateUpToNumFiles(t, server, oldgendir, upstreamLenOnConjoin)
	cnt := CountTableFiles(t, oldgendir)
	t.Logf("now there are %d", cnt)

	RunGCFull(t, server, oldgendir)

	require.NoError(t, server.GracefulStop())
	output := server.Output.String()
	assert.Equal(t, 1, strings.Count(output, "beginning conjoin of database"))
	// The line for triggering a conjoin on policy but not proceeding because
	// conjoin is dynamically disabled looks like:
	// time="2026-03-28T12:01:48-07:00" level=info msg="conjoin dynamically disabled. not conjoining." database=full_gc_no_oldgen_conjoin_test generation=old pkg=store.noms
	assert.Equal(t, 1, strings.Count(output, "conjoin dynamically disabled"))
	cnt = CountTableFiles(t, oldgendir)
	assert.Equal(t, 1, cnt)

	newServer := MakeServer(t, repo, srvSettings, &ports)
	newServer.DBName = dbname
	db, err := newServer.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.PingContext(t.Context()))
}

func CountTableFiles(t *testing.T, dir string) int {
	var count int
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		n := d.Name()
		n = strings.TrimSuffix(n, ".darc")
		_, ok := hash.MaybeParse(n)
		if ok {
			count += 1
		}
		return nil
	})
	require.NoError(t, err)
	return count
}

func CommitAndGCUntilConjoin(t *testing.T, srv *driver.SqlServer, conjoinStarted *atomic.Bool, path string) {
	ctx := t.Context()
	db, err := srv.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	for {
		_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-A', '--allow-empty', '-m', 'creating a new commit')")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CALL DOLT_GC()")
		require.NoError(t, err)
		if conjoinStarted.Load() {
			return
		}
	}
}

func CreateUpToNumFiles(t *testing.T, srv *driver.SqlServer, path string, numFiles int) {
	ctx := t.Context()
	db, err := srv.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	for {
		cnt := CountTableFiles(t, path)
		if cnt == numFiles {
			return
		}
		_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-A', '--allow-empty', '-m', 'creating a new commit')")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CALL DOLT_GC()")
		require.NoError(t, err)
	}
}

func RunGCFull(t *testing.T, srv *driver.SqlServer, path string) {
	ctx := t.Context()
	db, err := srv.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	_ = CountTableFiles(t, path)
	_, err = conn.ExecContext(ctx, "CALL DOLT_GC('--full')")
	cnt := CountTableFiles(t, path)
	require.Equal(t, 1, cnt)
}
