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

// After this, we expect a conjoin.
// This test is tightly coupled with /go/store/nbs/store.go and
// how its inline conjoiner is configured. This test could be
// robust against that if it knew when conjoin was running or
// could sniff log lines from when it started, but we don't do
// that currently.
//
// This is defaultMaxTables + 2 because starting a conjoin on a new
// add is checked before the add itself and needs to be strictly >
// defaultMaxTables. So there is one more file already in the manifest
// and one which is about to be.
const ExpectedTableFilesHighWaterMark = 258

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
	server := MakeServer(t, repo, srvSettings, &ports, driver.WithOutputVisitor(func(out string) {
		if strings.Contains(out, "beginning conjoin of database") {
			conjoinStarted.Store(true)
		}
		if strings.Contains(out, "conjoin completed successfully") {
			conjoinFinished.Store(true)
		}
	}))
	server.DBName = dbname

	oldgendir := filepath.Join(filepath.Join(rs.Dir, dbname), "/.dolt/noms/oldgen")

	hwm := GetOldGenTableFilesHighWaterMark(t, server, &conjoinStarted, oldgendir)
	t.Logf("found high water mark of %v files", hwm)
	require.Equal(t, ExpectedTableFilesHighWaterMark, hwm)
	require.Eventually(t, func() bool {
		return conjoinFinished.Load()
	}, 5 * time.Second, 32 * time.Millisecond)

	CreateToJustBelowHighWaterMark(t, server, oldgendir, hwm)
	cnt := CountTableFiles(t, oldgendir)
	t.Logf("now there are %d", cnt)

	RunGCFull(t, server, oldgendir)

	require.NoError(t, server.GracefulStop())
	output := server.Output.String()
	assert.Equal(t, 1, strings.Count(output, "beginning conjoin of database"))
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

func GetOldGenTableFilesHighWaterMark(t *testing.T, srv *driver.SqlServer, conjoinStarted *atomic.Bool, path string) int {
	var hwm int
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
		cnt := CountTableFiles(t, path)
		if cnt > hwm {
			hwm = cnt
		} else if cnt < hwm {
			return hwm
		}
		if conjoinStarted.Load() {
			return hwm
		}
	}
}

func CreateToJustBelowHighWaterMark(t *testing.T, srv *driver.SqlServer, path string, hwm int) {
	ctx := t.Context()
	db, err := srv.DB(driver.Connection{User: "root"})
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	cnt := CountTableFiles(t, path)
	for range (hwm-cnt-1) {
		_, err = conn.ExecContext(ctx, "CALL DOLT_COMMIT('-A', '--allow-empty', '-m', 'creating a new commit')")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CALL DOLT_GC()")
		require.NoError(t, err)
	}
	cnt = CountTableFiles(t, path)
	require.Equal(t, hwm-1, cnt)
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
