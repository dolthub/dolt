// Copyright 2024 Dolthub, Inc.
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

package sqle

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
)

func setGlobalSqlVariable(t *testing.T, name string, val interface{}) {
	ctx := sql.NewEmptyContext()
	_, cur, _ := sql.SystemVariables.GetGlobal(name)
	t.Cleanup(func() {
		sql.SystemVariables.SetGlobal(ctx, name, cur)
	})
	sql.SystemVariables.SetGlobal(ctx, name, val)
}

func TestDatabaseProvider(t *testing.T) {
	setup := func(t *testing.T) (*sqle.Engine, *sql.Context, *DoltDatabaseProvider) {
		ctx := context.Background()
		dEnv := dtestutils.CreateTestEnv()

		db, err := NewDatabase(context.Background(), "dolt", dEnv.DbData(ctx), editor.Options{})
		require.NoError(t, err)

		engine, sqlCtx, err := NewTestEngine(dEnv, context.Background(), db)
		require.NoError(t, err)

		sess := dsess.DSessFromSess(sqlCtx.Session)
		pro := sess.Provider().(*DoltDatabaseProvider)

		ctxF := func(ctx context.Context) (*sql.Context, error) {
			config, _ := dEnv.Config.GetConfig(env.GlobalConfig)
			sqlCtx := NewTestSQLCtxWithProvider(ctx, pro, config, nil, sess.GCSafepointController())
			sqlCtx.SetCurrentDatabase(db.Name())
			return sqlCtx, nil
		}

		bThreads := sql.NewBackgroundThreads()
		t.Cleanup(func() {
			bThreads.Shutdown()
		})

		pro.InstallReplicationInitDatabaseHook(bThreads, ctxF)
		pro.AddInitDatabaseHook(InstallSnoopingCommitHook)
		return engine, sqlCtx, pro
	}
	t.Run("ReplicationConfig", func(t *testing.T) {
		t.Run("CreateDatabase", func(t *testing.T) {
			t.Run("NoReplication", func(t *testing.T) {
				engine, sqlCtx, pro := setup(t)

				err := ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE mytest;")
				require.NoError(t, err)

				sqlDb, err := pro.Database(sqlCtx, "mytest")
				require.NoError(t, err)
				ddbs := sqlDb.(Database).DoltDatabases()
				require.Len(t, ddbs, 1)
				hooks := doltdb.ExposeDatabaseFromDoltDB(ddbs[0]).(interface {
					PostCommitHooks() []doltdb.CommitHook
				}).PostCommitHooks()
				assert.Len(t, hooks, 1)
				_, ok := hooks[0].(*snoopingCommitHook)
				assert.True(t, ok, "expect hook to be PushOnWriteHook, it is %T", hooks[0])
			})
			t.Run("PushOnWriteReplication", func(t *testing.T) {
				setGlobalSqlVariable(t, dsess.ReplicateToRemote, "fileremote")
				setGlobalSqlVariable(t, dsess.ReplicationRemoteURLTemplate, "mem://remote_{database}")
				engine, sqlCtx, pro := setup(t)

				err := ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE mytest;")
				require.NoError(t, err)

				sqlDb, err := pro.Database(sqlCtx, "mytest")
				require.NoError(t, err)
				ddbs := sqlDb.(Database).DoltDatabases()
				require.Len(t, ddbs, 1)
				hooks := doltdb.ExposeDatabaseFromDoltDB(ddbs[0]).(interface {
					PostCommitHooks() []doltdb.CommitHook
				}).PostCommitHooks()
				require.Len(t, hooks, 2)
				_, ok := hooks[0].(*snoopingCommitHook)
				assert.True(t, ok, "expect hook to be snoopingCommitHook, it is %T", hooks[0])
				_, ok = hooks[1].(*DynamicPushOnWriteHook)
				assert.True(t, ok, "expect hook to be PushOnWriteHook, it is %T", hooks[1])
			})
			t.Run("AsyncPushOnWrite", func(t *testing.T) {
				setGlobalSqlVariable(t, dsess.ReplicateToRemote, "fileremote")
				setGlobalSqlVariable(t, dsess.ReplicationRemoteURLTemplate, "mem://remote_{database}")
				setGlobalSqlVariable(t, dsess.AsyncReplication, dsess.SysVarTrue)

				engine, sqlCtx, pro := setup(t)

				err := ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE mytest;")
				require.NoError(t, err)

				sqlDb, err := pro.Database(sqlCtx, "mytest")
				require.NoError(t, err)
				ddbs := sqlDb.(Database).DoltDatabases()
				require.Len(t, ddbs, 1)
				hooks := doltdb.ExposeDatabaseFromDoltDB(ddbs[0]).(interface {
					PostCommitHooks() []doltdb.CommitHook
				}).PostCommitHooks()
				require.Len(t, hooks, 2)
				_, ok := hooks[0].(*snoopingCommitHook)
				assert.True(t, ok, "expect hook to be snoopingCommitHook, it is %T", hooks[0])
				_, ok = hooks[1].(*DynamicPushOnWriteHook)
				assert.True(t, ok, "expect hook to be AsyncPushOnWriteHook, it is %T", hooks[1])
			})
		})
	})
}

type snoopingCommitHook struct {
}

func (*snoopingCommitHook) Execute(ctx context.Context, ds datas.Dataset, db *doltdb.DoltDB) (func(context.Context) error, error) {
	return nil, nil
}

func (*snoopingCommitHook) ExecuteForWorkingSets() bool {
	return true
}

func (*snoopingCommitHook) ExecuteForReplicaWrite() bool {
	return true
}

func InstallSnoopingCommitHook(ctx *sql.Context, pro *DoltDatabaseProvider, name string, dEnv *env.DoltEnv, db dsess.SqlDatabase) error {
	dEnv.DoltDB(ctx).PrependCommitHooks(ctx, &snoopingCommitHook{})
	return nil
}

// orphanCases are the on-disk remains an interrupted creation can leave behind: an in-progress marker,
// partial Dolt storage without a repo-state file, or a bare directory with no Dolt storage at all (an
// early-cancelled clone interrupted before .dolt was written).
var orphanCases = []struct {
	name       string
	makeOrphan func(t *testing.T, fs filesys.Filesys)
}{
	{"in-progress marker", func(t *testing.T, fs filesys.Filesys) {
		require.NoError(t, dbfactory.MarkDatabaseInProgress(fs))
	}},
	{"missing repo state", func(t *testing.T, fs filesys.Filesys) {
		require.NoError(t, fs.MkDirs(filepath.Join(dbfactory.DoltDir, dbfactory.DataDir)))
	}},
	{"no dolt storage", func(*testing.T, filesys.Filesys) {
		// providerWithOrphanedDir already created an empty directory at the name; leave it bare (no
		// .dolt), modelling a clone cancelled before any storage was written.
	}},
}

func newProviderEngine(t *testing.T) (*sqle.Engine, *sql.Context, *DoltDatabaseProvider, *env.DoltEnv) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	sess := dsess.DSessFromSess(sqlCtx.Session)
	return engine, sqlCtx, sess.Provider().(*DoltDatabaseProvider), dEnv
}

// providerWithOrphanedDir returns an engine whose filesystem holds a directory named foo that |makeOrphan|
// has turned into the remains of an interrupted creation.
func providerWithOrphanedDir(t *testing.T, makeOrphan func(t *testing.T, fs filesys.Filesys)) (*sqle.Engine, *sql.Context, *DoltDatabaseProvider) {
	engine, sqlCtx, pro, dEnv := newProviderEngine(t)

	require.NoError(t, dEnv.FS.MkDirs("foo"))
	fooFS, err := dEnv.FS.WithWorkingDir("foo")
	require.NoError(t, err)
	makeOrphan(t, fooFS)

	return engine, sqlCtx, pro
}

func TestCreateDatabaseOverIncompleteDirectory(t *testing.T) {
	// An interrupted create/clone leaves a directory that discovery ignores; a later CREATE must reclaim
	// it (quarantine to the dropped-database holding area) and succeed, rather than wedging on "database
	// exists" (murmur-zsyi [a]). IF NOT EXISTS behaves the same: the database genuinely does not exist yet.
	for _, tc := range orphanCases {
		for _, q := range []string{"CREATE DATABASE foo;", "CREATE DATABASE IF NOT EXISTS foo;"} {
			t.Run(tc.name+" / "+q, func(t *testing.T) {
				engine, sqlCtx, pro := providerWithOrphanedDir(t, tc.makeOrphan)

				require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, q))

				// foo is now a real, usable database...
				_, err := pro.Database(sqlCtx, "foo")
				require.NoError(t, err)

				// ...and the incomplete leftover was quarantined (recoverable), not deleted or left in place.
				dropped, err := pro.ListDroppedDatabases(sqlCtx)
				require.NoError(t, err)
				found, _ := hasCaseInsensitiveMatch(dropped, "foo")
				require.Truef(t, found, "incomplete dir should have been quarantined; dropped=%v", dropped)
			})
		}
	}
}

func TestCloneDatabaseOverIncompleteDirectory(t *testing.T) {
	// A retried clone must reclaim the incomplete leftover and get PAST the on-disk check to the remote,
	// rather than being stuck behind a directory it can neither use nor recreate (murmur-zsyi [a]).
	for _, tc := range orphanCases {
		t.Run(tc.name, func(t *testing.T) {
			_, sqlCtx, pro := providerWithOrphanedDir(t, tc.makeOrphan)

			// The remote is unreachable, so the clone still fails — but only AFTER quarantining the orphan
			// and getting past the directory check (it now reaches the remote instead of wedging on the dir).
			err := pro.CloneDatabaseFromRemote(sqlCtx, "foo", "", "origin", "file://unreachable", -1, nil)
			require.Error(t, err)

			dropped, derr := pro.ListDroppedDatabases(sqlCtx)
			require.NoError(t, derr)
			found, _ := hasCaseInsensitiveMatch(dropped, "foo")
			require.Truef(t, found, "incomplete dir should have been quarantined; dropped=%v", dropped)
		})
	}
}

// TestReserveQuarantinesIncompleteDirectory exercises the shared reserve gate directly (murmur-zsyi [a]):
// checkDatabaseNameAvailableLocked quarantines an incomplete on-disk leftover and reports the name
// available, a complete database directory still conflicts, and concurrent reservations of the same name
// over a leftover are serialized so exactly one wins.
func TestReserveQuarantinesIncompleteDirectory(t *testing.T) {
	for _, tc := range orphanCases {
		t.Run("incomplete leftover is reclaimed: "+tc.name, func(t *testing.T) {
			_, sqlCtx, pro := providerWithOrphanedDir(t, tc.makeOrphan)

			require.NoError(t, pro.reserveCreatingDatabase("foo"))
			pro.releaseCreatingDatabase("foo")

			// the leftover no longer squats on the live name...
			exists, _ := pro.fs.Exists("foo")
			require.False(t, exists, "incomplete dir must be moved out of the live path")

			// ...it was quarantined (recoverable), not deleted.
			dropped, err := pro.ListDroppedDatabases(sqlCtx)
			require.NoError(t, err)
			found, _ := hasCaseInsensitiveMatch(dropped, "foo")
			require.Truef(t, found, "incomplete dir should have been quarantined; dropped=%v", dropped)
		})
	}

	t.Run("a complete database directory still conflicts", func(t *testing.T) {
		engine, sqlCtx, pro, _ := newProviderEngine(t)
		require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE bar;"))

		// bar is a real, complete database — a clone/create over it must still conflict, never quarantine.
		require.Truef(t, sql.ErrDatabaseExists.Is(pro.reserveCreatingDatabase("bar")),
			"a complete database must not be quarantined")
	})

	t.Run("concurrent reservations over a leftover: exactly one wins", func(t *testing.T) {
		_, _, pro := providerWithOrphanedDir(t, orphanCases[0].makeOrphan)

		const n = 8
		var wg sync.WaitGroup
		errs := make([]error, n)
		wg.Add(n)
		for i := range n {
			go func(i int) {
				defer wg.Done()
				errs[i] = pro.reserveCreatingDatabase("foo")
			}(i)
		}
		wg.Wait()

		wins := 0
		for _, err := range errs {
			if err == nil {
				wins++
				continue
			}
			require.Truef(t, sql.ErrDatabaseExists.Is(err), "a losing reservation must get ErrDatabaseExists, got %v", err)
		}
		require.Equalf(t, 1, wins, "exactly one concurrent reservation may win; got %d", wins)
	})
}

func TestCreateDatabaseClearsInProgressMarker(t *testing.T) {
	// The collation case is covered separately because it does extra work after the marker is cleared.
	for _, tc := range []struct {
		name  string
		query string
	}{
		{"default", "CREATE DATABASE mytest;"},
		{"collation", "CREATE DATABASE mytest COLLATE utf8mb4_0900_bin;"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, sqlCtx, pro, dEnv := newProviderEngine(t)

			require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, tc.query))

			_, err := pro.Database(sqlCtx, "mytest")
			require.NoError(t, err)

			newFs, err := dEnv.FS.WithWorkingDir("mytest")
			require.NoError(t, err)
			assert.False(t, dbfactory.IsDatabaseInProgress(newFs), "a completed CREATE DATABASE must clear the marker")
		})
	}
}

// TestCreatingDatabaseReservation covers the name-reservation used by
// CloneDatabaseFromRemote to hold a database name while it fetches from a remote
// without the provider lock held. See the deadlock fix in database_provider.go.
func TestCreatingDatabaseReservation(t *testing.T) {
	setup := func(t *testing.T) (*sql.Context, *DoltDatabaseProvider) {
		ctx := context.Background()
		dEnv := dtestutils.CreateTestEnv()

		db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
		require.NoError(t, err)

		_, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
		require.NoError(t, err)

		sess := dsess.DSessFromSess(sqlCtx.Session)
		return sqlCtx, sess.Provider().(*DoltDatabaseProvider)
	}

	// checkNameAvailable runs checkDatabaseNameAvailableLocked under the provider
	// lock, mirroring how the create (checkDisk=true) and undrop
	// (checkDisk=false) paths consult it.
	checkNameAvailable := func(pro *DoltDatabaseProvider, name string, checkDisk bool) error {
		pro.mu.Lock()
		defer pro.mu.Unlock()
		return pro.checkDatabaseNameAvailableLocked(name, checkDisk)
	}

	t.Run("second reservation of the same name conflicts", func(t *testing.T) {
		_, pro := setup(t)
		require.NoError(t, pro.reserveCreatingDatabase("clonedb"))
		defer pro.releaseCreatingDatabase("clonedb")

		err := pro.reserveCreatingDatabase("clonedb")
		require.Truef(t, sql.ErrDatabaseExists.Is(err), "expected ErrDatabaseExists, got %v", err)
	})

	t.Run("reservation conflicts case-insensitively across create/clone/undrop", func(t *testing.T) {
		_, pro := setup(t)
		require.NoError(t, pro.reserveCreatingDatabase("CloneDB"))
		defer pro.releaseCreatingDatabase("CloneDB")

		for _, variant := range []string{"clonedb", "CLONEDB", "CloneDB"} {
			require.Truef(t, sql.ErrDatabaseExists.Is(pro.reserveCreatingDatabase(variant)),
				"clone of case-variant %q should conflict", variant)
			require.Truef(t, sql.ErrDatabaseExists.Is(checkNameAvailable(pro, variant, true)),
				"CREATE of case-variant %q should conflict", variant)
			require.Truef(t, sql.ErrDatabaseExists.Is(checkNameAvailable(pro, variant, false)),
				"UNDROP of case-variant %q should conflict", variant)
		}
	})

	t.Run("release frees the name case-insensitively", func(t *testing.T) {
		_, pro := setup(t)
		require.NoError(t, pro.reserveCreatingDatabase("clonedb"))
		// Releasing via a different case must clear the same reservation.
		pro.releaseCreatingDatabase("CLONEDB")
		require.NoError(t, checkNameAvailable(pro, "clonedb", true))
		require.NoError(t, pro.reserveCreatingDatabase("clonedb"))
		pro.releaseCreatingDatabase("clonedb")
	})

	t.Run("a deleting database also conflicts case-insensitively", func(t *testing.T) {
		_, pro := setup(t)
		pro.mu.Lock()
		pro.deletingDatabases[formatDbMapKeyName("delDB")] = struct{}{}
		pro.mu.Unlock()
		t.Cleanup(func() {
			pro.mu.Lock()
			delete(pro.deletingDatabases, formatDbMapKeyName("delDB"))
			pro.mu.Unlock()
		})

		require.Truef(t, sql.ErrDatabaseExists.Is(checkNameAvailable(pro, "DELDB", true)),
			"CREATE of a case-variant of a deleting database should conflict")
	})

	t.Run("reservation does not gate database enumeration", func(t *testing.T) {
		sqlCtx, pro := setup(t)
		require.NoError(t, pro.reserveCreatingDatabase("clonedb"))
		defer pro.releaseCreatingDatabase("clonedb")

		// AllDatabases must return promptly (a reserved-but-unregistered clone
		// must not block enumeration the way a deleting database does) and must
		// not expose the in-progress clone. The bounded wait turns a regression
		// (reservation gating enumeration) into a clean failure instead of a hang.
		done := make(chan []sql.Database, 1)
		go func() { done <- pro.AllDatabases(sqlCtx) }()
		select {
		case dbs := <-done:
			for _, db := range dbs {
				require.NotEqualf(t, "clonedb", strings.ToLower(db.Name()),
					"in-progress clone must not be visible in AllDatabases")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("AllDatabases blocked while a name was reserved for cloning; reservation must not gate enumeration")
		}
	})
}

// headCommit returns the commit at the head of the named branch.
func headCommit(ctx context.Context, t *testing.T, ddb *doltdb.DoltDB, branch string) *doltdb.Commit {
	t.Helper()
	cs, err := doltdb.NewCommitSpec(branch)
	require.NoError(t, err)
	optCmt, err := ddb.Resolve(ctx, cs, nil)
	require.NoError(t, err)
	commit, ok := optCmt.ToCommit()
	require.True(t, ok)
	return commit
}

func TestResolveCaseVariantBranchConflict(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11270
	engine, sqlCtx, _, dEnv := newProviderEngine(t)
	ctx := context.Background()
	ddb := dEnv.DoltDB(ctx)

	// mustQuery asserts no error occurs when running |q| and returns resulting rows.
	mustQuery := func(q string) []sql.Row {
		t.Helper()
		rows, err := QueryRows(sqlCtx, engine, q)
		require.NoError(t, err)
		return rows
	}

	mustQuery("create table t (a int primary key)")
	mustQuery("insert into t values (111)")
	mustQuery("call dolt_commit('-Am', 'lower')")
	mustQuery("update t set a = 222")
	mustQuery("call dolt_commit('-am', 'upper')")

	require.NoError(t, ddb.NewBranchAtCommitAllowCaseConflict(ctx, ref.NewBranchRef("br"), headCommit(ctx, t, ddb, "main~1"), nil))
	require.NoError(t, ddb.NewBranchAtCommitAllowCaseConflict(ctx, ref.NewBranchRef("BR"), headCommit(ctx, t, ddb, "main"), nil))

	// Each casing folds onto the branches above, making it ambiguous which branch to read.
	for _, db := range []string{"dolt/br", "dolt/BR", "dolt/Br"} {
		_, err := QueryRows(sqlCtx, engine, "select a from `"+db+"`.t")
		require.ErrorIs(t, err, doltdb.ErrAmbiguousRefName)
		require.ErrorContains(t, err, "could be BR, br")
	}

	mustQuery("call dolt_branch('-m', 'BR', 'keepBR')")

	require.Equal(t, []sql.Row{{int32(111)}}, mustQuery("select a from `dolt/br`.t"))
	require.Equal(t, []sql.Row{{int32(222)}}, mustQuery("select a from `dolt/keepBR`.t"))
}
