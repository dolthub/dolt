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
	"errors"
	"path/filepath"
	"strings"
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
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
)

func setGlobalSqlVariable(t *testing.T, ctx *sql.Context, name string, val interface{}) {
	require.NoError(t, ctx.SystemVariables().SetGlobal(ctx, name, val))
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
			sqlCtx := NewTestSQLCtxWithProvider(ctx, pro, config, nil, sess.GCSafepointController(), engine.Variables())
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
				engine, sqlCtx, pro := setup(t)
				setGlobalSqlVariable(t, sqlCtx, dsess.ReplicateToRemote, "fileremote")
				setGlobalSqlVariable(t, sqlCtx, dsess.ReplicationRemoteURLTemplate, "mem://remote_{database}")

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
				engine, sqlCtx, pro := setup(t)
				setGlobalSqlVariable(t, sqlCtx, dsess.ReplicateToRemote, "fileremote")
				setGlobalSqlVariable(t, sqlCtx, dsess.ReplicationRemoteURLTemplate, "mem://remote_{database}")
				setGlobalSqlVariable(t, sqlCtx, dsess.AsyncReplication, dsess.SysVarTrue)

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

// orphanCases are the two on-disk remains an interrupted creation can leave behind.
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
	for _, tc := range orphanCases {
		t.Run(tc.name, func(t *testing.T) {
			engine, sqlCtx, _ := providerWithOrphanedDir(t, tc.makeOrphan)

			// IF NOT EXISTS must not be silently suppressed, because the database does not exist and a
			// client that believes it does cannot use it.
			for _, q := range []string{"CREATE DATABASE foo;", "CREATE DATABASE IF NOT EXISTS foo;"} {
				err := ExecuteSqlOnEngine(sqlCtx, engine, q)
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrIncompleteDatabaseDir, "query %q", q)
			}
		})
	}
}

func TestCloneDatabaseOverIncompleteDirectory(t *testing.T) {
	// A retried clone must not be stuck behind a directory it can neither use nor recreate.
	for _, tc := range orphanCases {
		t.Run(tc.name, func(t *testing.T) {
			_, sqlCtx, pro := providerWithOrphanedDir(t, tc.makeOrphan)

			// The orphaned directory is detected before any remote work, so the unreachable remote is never contacted.
			err := pro.CloneDatabaseFromRemote(sqlCtx, "foo", "", "origin", "file://unreachable", -1, nil)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrIncompleteDatabaseDir)
		})
	}
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

// TestCreateDatabaseFailureLeavesNothingBehind covers the whole of what a failed CREATE DATABASE must undo: the
// directory it made, the in-progress marker inside it, and the DoltDB it opened. Only a process that dies
// outright should leave any of that on disk.
func TestCreateDatabaseFailureLeavesNothingBehind(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnvForLocalFilesystem()
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	pro := dsess.DSessFromSess(sqlCtx.Session).Provider().(*DoltDatabaseProvider)

	failCreate := true
	pro.AddInitDatabaseHook(func(_ *sql.Context, _ *DoltDatabaseProvider, _ string, _ *env.DoltEnv, _ dsess.SqlDatabase) error {
		if failCreate {
			return errors.New("there was an error initializing this database. abort!")
		}
		return nil
	})

	require.Error(t, ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE mytest;"))

	exists, _ := dEnv.FS.Exists("mytest")
	require.False(t, exists, "a failed CREATE DATABASE must not leave its directory behind")

	// The retry must get a database of its own. If the failed attempt left its DoltDB in the database cache,
	// this one is handed a store whose files were deleted and writes never reach the new directory.
	failCreate = false
	require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE mytest;"))
	sqlCtx.SetCurrentDatabase("mytest")
	require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, "CREATE TABLE t (pk int primary key);\nINSERT INTO t VALUES (1);"))

	newFs, err := dEnv.FS.WithWorkingDir("mytest")
	require.NoError(t, err)
	assert.False(t, dbfactory.IsDatabaseInProgress(newFs), "the retried CREATE DATABASE must clear the marker")

	// Reopen the database from disk to prove its contents landed in its own directory.
	absPath, err := newFs.Abs("")
	require.NoError(t, err)
	require.NoError(t, dbfactory.DeleteFromSingletonCache(dbfactory.SingletonCacheKeyForDatabaseDir(absPath), true))
	reopened := env.Load(ctx, env.GetCurrentUserHomeDir, newFs, doltdb.LocalDirDoltDB, "test")
	require.NoError(t, reopened.DBLoadError)
	require.NoError(t, reopened.RSLoadErr)
	t.Cleanup(func() { reopened.Close() })

	root, err := reopened.WorkingRoot(ctx)
	require.NoError(t, err)
	_, ok, err := root.GetTable(ctx, doltdb.TableName{Name: "t"})
	require.NoError(t, err)
	assert.True(t, ok, "the retried database's table must be on disk in its own directory")
}

func TestDuplicateDatabaseNameSkipped(t *testing.T) {
	ctx := context.Background()

	dEnv1 := dtestutils.CreateTestEnvWithName("dupdb")
	db1, err := NewDatabase(ctx, "dupdb", dEnv1.DbData(ctx), editor.Options{})
	require.NoError(t, err)

	dEnv2 := dtestutils.CreateTestEnvWithName("dupdb2")
	db2, err := NewDatabase(ctx, "dupdb", dEnv2.DbData(ctx), editor.Options{})
	require.NoError(t, err)

	pro, err := NewDoltDatabaseProviderWithDatabases(
		"main",
		dEnv1.FS,
		[]dsess.SqlDatabase{db1, db2},
		[]filesys.Filesys{dEnv1.FS, dEnv2.FS},
		sql.EngineOverrides{},
	)
	require.NoError(t, err)

	config, _ := dEnv1.Config.GetConfig(env.GlobalConfig)
	sqlCtx := NewTestSQLCtxWithProvider(ctx, pro, config, nil, nil)
	dbs := pro.AllDatabases(sqlCtx)
	assert.Len(t, dbs, 1)

	db, err := pro.Database(sqlCtx, "dupdb")
	require.NoError(t, err)
	assert.Same(t, db1.GetDoltDB(), db.(Database).GetDoltDB())
}

func TestCloneDatabaseDuplicateNameRejected(t *testing.T) {
	_, sqlCtx, pro, _ := newProviderEngine(t)
	err := pro.CloneDatabaseFromRemote(sqlCtx, "DOLT", "main", "origin", "file:///nonexistent", -1, nil)
	require.Error(t, err)
	assert.True(t, sql.ErrDatabaseExists.Is(err))
}

// TestDroppedDatabaseReportsNotFound covers a Database value that was obtained before the database was dropped, as
// happens when information_schema enumerates databases while another session runs DROP DATABASE. Visiting such a
// Database must report that it was not found, rather than an internal error.
func TestDroppedDatabaseReportsNotFound(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnvForLocalFilesystem()
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	sess := dsess.DSessFromSess(sqlCtx.Session)
	pro := sess.Provider().(*DoltDatabaseProvider)

	require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE mytest;"))
	stale, ok, err := pro.SessionDatabase(sqlCtx, "mytest")
	require.NoError(t, err)
	require.True(t, ok)
	_, err = stale.GetRoot(sqlCtx)
	require.NoError(t, err)

	require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, "DROP DATABASE mytest;"))

	t.Run("GetRoot", func(t *testing.T) {
		_, err := stale.GetRoot(sqlCtx)
		require.Error(t, err)
		assert.True(t, sql.ErrDatabaseNotFound.Is(err), "unexpected error: %v", err)
	})

	t.Run("GetWorkingSet", func(t *testing.T) {
		_, err := stale.(Database).GetWorkingSet(sqlCtx)
		require.Error(t, err)
		assert.True(t, sql.ErrDatabaseNotFound.Is(err), "unexpected error: %v", err)
	})

	t.Run("LookupDbState", func(t *testing.T) {
		_, ok, err := sess.LookupDbState(sqlCtx, "mytest")
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

// newLeaseTestEngine returns an engine with a database for each name in |dbNames|, the context it was created with,
// and its provider.
func newLeaseTestEngine(t *testing.T, dbNames ...string) (*sqle.Engine, *sql.Context, *DoltDatabaseProvider) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnvForLocalFilesystem()
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)

	for _, name := range dbNames {
		require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE "+name+";"))
	}

	pro := dsess.DSessFromSess(sqlCtx.Session).Provider().(*DoltDatabaseProvider)
	return engine, sqlCtx, pro
}

// newLeaseTestSession returns a new session for |engine| and |pro| that is running a command, which holds a lease on
// each database in |leasedDbs|. The command ends when the test does, unless the caller ends it first.
func newLeaseTestSession(t *testing.T, engine *sqle.Engine, pro *DoltDatabaseProvider, leasedDbs ...string) (*sql.Context, *dsess.DoltSession) {
	sqlCtx := NewTestSQLCtxWithProvider(context.Background(), pro, config.NewMapConfig(map[string]string{}), nil, nil, engine.Variables())
	sess := dsess.DSessFromSess(sqlCtx.Session)
	require.NoError(t, sess.CommandBegin())
	t.Cleanup(sess.ReleaseDatabaseLeases)

	for _, name := range leasedDbs {
		_, ok, err := sess.LookupDbState(sqlCtx, name)
		require.NoError(t, err)
		require.True(t, ok)
	}

	return sqlCtx, sess
}

// requireDone waits for |done| to receive and returns the received error, failing the test if that takes too long.
func requireDone(t *testing.T, done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timed out waiting for DROP DATABASE")
		return nil
	}
}

// TestDropDatabaseWaitsForLeases tests that DROP DATABASE waits for commands in other sessions using the database to
// finish, and that the database cannot be used by new commands or re-created while it waits.
func TestDropDatabaseWaitsForLeases(t *testing.T) {
	engine, sqlCtx, pro := newLeaseTestEngine(t, "mytest")
	readerCtx, reader := newLeaseTestSession(t, engine, pro, "mytest")

	done := make(chan error, 1)
	go func() {
		done <- pro.DropDatabase(sqlCtx, "mytest")
	}()

	require.Never(t, func() bool { return len(done) > 0 }, 200*time.Millisecond, 10*time.Millisecond)

	// The command holding the lease can still use the database
	_, ok, err := reader.LookupDbState(readerCtx, "mytest")
	require.NoError(t, err)
	assert.True(t, ok)

	// New commands cannot use it, and it cannot be re-created
	otherCtx, other := newLeaseTestSession(t, engine, pro)
	_, ok, err = other.LookupDbState(otherCtx, "mytest")
	require.NoError(t, err)
	assert.False(t, ok)
	err = pro.CreateDatabase(otherCtx, "mytest")
	assert.True(t, sql.ErrDatabaseExists.Is(err), "unexpected error: %v", err)

	reader.CommandEnd()
	require.NoError(t, requireDone(t, done))
	assert.False(t, pro.HasDatabase(otherCtx, "mytest"))
}

// TestDropDatabaseCanceledWhileWaitingForLeases tests that a DROP DATABASE canceled while it waits for another
// session's lease leaves the database in place.
func TestDropDatabaseCanceledWhileWaitingForLeases(t *testing.T) {
	engine, sqlCtx, pro := newLeaseTestEngine(t, "mytest")
	readerCtx, reader := newLeaseTestSession(t, engine, pro, "mytest")

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := pro.DropDatabase(sqlCtx.WithContext(timeoutCtx), "mytest")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	assert.True(t, pro.HasDatabase(sqlCtx, "mytest"))
	_, ok, err := reader.LookupDbState(readerCtx, "mytest")
	require.NoError(t, err)
	assert.True(t, ok)

	reader.CommandEnd()
	require.NoError(t, pro.DropDatabase(sqlCtx, "mytest"))
	assert.False(t, pro.HasDatabase(sqlCtx, "mytest"))
}

// TestDropDatabasesLeasedByEachOther tests that two sessions can each drop a database the other is using, since a
// session releases its own leases before it waits for others.
func TestDropDatabasesLeasedByEachOther(t *testing.T) {
	engine, _, pro := newLeaseTestEngine(t, "db1", "db2")
	ctx1, _ := newLeaseTestSession(t, engine, pro, "db2")
	ctx2, _ := newLeaseTestSession(t, engine, pro, "db1")

	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	go func() {
		done1 <- pro.DropDatabase(ctx1, "db1")
	}()
	go func() {
		done2 <- pro.DropDatabase(ctx2, "db2")
	}()

	require.NoError(t, requireDone(t, done1))
	require.NoError(t, requireDone(t, done2))
	assert.False(t, pro.HasDatabase(ctx1, "db1"))
	assert.False(t, pro.HasDatabase(ctx1, "db2"))
}
