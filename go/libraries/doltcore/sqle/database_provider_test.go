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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
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

func newProviderEngine(t *testing.T) (*sqle.Engine, *sql.Context, *DoltDatabaseProvider, *env.DoltEnv) {
	t.Helper()
	return newProviderEngineWithEnv(t, dtestutils.CreateTestEnv())
}

func newLocalProviderEngine(t *testing.T) (*sqle.Engine, *sql.Context, *DoltDatabaseProvider, *env.DoltEnv) {
	t.Helper()
	return newProviderEngineWithEnv(t, dtestutils.CreateTestEnvForLocalFilesystem())
}

func newProviderEngineWithEnv(t *testing.T, dEnv *env.DoltEnv) (*sqle.Engine, *sql.Context, *DoltDatabaseProvider, *env.DoltEnv) {
	t.Helper()
	ctx := context.Background()
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	sess := dsess.DSessFromSess(sqlCtx.Session)
	pro := sess.Provider().(*DoltDatabaseProvider)
	pro.remoteDialer = env.NewGRPCDialProviderFromDoltEnv(dEnv)
	return engine, sqlCtx, pro, dEnv
}

func providerWithIncompleteDir(t *testing.T, setupDir func(t *testing.T, fs filesys.Filesys)) (*sqle.Engine, *sql.Context, *DoltDatabaseProvider) {
	t.Helper()
	engine, sqlCtx, pro, dEnv := newLocalProviderEngine(t)
	require.NoError(t, dEnv.FS.MkDirs("foo"))
	fooFS, err := dEnv.FS.WithWorkingDir("foo")
	require.NoError(t, err)
	setupDir(t, fooFS)
	return engine, sqlCtx, pro
}

func testWithIncompleteDir(
	t *testing.T,
	testFn func(t *testing.T, engine *sqle.Engine, sqlCtx *sql.Context, pro *DoltDatabaseProvider, reclaimable bool),
) {
	t.Helper()
	tests := []struct {
		name        string
		reclaimable bool
		setupDir    func(t *testing.T, fs filesys.Filesys)
	}{
		{
			name:        "in-progress marker",
			reclaimable: true,
			setupDir: func(t *testing.T, fs filesys.Filesys) {
				require.NoError(t, fs.WriteFile(dbfactory.SafeToIgnoreMarkerFile, nil, 0o644))
			},
		},
		{
			name:        "missing repo state",
			reclaimable: false,
			setupDir: func(t *testing.T, fs filesys.Filesys) {
				require.NoError(t, fs.MkDirs(filepath.Join(dbfactory.DoltDir, dbfactory.DataDir)))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()
			engine, sqlCtx, pro := providerWithIncompleteDir(t, tc.setupDir)
			testFn(t, engine, sqlCtx, pro, tc.reclaimable)
		})
	}
}

func TestCreateDatabaseWithIncompleteDir(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	testWithIncompleteDir(t, func(t *testing.T, engine *sqle.Engine, sqlCtx *sql.Context, pro *DoltDatabaseProvider, reclaimable bool) {
		if reclaimable {
			for _, q := range []string{"CREATE DATABASE foo;", "CREATE DATABASE IF NOT EXISTS foo;"} {
				require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, q), "query %q", q)
				_, err := pro.Database(sqlCtx, "foo")
				require.NoError(t, err)
			}
		} else {
			err := ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE foo;")
			require.Error(t, err)
			assert.True(t, sql.ErrDatabaseExists.Is(err))
		}
	})
}

func TestCloneDatabaseWithIncompleteDir(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	testWithIncompleteDir(t, func(t *testing.T, _ *sqle.Engine, sqlCtx *sql.Context, pro *DoltDatabaseProvider, reclaimable bool) {
		err := pro.CloneDatabaseFromRemote(sqlCtx, "foo", "main", "origin", "file://unreachable", -1, nil)
		if reclaimable {
			// Reclaimable directory allows clone to proceed to remote.
			require.Error(t, err)
			assert.False(t, sql.ErrDatabaseExists.Is(err))
		} else {
			// Non-reclaimable existing directory is rejected before remote.
			require.Error(t, err)
			assert.True(t, sql.ErrDatabaseExists.Is(err))
		}
	})
}

func TestInProgressCommitConflict(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	for _, tc := range []struct {
		name        string
		startCreate func(t *testing.T, ctx context.Context, dEnv *env.DoltEnv) func() error
	}{
		{
			name: "create in progress",
			startCreate: func(t *testing.T, _ context.Context, dEnv *env.DoltEnv) func() error {
				fsTx, err := dbfactory.BeginCreate(dEnv.FS, "foo", uuid.Nil)
				require.NoError(t, err)
				require.NoError(t, fsTx.FS().WriteFile("uncommitted.txt", []byte("uncommitted"), 0o644))
				return fsTx.Commit
			},
		},
		{
			name: "clone in progress",
			startCreate: func(t *testing.T, ctx context.Context, dEnv *env.DoltEnv) func() error {
				hdp := func() (string, error) { return dEnv.FS.TempDir(), nil }
				clonedEnv, fsTx, err := actions.EnvForClone(ctx, types.Format_DOLT, env.NoRemote, "foo", dEnv.FS, "test", hdp)
				require.NoError(t, err)
				return func() error {
					return errors.Join(clonedEnv.Close(), fsTx.Commit())
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, sqlCtx, pro, dEnv := newLocalProviderEngine(t)
			commit := tc.startCreate(t, sqlCtx, dEnv)

			err := ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE foo;")
			require.NoError(t, err)
			_, err = pro.Database(sqlCtx, "foo")
			require.NoError(t, err)

			err = commit()
			require.Error(t, err)
			assert.ErrorIs(t, err, dbfactory.ErrExists)

			db, err := pro.Database(sqlCtx, "foo")
			require.NoError(t, err)
			require.NotNil(t, db)
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
			exists, _ := newFs.Exists(dbfactory.SafeToIgnoreMarkerFile)
			assert.False(t, exists, "a completed CREATE DATABASE must clear the marker")
		})
	}
}

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
			require.Truef(t, sql.ErrDatabaseExists.Is(pro.reserveCreatingDatabase(variant)), "clone of %q should conflict", variant)
			require.Truef(t, sql.ErrDatabaseExists.Is(checkNameAvailable(pro, variant, true)), "CREATE of %q should conflict", variant)
			require.Truef(t, sql.ErrDatabaseExists.Is(checkNameAvailable(pro, variant, false)), "UNDROP of %q should conflict", variant)
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

		require.Truef(t, sql.ErrDatabaseExists.Is(checkNameAvailable(pro, "DELDB", true)), "CREATE of deleting db should conflict")
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

	// Ensure no scratchpad was leaked on failure.
	err = dEnv.FS.Iter("", false, func(path string, size int64, isDir bool) bool {
		base := filepath.Base(path)
		assert.False(t, strings.HasPrefix(base, dbfactory.TempDirPrefix), "no scratchpad should remain: %s", path)
		return false
	})
	require.NoError(t, err)

	// The retry must get a database of its own. If the failed attempt left its DoltDB in the database cache,
	// this one is handed a store whose files were deleted and writes never reach the new directory.
	failCreate = false
	require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE mytest;"))
	sqlCtx.SetCurrentDatabase("mytest")
	require.NoError(t, ExecuteSqlOnEngine(sqlCtx, engine, "CREATE TABLE t (pk int primary key);\nINSERT INTO t VALUES (1);"))

	newFs, err := dEnv.FS.WithWorkingDir("mytest")
	require.NoError(t, err)
	exists, _ = newFs.Exists(dbfactory.SafeToIgnoreMarkerFile)
	assert.False(t, exists, "the retried CREATE DATABASE must clear the marker")

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

func TestCloneDatabaseFailureLeavesNothingBehind(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnvForLocalFilesystem()
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	_, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	pro := dsess.DSessFromSess(sqlCtx.Session).Provider().(*DoltDatabaseProvider)
	pro.SetRemoteDialer(env.NewGRPCDialProviderFromDoltEnv(dEnv))

	err = pro.CloneDatabaseFromRemote(sqlCtx, "fail1", "main", "origin", "file://unreachable", -1, nil)
	require.Error(t, err)
	exists, _ := dEnv.FS.Exists("fail1")
	assert.False(t, exists, "failed clone must not leave destination directory")

	err = dEnv.FS.Iter("", false, func(path string, _ int64, _ bool) bool {
		base := filepath.Base(path)
		assert.False(t, strings.HasPrefix(base, dbfactory.TempDirPrefix), "no scratchpad should remain: %s", path)
		return false
	})
	require.NoError(t, err)
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

type testDatabaseUpdateListener struct {
	created      []string
	createdXIDs  []uint64
	updatedRoots []string
}

var _ doltdb.DatabaseUpdateListener = (*testDatabaseUpdateListener)(nil)

func (t *testDatabaseUpdateListener) WorkingRootUpdated(ctx *sql.Context, dbName string, branch string, before, after doltdb.RootValue) error {
	t.updatedRoots = append(t.updatedRoots, dbName)
	return nil
}

func (t *testDatabaseUpdateListener) DatabaseCreated(ctx *sql.Context, dbName string, xid uint64) error {
	t.created = append(t.created, dbName)
	t.createdXIDs = append(t.createdXIDs, xid)
	return nil
}

func (t *testDatabaseUpdateListener) DatabaseDropped(ctx *sql.Context, dbName string) error {
	return nil
}

func TestUndropNotifiesDatabaseUpdateListener(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnvForLocalFilesystem()
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)
	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	require.NoError(t, err)
	pro := dsess.DSessFromSess(sqlCtx.Session).Provider().(*DoltDatabaseProvider)

	listener := &testDatabaseUpdateListener{}
	doltdb.RegisterDatabaseUpdateListener(listener)
	defer ResetDatabaseUpdateListenersForTesting()

	err = ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE undrop_test;")
	require.NoError(t, err)
	assert.Contains(t, listener.created, "undrop_test")

	err = ExecuteSqlOnEngine(sqlCtx, engine, "DROP DATABASE undrop_test;")
	require.NoError(t, err)

	listener.created = nil
	listener.updatedRoots = nil

	err = pro.UndropDatabase(sqlCtx, "undrop_test")
	require.NoError(t, err)

	assert.Contains(t, listener.created, "undrop_test")
	assert.Contains(t, listener.updatedRoots, "undrop_test")
}
