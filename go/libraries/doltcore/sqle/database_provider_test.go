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
