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
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/datas"
)

func setGlobalSqlVariable(t *testing.T, name string, val interface{}) {
	_, cur, _ := sql.SystemVariables.GetGlobal(name)
	t.Cleanup(func() {
		sql.SystemVariables.SetGlobal(name, cur)
	})
	sql.SystemVariables.SetGlobal(name, val)
}

func TestDatabaseProvider(t *testing.T) {
	t.Run("ReplicationConfig", func(t *testing.T) {
		t.Run("CreateDatabase", func(t *testing.T) {
			t.Run("NoReplication", func(t *testing.T) {
				ctx := context.Background()
				dEnv := dtestutils.CreateTestEnv()
				tmpDir, err := dEnv.TempTableFilesDir()
				require.NoError(t, err)
				opts := editor.Options{Deaf: dEnv.DbEaFactory(ctx), Tempdir: tmpDir}
				db, err := NewDatabase(context.Background(), "dolt", dEnv.DbData(ctx), opts)
				require.NoError(t, err)

				engine, sqlCtx, err := NewTestEngine(dEnv, context.Background(), db)
				require.NoError(t, err)

				sess := dsess.DSessFromSess(sqlCtx.Session)
				sess.Provider().(*DoltDatabaseProvider).AddInitDatabaseHook(InstallSnoopingCommitHook)

				err = ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE mytest;")
				require.NoError(t, err)

				sqlDb, err := sess.Provider().Database(sqlCtx, "mytest")
				require.NoError(t, err)
				ddbs := sqlDb.(Database).DoltDatabases()
				require.Len(t, ddbs, 1)
				hooks := doltdb.HackDatasDatabaseFromDoltDB(ddbs[0]).(interface {
					PostCommitHooks() []doltdb.CommitHook
				}).PostCommitHooks()
				assert.Len(t, hooks, 1)
				_, ok := hooks[0].(*snoopingCommitHook)
				assert.True(t, ok, "expect hook to be PushOnWriteHook, it is %T", hooks[0])
			})
			t.Run("PushOnWriteReplication", func(t *testing.T) {
				ctx := context.Background()
				dEnv := dtestutils.CreateTestEnv()
				tmpDir, err := dEnv.TempTableFilesDir()

				setGlobalSqlVariable(t, dsess.ReplicateToRemote, "fileremote")
				setGlobalSqlVariable(t, dsess.ReplicationRemoteURLTemplate, "mem://remote_{database}")

				require.NoError(t, err)
				opts := editor.Options{Deaf: dEnv.DbEaFactory(ctx), Tempdir: tmpDir}
				db, err := NewDatabase(context.Background(), "dolt", dEnv.DbData(ctx), opts)
				require.NoError(t, err)

				engine, sqlCtx, err := NewTestEngine(dEnv, context.Background(), db)
				require.NoError(t, err)

				sess := dsess.DSessFromSess(sqlCtx.Session)
				sess.Provider().(*DoltDatabaseProvider).AddInitDatabaseHook(InstallSnoopingCommitHook)

				err = ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE mytest;")
				require.NoError(t, err)

				sqlDb, err := sess.Provider().Database(sqlCtx, "mytest")
				require.NoError(t, err)
				ddbs := sqlDb.(Database).DoltDatabases()
				require.Len(t, ddbs, 1)
				hooks := doltdb.HackDatasDatabaseFromDoltDB(ddbs[0]).(interface {
					PostCommitHooks() []doltdb.CommitHook
				}).PostCommitHooks()
				require.Len(t, hooks, 2)
				_, ok := hooks[0].(*snoopingCommitHook)
				assert.True(t, ok, "expect hook to be snoopingCommitHook, it is %T", hooks[0])
				_, ok = hooks[1].(*PushOnWriteHook)
				assert.True(t, ok, "expect hook to be PushOnWriteHook, it is %T", hooks[1])
			})
			t.Run("AsyncPushOnWrite", func(t *testing.T) {
				ctx := context.Background()
				dEnv := dtestutils.CreateTestEnv()
				tmpDir, err := dEnv.TempTableFilesDir()

				setGlobalSqlVariable(t, dsess.ReplicateToRemote, "fileremote")
				setGlobalSqlVariable(t, dsess.ReplicationRemoteURLTemplate, "mem://remote_{database}")
				setGlobalSqlVariable(t, dsess.AsyncReplication, dsess.SysVarTrue)

				require.NoError(t, err)
				opts := editor.Options{Deaf: dEnv.DbEaFactory(ctx), Tempdir: tmpDir}
				db, err := NewDatabase(context.Background(), "dolt", dEnv.DbData(ctx), opts)
				require.NoError(t, err)

				engine, sqlCtx, err := NewTestEngine(dEnv, context.Background(), db)
				require.NoError(t, err)

				sess := dsess.DSessFromSess(sqlCtx.Session)
				sess.Provider().(*DoltDatabaseProvider).AddInitDatabaseHook(InstallSnoopingCommitHook)

				err = ExecuteSqlOnEngine(sqlCtx, engine, "CREATE DATABASE mytest;")
				require.NoError(t, err)

				sqlDb, err := sess.Provider().Database(sqlCtx, "mytest")
				require.NoError(t, err)
				ddbs := sqlDb.(Database).DoltDatabases()
				require.Len(t, ddbs, 1)
				hooks := doltdb.HackDatasDatabaseFromDoltDB(ddbs[0]).(interface {
					PostCommitHooks() []doltdb.CommitHook
				}).PostCommitHooks()
				require.Len(t, hooks, 2)
				_, ok := hooks[0].(*snoopingCommitHook)
				assert.True(t, ok, "expect hook to be snoopingCommitHook, it is %T", hooks[0])
				_, ok = hooks[1].(*AsyncPushOnWriteHook)
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

func (*snoopingCommitHook) HandleError(ctx context.Context, err error) error {
	return nil
}

func (*snoopingCommitHook) SetLogger(ctx context.Context, wr io.Writer) error {
	return nil
}

func (*snoopingCommitHook) ExecuteForWorkingSets() bool {
	return true
}

func InstallSnoopingCommitHook(ctx *sql.Context, pro *DoltDatabaseProvider, name string, dEnv *env.DoltEnv, db dsess.SqlDatabase) error {
	dEnv.DoltDB(ctx).PrependCommitHooks(ctx, &snoopingCommitHook{})
	return nil
}
