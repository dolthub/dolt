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

package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// TestSqlEngineCloseStopsWorkers checks engine-owned cleanup with a real database and live caller context.
func TestSqlEngineCloseStopsWorkers(t *testing.T) {
	root, home := t.TempDir(), t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		mrEnv, cfg := lifecycleEngineEnvironment(t, ctx, root, home)
		cfg.BranchActivityTracking = true
		se, err := NewSqlEngine(ctx, mrEnv, cfg)
		require.NoError(t, err)
		defer func() { require.NoError(t, se.Close()) }()

		sqlCtx, err := se.NewLocalContext(ctx)
		require.NoError(t, err)
		defer sql.SessionEnd(sqlCtx.Session)
		lifecycleQuery(t, se, sqlCtx, "CREATE DATABASE lifecycle")
		lifecycleQuery(t, se, sqlCtx, "USE lifecycle")
		require.Equal(t, []sql.Row{{"lifecycle"}}, lifecycleQuery(t, se, sqlCtx, "SELECT DATABASE()"))
		// synctest detects any engine workers left running after Close.
	})
}

// TestSqlEngineFailedConstructionStopsWorkers checks cleanup on errors before and after tracker creation.
func TestSqlEngineFailedConstructionStopsWorkers(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		name := "privilege_read_error"
		if malformed {
			name = "privilege_parse_error"
		}
		t.Run(name, func(t *testing.T) {
			root, home := t.TempDir(), t.TempDir()
			privs := root
			if malformed {
				privs = filepath.Join(root, "privileges.db")
				require.NoError(t, os.WriteFile(privs, []byte{0}, 0600))
			}
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				mrEnv, cfg := lifecycleEngineEnvironment(t, ctx, root, home)
				cfg.PrivFilePath = privs
				cfg.BranchActivityTracking = true
				se, err := NewSqlEngine(ctx, mrEnv, cfg)
				require.Error(t, err)
				require.Nil(t, se)
				if malformed {
					require.ErrorContains(t, err, "ill formatted privileges file")
				}
				require.NoError(t, ctx.Err())
				// No engine is returned to close; construction must join its own workers.
			})
		})
	}
}

// lifecycleEngineEnvironment configures a disk-backed engine that owns its storage lifetime.
func lifecycleEngineEnvironment(t *testing.T, ctx context.Context, root, home string) (*env.MultiRepoEnv, *SqlEngineConfig) {
	t.Helper()
	fs, err := filesys.LocalFS.WithWorkingDir(root)
	require.NoError(t, err)
	params := map[string]interface{}{
		dbfactory.DisableSingletonCacheParam:    struct{}{},
		dbfactory.FailOnJournalLockTimeoutParam: struct{}{},
	}
	rootEnv := env.LoadWithoutDB(ctx, func() (string, error) { return home, nil }, fs, doltdb.LocalDirDoltDB, "test")
	rootEnv.DBLoadParams = params
	require.NoError(t, rootEnv.Config.WriteableConfig().SetStrings(map[string]string{
		config.UserNameKey:  "test",
		config.UserEmailKey: "test@example.com",
	}))
	mrEnv, err := env.MultiEnvForDirectory(ctx, fs, rootEnv)
	require.NoError(t, err)
	return mrEnv, &SqlEngineConfig{
		ServerUser:   "root",
		ServerHost:   "localhost",
		Autocommit:   true,
		DBLoadParams: params,
	}
}

// lifecycleQuery executes a statement completely, including its commit and iterator cleanup.
func lifecycleQuery(t *testing.T, se *SqlEngine, ctx *sql.Context, query string) []sql.Row {
	t.Helper()
	sql.SessionCommandBegin(ctx.Session)
	defer sql.SessionCommandEnd(ctx.Session)
	_, iter, _, err := se.Query(ctx, query)
	require.NoError(t, err, query)
	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(t, err, query)
	return rows
}
