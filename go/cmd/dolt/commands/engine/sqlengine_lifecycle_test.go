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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

// TestSqlEngineCloseStopsWorkers checks engine-owned cleanup with a real database and live caller context.
func TestSqlEngineCloseStopsWorkers(t *testing.T) {
	root, home := t.TempDir(), t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		mrEnv, cfg, _ := lifecycleEngineEnvironment(t, ctx, root, home)
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

func TestIndependentSqlEngineBootstrap(t *testing.T) {
	ctx := context.Background()
	engines := make([]*SqlEngine, 2)
	for i, maxConnections := range []string{"71", "83"} {
		_, cfg, dEnv := lifecycleEngineEnvironment(t, ctx, t.TempDir(), t.TempDir())
		require.NoError(t, dEnv.InitRepo(ctx, types.Format_DOLT, "test", "test@example.com", env.DefaultInitBranch))
		mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.FS, dEnv)
		require.NoError(t, err)
		cfg.EngineVariables = sqle.NewEngineVariables()
		require.NoError(t, dEnv.Config.WriteableConfig().SetStrings(map[string]string{
			env.SqlServerGlobalsPrefix + ".max_connections": maxConnections,
		}))
		if i == 0 {
			dsess.DefineSystemVariablesForDBInRegistry(cfg.EngineVariables.System, "private_db")
		}
		require.NoError(t, dsess.PersistSystemVarDefaultsWithRegistry(cfg.EngineVariables.System, dEnv))
		require.NoError(t, dsess.InitPersistedSystemVarsWithRegistry(cfg.EngineVariables.System, dEnv))
		engines[i], err = NewSqlEngine(ctx, mrEnv, cfg)
		require.NoError(t, err)
		defer engines[i].Close()
		require.Same(t, cfg.EngineVariables, engines[i].GetUnderlyingEngine().Variables())
		localCtx, err := engines[i].NewLocalContext(ctx)
		require.NoError(t, err)
		require.Same(t, cfg.EngineVariables, localCtx.EngineVariables())
		require.Equal(t, maxConnections, fmt.Sprint(lifecycleQuery(t, engines[i], localCtx, "SELECT @@global.max_connections")[0][0]))
	}

	varsA := engines[0].GetUnderlyingEngine().Variables().System
	varsB := engines[1].GetUnderlyingEngine().Variables().System
	_, uuidA, _ := varsA.GetGlobal("server_uuid")
	_, uuidB, _ := varsB.GetGlobal("server_uuid")
	require.NotEqual(t, uuidA, uuidB)
	_, _, ok := varsB.GetGlobal(dsess.HeadKey("private_db"))
	require.False(t, ok)
	_, maxB, _ := varsB.GetGlobal("max_connections")
	require.EqualValues(t, 83, maxB)
}

func TestNewSqlEngineForEnvLoadsPersistedDefaults(t *testing.T) {
	ctx := context.Background()
	_, cfg, dEnv := lifecycleEngineEnvironment(t, ctx, t.TempDir(), t.TempDir())
	require.NoError(t, dEnv.InitRepo(ctx, types.Format_DOLT, "test", "test@example.com", env.DefaultInitBranch))
	require.NoError(t, dEnv.Config.WriteableConfig().SetStrings(map[string]string{
		env.SqlServerGlobalsPrefix + ".max_connections": "91",
	}))
	se, _, err := NewSqlEngineForEnv(ctx, dEnv, func(c *SqlEngineConfig) {
		c.DBLoadParams = cfg.DBLoadParams
	})
	require.NoError(t, err)
	_, value, ok := se.GetUnderlyingEngine().Variables().System.GetGlobal("max_connections")
	require.True(t, ok)
	require.EqualValues(t, 91, value)
	stats := se.GetUnderlyingEngine().Analyzer.Catalog.StatsProvider.(*statspro.StatsController)
	sqlCtx, err := se.NewDefaultContext(ctx)
	require.NoError(t, err)
	defer sql.SessionEnd(sqlCtx.Session)
	require.Same(t, se.GetUnderlyingEngine().Variables(), sqlCtx.EngineVariables())
	require.NoError(t, se.Close())
	require.ErrorContains(t, stats.Restart(sqlCtx), "StatsController is closed")
}

func TestSqlEngineCapturesBundleFromReusableConfig(t *testing.T) {
	ctx := context.Background()
	mrA, cfg, _ := lifecycleEngineEnvironment(t, ctx, t.TempDir(), t.TempDir())
	varsA := sqle.NewEngineVariables()
	cfg.EngineVariables = varsA
	engineA, err := NewSqlEngine(ctx, mrA, cfg)
	require.NoError(t, err)
	defer engineA.Close()

	mrB, _, _ := lifecycleEngineEnvironment(t, ctx, t.TempDir(), t.TempDir())
	varsB := sqle.NewEngineVariables()
	cfg.EngineVariables = varsB
	engineB, err := NewSqlEngine(ctx, mrB, cfg)
	require.NoError(t, err)
	defer engineB.Close()

	ctxA, err := engineA.NewDefaultContext(ctx)
	require.NoError(t, err)
	require.Same(t, varsA, ctxA.EngineVariables())
	ctxB, err := engineB.NewDefaultContext(ctx)
	require.NoError(t, err)
	require.Same(t, varsB, ctxB.EngineVariables())
}

// TestNewSqlEngineWithoutBundleSharesGlobalRegistries checks the contract that
// embedders built before engine-owned variables rely on.
func TestNewSqlEngineWithoutBundleSharesGlobalRegistries(t *testing.T) {
	ctx := context.Background()
	mrEnv, cfg, _ := lifecycleEngineEnvironment(t, ctx, t.TempDir(), t.TempDir())
	se, err := NewSqlEngine(ctx, mrEnv, cfg)
	require.NoError(t, err)
	defer se.Close()

	require.Same(t, sql.SystemVariables, se.GetUnderlyingEngine().Variables().System)
	require.NotNil(t, sql.StatusVariables)
	sess, err := se.NewDoltSession(ctx, sql.NewBaseSession())
	require.NoError(t, err)
	require.Nil(t, sess.EngineVariables())
	sqlCtx := se.ContextFactory(ctx, sql.WithSession(sess))
	_, iter, _, err := se.Query(sqlCtx, "SELECT @@max_connections")
	require.NoError(t, err)
	_, err = sql.RowIterToRows(sqlCtx, iter)
	require.NoError(t, err)
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
				mrEnv, cfg, _ := lifecycleEngineEnvironment(t, ctx, root, home)
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
func lifecycleEngineEnvironment(t *testing.T, ctx context.Context, root, home string) (*env.MultiRepoEnv, *SqlEngineConfig, *env.DoltEnv) {
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
	}, rootEnv
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
