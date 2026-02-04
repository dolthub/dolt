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
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/dolthub/fslock"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// TestCreateDatabase_ReleasesLockOnEngineClose asserts that when embedded callers opt into
// disable_singleton_cache, closing the SQL engine releases the underlying filesystem lock
// for a newly created database so subsequent opens can proceed.
func TestCreateDatabase_ReleasesLockOnEngineClose(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping on windows due to differing file locking semantics")
	}

	ctx := context.Background()
	root := t.TempDir()

	fs, err := filesys.LocalFS.WithWorkingDir(root)
	require.NoError(t, err)

	cfg := config.NewMapConfig(map[string]string{
		config.UserNameKey:  "test",
		config.UserEmailKey: "test@example.com",
	})

	dbLoadParams := map[string]interface{}{
		dbfactory.DisableSingletonCacheParam:    struct{}{},
		dbfactory.FailOnJournalLockTimeoutParam: struct{}{},
	}

	rootEnv := env.LoadWithoutDB(ctx, env.GetCurrentUserHomeDir, fs, doltdb.LocalDirDoltDB, "test")
	rootEnv.DBLoadParams = map[string]interface{}{
		dbfactory.DisableSingletonCacheParam:    struct{}{},
		dbfactory.FailOnJournalLockTimeoutParam: struct{}{},
	}
	mrEnv, err := env.MultiEnvForDirectory(ctx, cfg, fs, "test", rootEnv)
	require.NoError(t, err)

	seCfg := &SqlEngineConfig{
		ServerUser:   "root",
		ServerHost:   "localhost",
		Autocommit:   true,
		DBLoadParams: dbLoadParams,
	}

	se, err := NewSqlEngine(ctx, mrEnv, seCfg)
	require.NoError(t, err)

	sqlCtx, err := se.NewLocalContext(ctx)
	require.NoError(t, err)

	_, _, _, err = se.Query(sqlCtx, "CREATE DATABASE IF NOT EXISTS testdb")
	require.NoError(t, err)

	err = se.Close()
	require.True(t, err == nil || errors.Is(err, context.Canceled), "unexpected close error: %v", err)

	// If the DB is properly closed, we should be able to take the lock quickly.
	lockPath := filepath.Join(root, "testdb", ".dolt", "noms", "LOCK")
	_, err = os.Stat(lockPath)
	require.NoError(t, err, "expected lock file to exist at %s", lockPath)

	lck := fslock.New(lockPath)
	err = lck.LockWithTimeout(25 * time.Millisecond)
	require.NoError(t, err, "expected lock to be free after engine close (path=%s)", lockPath)
	require.NoError(t, lck.Unlock())
}
