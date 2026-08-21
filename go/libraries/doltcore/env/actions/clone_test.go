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

package actions

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

func TestEnvForCloneMarksDatabaseInProgress(t *testing.T) {
	// A process killed after EnvForClone must leave the directory marked, so callers that die before the
	// clone content is complete leave a directory that is ignored rather than served.
	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)
	hdp := func() (string, error) { return fs.TempDir(), nil }

	dEnv, err := EnvForClone(context.Background(), types.Format_DOLT, env.NoRemote, "cloned", fs, "test", hdp)
	require.NoError(t, err)
	defer dEnv.Close()

	require.True(t, dbfactory.IsDatabaseInProgress(dEnv.FS), "EnvForClone must mark the directory before clone content arrives")

	require.NoError(t, dbfactory.ClearDatabaseInProgress(dEnv.FS))
	require.False(t, dbfactory.IsDatabaseInProgress(dEnv.FS))
}

func TestAbortIncompleteCloneRemovesCreatedDir(t *testing.T) {
	ctx := context.Background()
	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)
	hdp := func() (string, error) { return fs.TempDir(), nil }

	dEnv, err := EnvForClone(ctx, types.Format_DOLT, env.NoRemote, "cloned", fs, "test", hdp)
	require.NoError(t, err)
	require.NotNil(t, dEnv.DoltDB(ctx))
	require.NoError(t, AbortIncompleteClone(dEnv, true /* dirCreated */))

	require.Nil(t, dEnv.DoltDB(ctx), "an aborted clone must close the database it opened before deleting its files")

	exists, _ := fs.Exists("cloned")
	require.False(t, exists, "an aborted clone must not leave the directory it created behind")

	// The retry must get a database of its own, not the store the aborted clone opened and left in the
	// database cache pointing at deleted files.
	retry, err := EnvForClone(ctx, types.Format_DOLT, env.NoRemote, "cloned", fs, "test", hdp)
	require.NoError(t, err)
	cfg, ok := retry.Config.GetConfig(env.GlobalConfig)
	require.True(t, ok)
	require.NoError(t, cfg.SetStrings(map[string]string{config.UserNameKey: "test", config.UserEmailKey: "test@test.com"}))
	require.NoError(t, InitEmptyClonedRepo(ctx, retry))
	require.NoError(t, retry.InitializeRepoState(ctx, env.DefaultInitBranch))
	require.NoError(t, dbfactory.ClearDatabaseInProgress(retry.FS))

	// Reopen from disk to prove the retry's content landed in the new directory.
	absPath, err := retry.FS.Abs("")
	require.NoError(t, err)
	require.NoError(t, dbfactory.DeleteFromSingletonCache(dbfactory.SingletonCacheKeyForDatabaseDir(absPath), true))
	reopened := env.Load(ctx, hdp, retry.FS, doltdb.LocalDirDoltDB, "test")
	t.Cleanup(func() { reopened.Close() })
	require.NoError(t, reopened.DBLoadError)
	require.NoError(t, reopened.RSLoadErr)
	_, err = reopened.WorkingRoot(ctx)
	require.NoError(t, err)
}

func TestAbortIncompleteCloneKeepsUsersDir(t *testing.T) {
	// Cloning into a directory the user already had must not delete that directory, only the Dolt state the
	// clone wrote into it, marker included.
	ctx := context.Background()
	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)
	hdp := func() (string, error) { return fs.TempDir(), nil }

	require.NoError(t, fs.MkDirs("cloned"))
	require.NoError(t, fs.WriteFile(filepath.Join("cloned", "keepme"), []byte("mine"), 0o644))

	dEnv, err := EnvForClone(ctx, types.Format_DOLT, env.NoRemote, "cloned", fs, "test", hdp)
	require.NoError(t, err)
	require.NoError(t, AbortIncompleteClone(dEnv, false /* dirCreated */))

	exists, _ := fs.Exists(filepath.Join("cloned", "keepme"))
	require.True(t, exists, "an aborted clone must not delete the user's own directory")
	exists, _ = fs.Exists(filepath.Join("cloned", dbfactory.DoltDir))
	require.False(t, exists, "an aborted clone must remove the Dolt state it wrote")

	clonedFs, err := fs.WithWorkingDir("cloned")
	require.NoError(t, err)
	require.False(t, env.IsIncompleteDatabaseDir(clonedFs), "an aborted clone must not leave a directory nothing can use")
}

func TestEnvForCloneCleansUpAfterFailure(t *testing.T) {
	// EnvForClone writes the marker before it can fail, so a failure inside it must take the directory with it.
	ctx := context.Background()
	fs, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)
	hdp := func() (string, error) { return fs.TempDir(), nil }

	// A file where the .dolt directory belongs fails the clone after the marker has been written.
	require.NoError(t, fs.MkDirs("cloned"))
	require.NoError(t, fs.WriteFile(filepath.Join("cloned", dbfactory.DoltDir), []byte("not a directory"), 0o644))

	_, err = EnvForClone(ctx, types.Format_DOLT, env.NoRemote, "cloned", fs, "test", hdp)
	require.Error(t, err)

	clonedFs, err := fs.WithWorkingDir("cloned")
	require.NoError(t, err)
	require.False(t, dbfactory.IsDatabaseInProgress(clonedFs), "a failed EnvForClone must not leave its marker behind")
}
