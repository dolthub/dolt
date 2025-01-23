// Copyright 2019 Dolthub, Inc.
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

package env

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/concurrentmap"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	testHomeDir = "/user/bheni"
	workingDir  = "/user/bheni/datasets/addresses"
)

func testHomeDirFunc() (string, error) {
	return testHomeDir, nil
}

func createTestEnv(isInitialized bool, hasLocalConfig bool) (*DoltEnv, *filesys.InMemFS) {
	initialDirs := []string{testHomeDir, workingDir}
	initialFiles := map[string][]byte{}

	if isInitialized {
		doltDir := filepath.Join(workingDir, dbfactory.DoltDir)
		doltDataDir := filepath.Join(workingDir, dbfactory.DoltDataDir)
		initialDirs = append(initialDirs, doltDir)
		initialDirs = append(initialDirs, doltDataDir)

		mainRef := ref.NewBranchRef(DefaultInitBranch)
		repoState := &RepoState{Head: ref.MarshalableRef{Ref: mainRef}, Remotes: concurrentmap.New[string, Remote](), Backups: concurrentmap.New[string, Remote](), Branches: concurrentmap.New[string, BranchConfig]()}
		repoStateData, err := json.Marshal(repoState)

		if err != nil {
			panic("Could not setup test.  Could not marshall repostate struct")
		}

		initialFiles[getRepoStateFile()] = []byte(repoStateData)

		if hasLocalConfig {
			initialFiles[getLocalConfigPath()] = []byte(`{"user.name":"bheni"}`)
		}
	} else if hasLocalConfig {
		panic("Bad test.  Cant have a local config in a non initialized directory.")
	}

	fs := filesys.NewInMemFS(initialDirs, initialFiles, workingDir)
	dEnv := Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB, "test")

	return dEnv, fs
}

func createFileTestEnv(t *testing.T, workingDir, homeDir string) *DoltEnv {
	fs, err := filesys.LocalFilesysWithWorkingDir(filepath.ToSlash(workingDir))
	require.NoError(t, err)

	return Load(context.Background(), func() (string, error) {
		return homeDir, nil
	}, fs, doltdb.LocalDirDoltDB, "test")
}

func TestNonRepoDir(t *testing.T) {
	dEnv, _ := createTestEnv(false, false)

	if !isCWDEmpty(dEnv) {
		t.Error("Should start with a clean wd")
	}

	if dEnv.HasDoltDir() || dEnv.HasLocalConfig() {
		t.Fatal("These should not exist in the environment for a non repo dir.")
	}

	if dEnv.CfgLoadErr != nil {
		t.Error("Only global config load / create error should result in an error")
	}

	if dEnv.RSLoadErr == nil {
		t.Error("File doesn't exist.  There should be an error if the directory doesn't exist.")
	}
}

func TestRepoDir(t *testing.T) {
	dEnv, _ := createTestEnv(true, true)
	assert.True(t, dEnv.HasDoltDir())
	assert.True(t, dEnv.HasLocalConfig())

	userName, err := dEnv.Config.GetString("user.name")
	require.NoError(t, err)
	assert.Equal(t, "bheni", userName)

	assert.NoError(t, dEnv.CfgLoadErr)
	// RSLoadErr will be set because the above method of creating the repo doesn't initialize a valid working or staged
}

func TestRepoDirNoLocal(t *testing.T) {
	dEnv, _ := createTestEnv(true, false)

	if !dEnv.HasDoltDir() {
		t.Fatal(".dolt dir should exist.")
	} else if dEnv.HasLocalConfig() {
		t.Fatal("This should not be here before creation")
	}

	require.NoError(t, dEnv.CfgLoadErr)
	// RSLoadErr will be set because the above method of creating the repo doesn't initialize a valid working or staged

	configDir, err := dEnv.FS.Abs(".")
	require.NoError(t, err)

	err = dEnv.Config.CreateLocalConfig(configDir, map[string]string{"user.name": "bheni"})
	require.NoError(t, err)

	if !dEnv.HasLocalConfig() {
		t.Error("Failed to create local config file")
	}

	if un, err := dEnv.Config.GetString("user.name"); err != nil || un != "bheni" {
		t.Error("Bad local config value.")
	}
}

func TestInitRepo(t *testing.T) {
	dEnv, _ := createTestEnv(false, false)
	err := dEnv.InitRepo(context.Background(), types.Format_Default, "aoeu aoeu", "aoeu@aoeu.org", DefaultInitBranch)
	require.NoError(t, err)
	defer dEnv.DoltDB(ctx).Close()

	_, err = dEnv.WorkingRoot(context.Background())
	require.NoError(t, err)

	_, err = dEnv.StagedRoot(context.Background())
	require.NoError(t, err)
}

// TestMigrateWorkingSet tests migrating a repo with the old RepoState fields to a new one
func TestMigrateWorkingSet(t *testing.T) {
	t.Skip("This fails under race on ubuntu / mac")

	// TODO: t.TempDir breaks on windows because of automatic cleanup (files still in use)
	working, err := os.MkdirTemp("", "TestMigrateWorkingSet*")
	require.NoError(t, err)

	homeDir, err := os.MkdirTemp("", "TestMigrateWorkingSet*")
	require.NoError(t, err)

	dEnv := createFileTestEnv(t, working, homeDir)
	assert.NoError(t, dEnv.CfgLoadErr)

	err = dEnv.InitRepo(context.Background(), types.Format_Default, "aoeu aoeu", "aoeu@aoeu.org", DefaultInitBranch)
	require.NoError(t, err)
	defer dEnv.DoltDB(ctx).Close()

	ws, err := dEnv.WorkingSet(context.Background())
	require.NoError(t, err)

	// Make a new repo with the contents of this one, but with the working set cleared out and the repo state filled in
	// with the legacy values

	// We don't have a merge in progress, so we'll just fake one. We're only interested in seeing the fields loaded and
	// persisted to the working set
	commit, err := dEnv.DoltDB(ctx).ResolveCommitRef(context.Background(), dEnv.RepoState.CWBHeadRef())
	require.NoError(t, err)
	ws.StartMerge(commit, "HEAD")

	workingRoot := ws.WorkingRoot()
	stagedRoot := ws.StagedRoot()

	workingHash, err := workingRoot.HashOf()
	require.NoError(t, err)
	stagedHash, err := stagedRoot.HashOf()
	require.NoError(t, err)

	rs := repoStateLegacyFromRepoState(dEnv.RepoState)
	rs.Working = workingHash.String()
	rs.Staged = stagedHash.String()

	commitHash, err := commit.HashOf()
	require.NoError(t, err)
	rs.Merge = &mergeState{
		Commit:          commitHash.String(),
		PreMergeWorking: workingHash.String(),
	}

	// Clear the working set
	require.NoError(t, dEnv.DoltDB(ctx).DeleteWorkingSet(context.Background(), ws.Ref()))

	// Make sure it's gone
	_, err = dEnv.WorkingSet(context.Background())
	require.Equal(t, doltdb.ErrWorkingSetNotFound, err)

	// Now write the repo state file to disk and re-load the repo
	require.NoError(t, rs.save(dEnv.FS))

	dEnv = Load(context.Background(), testHomeDirFunc, dEnv.FS, doltdb.LocalDirDoltDB, "test")
	assert.NoError(t, dEnv.RSLoadErr)
	assert.NoError(t, dEnv.CfgLoadErr)

	ws, err = dEnv.WorkingSet(context.Background())
	require.NoError(t, err)

	assert.Equal(t, mustHash(workingRoot.HashOf()), mustHash(ws.WorkingRoot().HashOf()))
	assert.Equal(t, mustHash(stagedRoot.HashOf()), mustHash(ws.StagedRoot().HashOf()))
	assert.Equal(t, mustHash(commit.HashOf()), mustHash(ws.MergeState().Commit().HashOf()))
	assert.Equal(t, mustHash(workingRoot.HashOf()), mustHash(ws.MergeState().PreMergeWorkingRoot().HashOf()))
}

func isCWDEmpty(dEnv *DoltEnv) bool {
	isEmpty := true
	dEnv.FS.Iter("./", true, func(_ string, _ int64, _ bool) bool {
		isEmpty = false
		return true
	})

	return isEmpty
}

func mustHash(hash hash.Hash, err error) hash.Hash {
	if err != nil {
		panic(err)
	}
	return hash
}

func TestBestEffortDelete(t *testing.T) {
	dEnv, _ := createTestEnv(true, true)

	if isCWDEmpty(dEnv) {
		t.Error("Dir should not be empty before delete.")
	}

	dEnv.bestEffortDeleteAll(workingDir)

	if !isCWDEmpty(dEnv) {
		t.Error("Dir should be empty after delete.")
	}
}
