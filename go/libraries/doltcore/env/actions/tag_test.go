package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/concurrentmap"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/stretchr/testify/require"
)

const (
	testHomeDir = "/user/bheni"
	workingDir  = "/user/bheni/datasets/addresses"
	credsDir    = "creds"

	configFile       = "config.json"
	GlobalConfigFile = "config_global.json"

	repoStateFile = "repo_state.json"
)

func testHomeDirFunc() (string, error) {
	return testHomeDir, nil
}

func createTestEnv(isInitialized bool, hasLocalConfig bool) (*env.DoltEnv, *filesys.InMemFS) {
	initialDirs := []string{testHomeDir, workingDir}
	initialFiles := map[string][]byte{}

	if isInitialized {
		doltDir := filepath.Join(workingDir, dbfactory.DoltDir)
		doltDataDir := filepath.Join(workingDir, dbfactory.DoltDataDir)
		initialDirs = append(initialDirs, doltDir)
		initialDirs = append(initialDirs, doltDataDir)

		mainRef := ref.NewBranchRef(env.DefaultInitBranch)
		repoState := &env.RepoState{Head: ref.MarshalableRef{Ref: mainRef}, Remotes: concurrentmap.New[string, env.Remote](), Backups: concurrentmap.New[string, env.Remote](), Branches: concurrentmap.New[string, env.BranchConfig]()}
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
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB, "test")

	return dEnv, fs
}

func TestIterResolvedTagsPage(t *testing.T) {
	dEnv, _ := createTestEnv(false, false)
	ctx := context.Background()

	// Initialize repo
	err := dEnv.InitRepo(ctx, types.Format_Default, "test user", "test@test.com", "main")
	require.NoError(t, err)

	totalTags := doltdb.DefaultRefPageSize + doltdb.DefaultRefPageSize

	expectedFirstPage := make(map[string]string)
	expectedSecondPage := make(map[string]string)
	// Create some commits and tags
	for i := 0; i < totalTags; i++ {
		name := fmt.Sprintf("tag%d", i)
		msg := fmt.Sprintf("tag message %d", i)
		err := CreateTag(ctx, dEnv, name, "main", TagProps{TaggerName: "test user", TaggerEmail: "test@test.com", Description: msg})
		require.NoError(t, err)
		if i < doltdb.DefaultRefPageSize {
			expectedFirstPage[name] = name
		} else {
			expectedSecondPage[name] = name
		}
	}

	var tags []string
	var pageToken *doltdb.RefPageToken

	// Test first page
	_, err = IterResolvedTagsPage(ctx, dEnv.DoltDB, nil, func(tag *doltdb.Tag) (bool, error) {
		tags = append(tags, tag.Name)
		return false, nil
	})
	require.NoError(t, err)

	// Verify tags are in the first page
	for _, tag := range tags {
		require.Contains(t, expectedFirstPage, tag)
	}

	// Test with small page size to verify pagination
	tags = []string{}
	pageToken = nil

	var nextToken *doltdb.RefPageToken
	for {
		nextToken, err = IterResolvedTagsPage(ctx, dEnv.DoltDB, pageToken, func(tag *doltdb.Tag) (bool, error) {
			tags = append(tags, tag.Name)
			return false, nil
		})
		require.NoError(t, err)

		if nextToken == nil {
			break
		}
		pageToken = nextToken
	}

	require.Equal(t, len(tags), totalTags)

	// Verify tags are in the first page
	for _, tag := range expectedFirstPage {
		require.Contains(t, tags, tag)
	}

	// Verify tags are in the second page
	for _, tag := range expectedSecondPage {
		require.Contains(t, tags, tag)
	}
}

func getLocalConfigPath() string {
	return filepath.Join(dbfactory.DoltDir, configFile)
}

func getRepoStateFile() string {
	return filepath.Join(dbfactory.DoltDir, repoStateFile)
}
