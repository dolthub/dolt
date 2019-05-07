package env

import (
	"context"
	"encoding/json"
	"github.com/attic-labs/noms/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"path/filepath"
	"testing"
)

const (
	testHomeDir = "/user/bheni"
	workingDir  = "/user/bheni/datasets/addresses"
)

func testHomeDirFunc() (string, error) {
	return testHomeDir, nil
}

func createTestEnv(isInitialized bool, hasLocalConfig bool) *DoltEnv {
	initialDirs := []string{testHomeDir, workingDir}
	initialFiles := map[string][]byte{}

	if isInitialized {
		doltDir := filepath.Join(workingDir, doltdb.DoltDir)
		initialDirs = append(initialDirs, doltDir)

		hashStr := hash.Hash{}.String()
		masterRef := ref.NewBranchRef("master")
		repoState := &RepoState{ref.MarshalableRef{masterRef}, hashStr, hashStr, nil, nil, nil}
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
	dEnv := Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB)

	return dEnv
}

func TestNonRepoDir(t *testing.T) {
	dEnv := createTestEnv(false, false)

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
	dEnv := createTestEnv(true, true)

	if !dEnv.HasDoltDir() || !dEnv.HasLocalConfig() {
		t.Fatal("local config and .dolt dir should have been created")
	}

	if dEnv.CfgLoadErr != nil {
		t.Error("Only global config load / create error should result in an error")
	}

	if dEnv.RSLoadErr != nil {
		t.Error("Repostate should be valid for an initialized directory")
	}

	if un, err := dEnv.Config.GetString("user.name"); err != nil || un != "bheni" {
		t.Error("Bad local config value.")
	}
}

func TestRepoDirNoLocal(t *testing.T) {
	dEnv := createTestEnv(true, false)

	if !dEnv.HasDoltDir() {
		t.Fatal(".dolt dir should exist.")
	} else if dEnv.HasLocalConfig() {
		t.Fatal("This should not be here before creation")
	}

	if dEnv.CfgLoadErr != nil {
		t.Error("Only global config load / create error should result in an error")
	}

	if dEnv.RSLoadErr != nil {
		t.Error("File doesn't exist.  There should be an error if the directory doesn't exist.")
	}

	dEnv.Config.CreateLocalConfig(map[string]string{"user.name": "bheni"})

	if !dEnv.HasLocalConfig() {
		t.Error("Failed to create local config file")
	}

	if un, err := dEnv.Config.GetString("user.name"); err != nil || un != "bheni" {
		t.Error("Bad local config value.")
	}
}

func TestInitRepo(t *testing.T) {
	dEnv := createTestEnv(false, false)
	err := dEnv.InitRepo(context.Background(), "aoeu aoeu", "aoeu@aoeu.org")

	if err != nil {
		t.Error("Failed to init repo.", err.Error())
	}

	_, err = dEnv.WorkingRoot(context.Background())

	if err != nil {
		t.Error("Failed to get working root value.")
	}

	_, err = dEnv.StagedRoot(context.Background())

	if err != nil {
		t.Error("Failed to get staged root value.")
	}
}

func isCWDEmpty(dEnv *DoltEnv) bool {
	isEmpty := true
	dEnv.FS.Iter("./", true, func(_ string, _ int64, _ bool) bool {
		isEmpty = false
		return true
	})

	return isEmpty
}

func TestBestEffortDelete(t *testing.T) {
	dEnv := createTestEnv(true, true)

	if isCWDEmpty(dEnv) {
		t.Error("Dir should not be empty before delete.")
	}

	dEnv.bestEffortDeleteAll(workingDir)

	if !isCWDEmpty(dEnv) {
		t.Error("Dir should be empty after delete.")
	}
}
