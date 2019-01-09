package env

import (
	"encoding/json"
	"github.com/attic-labs/noms/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
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
		repoState := &RepoState{"master", hashStr, hashStr, nil}
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
	dEnv := Load(testHomeDirFunc, fs, doltdb.InMemDoltDB)

	return dEnv
}

func TestNonRepoDir(t *testing.T) {
	dEnv := createTestEnv(false, false)

	if !dEnv.IsCWDEmpty() {
		t.Error("Should start with a clean wd")
	}

	if dEnv.HasLDDir() || dEnv.HasLocalConfig() {
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

	if !dEnv.HasLDDir() || !dEnv.HasLocalConfig() {
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

	if !dEnv.HasLDDir() {
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
	err := dEnv.InitRepo("aoeu aoeu", "aoeu@aoeu.org")

	if err != nil {
		t.Error("Failed to init repo.", err.Error())
	}

	_, err = dEnv.WorkingRoot()

	if err != nil {
		t.Error("Failed to get working root value.")
	}

	_, err = dEnv.StagedRoot()

	if err != nil {
		t.Error("Failed to get staged root value.")
	}
}

func TestBestEffortDelete(t *testing.T) {
	dEnv := createTestEnv(true, true)

	if dEnv.IsCWDEmpty() {
		t.Error("Dir should not be empty before delete.")
	}

	dEnv.bestEffortDeleteAllFromCWD()

	if !dEnv.IsCWDEmpty() {
		t.Error("Dir should be empty after delete.")
	}
}
