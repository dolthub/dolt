// Copyright 2019 Liquidata, Inc.
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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/events"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	testHomeDir      = "/user/bheni"
	workingDir       = "/user/bheni/datasets/addresses"
	testMasterBranch = "master"
	testOriginRemote = "https://dolthub.com/org/repo"
)

func testHomeDirFunc() (string, error) {
	return testHomeDir, nil
}

func createTestEnv(isInitialized bool, hasLocalConfig bool) *DoltEnv {
	initialDirs := []string{testHomeDir, workingDir}
	initialFiles := map[string][]byte{}

	if isInitialized {
		doltDir := filepath.Join(workingDir, dbfactory.DoltDir)
		initialDirs = append(initialDirs, doltDir)

		remote := NewRemote("origin", testOriginRemote, map[string]string{})
		//remotes := make(map[string]Remote, 0)
		//remotes["origin"] = remote
		hashStr := hash.Hash{}.String()
		masterRef := ref.NewBranchRef(testMasterBranch)
		repoState := &RepoState{ref.MarshalableRef{Ref: masterRef}, hashStr, hashStr, nil, nil, nil, nil}
		repoState.AddRemote(remote)
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

	err := dEnv.Config.CreateLocalConfig(map[string]string{"user.name": "bheni"})
	require.NoError(t, err)

	if !dEnv.HasLocalConfig() {
		t.Error("Failed to create local config file")
	}

	if un, err := dEnv.Config.GetString("user.name"); err != nil || un != "bheni" {
		t.Error("Bad local config value.")
	}
}

func TestInitRepo(t *testing.T) {
	dEnv := createTestEnv(false, false)
	err := dEnv.InitRepo(context.Background(), types.Format_7_18, "aoeu aoeu", "aoeu@aoeu.org")

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

func TestDoltEnv_SetStandardEventAttributes(t *testing.T) {
	dEnv := createTestEnv(true, false)

	collector := events.NewCollector()
	testEvent := events.NewEvent(eventsapi.ClientEventType_CLONE)

	dEnv.SetStandardEventAttributes(testEvent)

	collector.CloseEventAndAdd(testEvent)

	assert.Panics(t, func() {
		collector.CloseEventAndAdd(testEvent)
	})

	clientEvents := collector.Close()

	assert.Equal(t, 1, len(clientEvents))
	assert.Equal(t, 2, len(clientEvents[0].Attributes))

	assert.Equal(t, eventsapi.AttributeID_LOCAL_REMOTE_URLS, clientEvents[0].Attributes[0].Id)
	assert.Equal(t, testOriginRemote, clientEvents[0].Attributes[0].Value)

	assert.Equal(t, eventsapi.AttributeID_BRANCH_NAME, clientEvents[0].Attributes[1].Id)
	assert.Equal(t, testMasterBranch, clientEvents[0].Attributes[1].Value)
}
