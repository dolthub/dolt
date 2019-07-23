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

package commands

import (
	"reflect"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/config"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/set"
)

var globalCfg = set.NewStrSet([]string{globalParamName})
var localCfg = set.NewStrSet([]string{localParamName})

func TestConfig(t *testing.T) {
	dEnv := createTestEnv()
	ret := Config("dolt config", []string{"-global", "--add", "name", "bheni"}, dEnv)
	ret += Config("dolt config", []string{"-global", "--add", "title", "dufus"}, dEnv)

	expectedGlobal := map[string]string{
		"name":  "bheni",
		"title": "dufus",
	}

	if ret != 0 {
		t.Error("Failed to set global config")
	} else if cfg, ok := dEnv.Config.GetConfig(env.GlobalConfig); !ok || !config.Equals(cfg, expectedGlobal) {
		t.Error("config -add did not yield expected global results")
	}

	ret = Config("dolt config", []string{"-local", "--add", "title", "senior dufus"}, dEnv)

	expectedLocal := map[string]string{
		"title": "senior dufus",
	}

	if ret != 0 {
		t.Error("Failed to set local config")
	} else if cfg, ok := dEnv.Config.GetConfig(env.LocalConfig); !ok || !config.Equals(cfg, expectedLocal) {
		t.Error("config -add did not yield expected local results")
	} else if val, err := cfg.GetString("title"); err != nil || val != "senior dufus" {
		t.Error("Unexpected value of \"title\" retrieved from the config hierarchy")
	}

	ret = Config("dolt config", []string{"-global", "--unset", "name"}, dEnv)

	expectedGlobal = map[string]string{
		"title": "dufus",
	}

	if ret != 0 {
		t.Error("Failed to set global config")
	} else if cfg, ok := dEnv.Config.GetConfig(env.GlobalConfig); !ok || !config.Equals(cfg, expectedGlobal) {
		t.Error("config -add did not yield expected global results")
	}

	expectedGlobal = map[string]string{"title": "dufus"}
	globalProperties := map[string]string{}
	ret = listOperation(dEnv, globalCfg, []string{}, func() {}, func(k string, v string) {
		globalProperties[k] = v
	})

	if ret != 0 {
		t.Error("Failed to list global config")
	} else if !reflect.DeepEqual(globalProperties, expectedGlobal) {
		t.Error("listOperation did not yield expected global results")
	}

	expectedLocal = map[string]string{"title": "senior dufus"}
	localProperties := map[string]string{}
	ret = listOperation(dEnv, localCfg, []string{}, func() {}, func(k string, v string) {
		localProperties[k] = v
	})

	if ret != 0 {
		t.Error("Failed to list local config")
	} else if !reflect.DeepEqual(localProperties, expectedLocal) {
		t.Error("listOperation did not yield expected local results")
	}

	ret = getOperation(dEnv, globalCfg, []string{"title"}, func(k string, v *string) {
		if v == nil || *v != "dufus" {
			t.Error("Failed to get expected value for title.")
		}
	})

	if ret != 0 {
		t.Error("get operation failed")
	}

	ret = getOperation(dEnv, globalCfg, []string{"name"}, func(k string, v *string) {
		if v != nil {
			t.Error("Failed to get expected value for \"name\" which should not be set in the config.")
		}
	})

	if ret == 0 {
		t.Error("get operation should return 1 for a key not found")
	}
}

func TestInvalidConfigArgs(t *testing.T) {
	dEnv := createTestEnv()

	// local and global flags passed together is invalid
	ret := Config("dolt config", []string{"--global", "--local", "--add", "name", "bheni"}, dEnv)

	if ret == 0 {
		t.Error("Invalid commands should fail. Command has both local and global")
	}

	// both -add and -get are used
	ret = Config("dolt config", []string{"-global", "--get", "--add", "title"}, dEnv)

	if ret == 0 {
		t.Error("Invalid commands should fail. Command is missing local/global")
	}
}
