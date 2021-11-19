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

package commands

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

var globalCfg = set.NewStrSet([]string{globalParamName})
var localCfg = set.NewStrSet([]string{localParamName})
var multiCfg = set.NewStrSet([]string{globalParamName, localParamName})

func initializeConfigs(dEnv *env.DoltEnv, element env.ConfigScope) {
	switch element {
	case env.GlobalConfig:
		globalCfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
		globalCfg.SetStrings(map[string]string{"title": "senior dufus"})
	case env.LocalConfig:
		dEnv.Config.CreateLocalConfig(map[string]string{"title": "senior dufus"})
	}
}
func TestConfigAdd(t *testing.T) {
	tests := []struct {
		Name   string
		CfgSet *set.StrSet
		Scope  env.ConfigScope
		Args   []string
		Code   int
	}{
		{
			Name:   "local",
			CfgSet: localCfg,
			Scope:  env.LocalConfig,
			Args:   []string{"title", "senior dufus"},
		},
		{
			Name:   "global",
			CfgSet: globalCfg,
			Scope:  env.GlobalConfig,
			Args:   []string{"title", "senior dufus"},
		},
		{
			Name:   "default",
			CfgSet: &set.StrSet{},
			Scope:  env.LocalConfig,
			Args:   []string{"title", "senior dufus"},
		},
		{
			Name:   "multi error",
			CfgSet: multiCfg,
			Scope:  env.LocalConfig,
			Args:   []string{"title", "senior dufus"},
			Code:   1,
		},
		{
			Name:   "no args",
			CfgSet: multiCfg,
			Scope:  env.LocalConfig,
			Args:   []string{},
			Code:   1,
		},
		{
			Name:   "odd args",
			CfgSet: multiCfg,
			Scope:  env.LocalConfig,
			Args:   []string{"title"},
			Code:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			dEnv := createTestEnv()
			resCode := addOperation(dEnv, tt.CfgSet, tt.Args, func() {})

			if tt.Code == 1 {
				assert.Equal(t, tt.Code, resCode)

			} else if cfg, ok := dEnv.Config.GetConfig(tt.Scope); ok {
				resVal := cfg.GetStringOrDefault("title", "")
				assert.Equal(t, "senior dufus", resVal)
			} else {
				t.Error("comparison config not found")
			}
		})
	}
}

func TestConfigGet(t *testing.T) {
	tests := []struct {
		Name       string
		CfgSet     *set.StrSet
		ConfigElem env.ConfigScope
		Key        string
		Code       int
	}{
		{
			Name:       "local",
			CfgSet:     localCfg,
			ConfigElem: env.LocalConfig,
			Key:        "title",
		},
		{
			Name:       "global",
			CfgSet:     globalCfg,
			ConfigElem: env.GlobalConfig,
			Key:        "title",
		},
		{
			Name:       "default",
			CfgSet:     &set.StrSet{},
			ConfigElem: env.LocalConfig,
			Key:        "title",
		},
		{
			Name:       "multi",
			CfgSet:     multiCfg,
			ConfigElem: env.LocalConfig,
			Key:        "title",
			Code:       1,
		},
		{
			Name:       "missing param",
			CfgSet:     multiCfg,
			ConfigElem: env.LocalConfig,
			Key:        "unknown",
			Code:       1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			dEnv := createTestEnv()
			initializeConfigs(dEnv, tt.ConfigElem)

			var resVal string
			resCode := getOperation(dEnv, tt.CfgSet, []string{tt.Key}, func(k string, v *string) { resVal = *v })

			if tt.Code == 1 {
				assert.Equal(t, tt.Code, resCode)
			} else {
				assert.Equal(t, "senior dufus", resVal)
			}
		})
	}
}

func TestConfigUnset(t *testing.T) {
	tests := []struct {
		Name       string
		CfgSet     *set.StrSet
		ConfigElem env.ConfigScope
		Key        string
		Code       int
	}{
		{
			Name:       "local",
			CfgSet:     localCfg,
			ConfigElem: env.LocalConfig,
			Key:        "title",
		},
		{
			Name:       "global",
			CfgSet:     globalCfg,
			ConfigElem: env.GlobalConfig,
			Key:        "title",
		},
		{
			Name:       "default",
			CfgSet:     &set.StrSet{},
			ConfigElem: env.LocalConfig,
			Key:        "title",
		},
		{
			Name:       "multi",
			CfgSet:     multiCfg,
			ConfigElem: env.LocalConfig,
			Key:        "title",
			Code:       1,
		},
		{
			Name:       "missing param",
			CfgSet:     multiCfg,
			ConfigElem: env.LocalConfig,
			Key:        "unknown",
			Code:       1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			dEnv := createTestEnv()
			initializeConfigs(dEnv, tt.ConfigElem)

			resCode := unsetOperation(dEnv, tt.CfgSet, []string{tt.Key}, func() {})

			if tt.Code == 1 {
				assert.Equal(t, tt.Code, resCode)
			} else if cfg, ok := dEnv.Config.GetConfig(tt.ConfigElem); ok {
				_, err := cfg.GetString(tt.Key)
				assert.Error(t, err)
			} else {
				t.Error("comparison config not found")
			}
		})
	}
}

func TestConfigList(t *testing.T) {
	tests := []struct {
		Name       string
		CfgSet     *set.StrSet
		ConfigElem env.ConfigScope
	}{
		{
			Name:       "local",
			CfgSet:     localCfg,
			ConfigElem: env.LocalConfig,
		},
		{
			Name:       "global",
			CfgSet:     globalCfg,
			ConfigElem: env.GlobalConfig,
		},
		{
			Name:       "default",
			CfgSet:     &set.StrSet{},
			ConfigElem: env.LocalConfig,
		},
		{
			Name:       "multi",
			CfgSet:     multiCfg,
			ConfigElem: env.LocalConfig,
		},
	}

	keys := []string{"title"}
	values := []string{"senior dufus"}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			dEnv := createTestEnv()
			initializeConfigs(dEnv, tt.ConfigElem)

			var resKeys []string
			var resVals []string
			resCode := listOperation(dEnv, tt.CfgSet, []string{}, func() {}, func(k, v string) {
				resKeys = append(resKeys, k)
				resVals = append(resVals, v)
			})
			assert.Equal(t, 0, resCode)
			assert.Equal(t, keys, resKeys)
			assert.Equal(t, values, resVals)
		})
	}
}
func TestConfig(t *testing.T) {
	ctx := context.TODO()
	dEnv := createTestEnv()

	configCmd := ConfigCmd{}
	ret := configCmd.Exec(ctx, "dolt config", []string{"-global", "--add", "name", "bheni"}, dEnv)
	ret += configCmd.Exec(ctx, "dolt config", []string{"-global", "--add", "title", "dufus"}, dEnv)

	expectedGlobal := map[string]string{
		"name":  "bheni",
		"title": "dufus",
	}

	if ret != 0 {
		t.Error("Failed to set global config")
	} else if cfg, ok := dEnv.Config.GetConfig(env.GlobalConfig); !ok || !config.Equals(cfg, expectedGlobal) {
		t.Error("config -add did not yield expected global results")
	}

	ret = configCmd.Exec(ctx, "dolt config", []string{"-local", "--add", "title", "senior dufus"}, dEnv)

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

	ret = configCmd.Exec(ctx, "dolt config", []string{"-global", "--unset", "name"}, dEnv)

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
	ctx := context.TODO()
	dEnv := createTestEnv()
	configCmd := ConfigCmd{}

	// local and global flags passed together is invalid
	ret := configCmd.Exec(ctx, "dolt config", []string{"--global", "--local", "--add", "name", "bheni"}, dEnv)

	if ret == 0 {
		t.Error("Invalid commands should fail. Command has both local and global")
	}

	// both -add and -get are used
	ret = configCmd.Exec(ctx, "dolt config", []string{"-global", "--get", "--add", "title"}, dEnv)

	if ret == 0 {
		t.Error("Invalid commands should fail. Command is missing local/global")
	}
}
