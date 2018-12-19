package commands

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/config"
	"reflect"
	"testing"
)

func TestSplitKeyValuePairs(t *testing.T) {
	kvps, err := splitKeyValPairs([]string{})

	if err != nil || kvps == nil || len(kvps) != 0 {
		t.Error("Failed to split empty args")
	}

	kvps, err = splitKeyValPairs(nil)

	if err != nil || kvps == nil || len(kvps) != 0 {
		t.Error("Failed to split empty args")
	}

	kvps, err = splitKeyValPairs([]string{"key:value", "key2:value:2"})
	expected := map[string]string{"key": "value", "key2": "value:2"}

	if err != nil {
		t.Error("Failed to split args")
	} else if !reflect.DeepEqual(kvps, expected) {
		t.Error("expected:", expected, "actual:", kvps)
	}

	kvps, err = splitKeyValPairs([]string{"no_colon_arg"})

	if err == nil {
		t.Error("Unexpected success.")
	}
}

func TestConfig(t *testing.T) {
	cliEnv := createTestEnv()
	ret := Config("dolt config", []string{"-global", "-set", "name:bheni", "title:dufus"}, cliEnv)

	expectedGlobal := map[string]string{
		"name":  "bheni",
		"title": "dufus",
	}

	if ret != 0 {
		t.Error("Failed to set global config")
	} else if cfg, ok := cliEnv.Config.GetConfig(env.GlobalConfig); !ok || !config.Equals(cfg, expectedGlobal) {
		t.Error("config -set did not yield expected global results")
	}

	ret = Config("dolt config", []string{"-local", "-set", "title:senior dufus"}, cliEnv)

	expectedLocal := map[string]string{
		"title": "senior dufus",
	}

	if ret != 0 {
		t.Error("Failed to set local config")
	} else if cfg, ok := cliEnv.Config.GetConfig(env.LocalConfig); !ok || !config.Equals(cfg, expectedLocal) {
		t.Error("config -set did not yield expected local results")
	} else if val, err := cfg.GetString("title"); err != nil || val != "senior dufus" {
		t.Error("Unexpected value of \"title\" retrieved from the config hierarchy")
	}

	ret = Config("dolt config", []string{"-global", "-unset", "name"}, cliEnv)

	expectedGlobal = map[string]string{
		"title": "dufus",
	}

	if ret != 0 {
		t.Error("Failed to set global config")
	} else if cfg, ok := cliEnv.Config.GetConfig(env.GlobalConfig); !ok || !config.Equals(cfg, expectedGlobal) {
		t.Error("config -set did not yield expected global results")
	}

	expectedGlobal = map[string]string{"title": "dufus"}
	globalProperties := map[string]string{}
	ret = listOperation(cliEnv, true, func(k string, v string) {
		globalProperties[k] = v
	})

	if ret != 0 {
		t.Error("Failed to list global config")
	} else if !reflect.DeepEqual(globalProperties, expectedGlobal) {
		t.Error("listOperation did not yield expected global results")
	}

	expectedLocal = map[string]string{"title": "senior dufus"}
	localProperties := map[string]string{}
	ret = listOperation(cliEnv, false, func(k string, v string) {
		localProperties[k] = v
	})

	if ret != 0 {
		t.Error("Failed to list local config")
	} else if !reflect.DeepEqual(localProperties, expectedLocal) {
		t.Error("listOperation did not yield expected local results")
	}

	ret = getOperation(cliEnv, true, []string{"title"}, func(k string, v *string) {
		if v == nil || *v != "dufus" {
			t.Error("Failed to get expected value for title.")
		}
	})

	if ret != 0 {
		t.Error("get operation failed")
	}

	ret = getOperation(cliEnv, true, []string{"name"}, func(k string, v *string) {
		if v != nil {
			t.Error("Failed to get expected value for \"name\" which should not be set in the cofig.")
		}
	})

	if ret != 0 {
		t.Error("get operation failed")
	}
}

func TestInvalidConfigArgs(t *testing.T) {
	cliEnv := createTestEnv()

	// local and global flags passed together is invalid
	ret := Config("dolt config", []string{"-global", "-local", "-set", "name:bheni", "title:dufus"}, cliEnv)

	if ret == 0 {
		t.Error("Invalid commands should fail. Command has both local and global")
	}

	// missing local and global flags is invalid
	ret = Config("dolt config", []string{"-set", "name:bheni", "title:dufus"}, cliEnv)

	if ret == 0 {
		t.Error("Invalid commands should fail. Command is missing local/global")
	}

	// both -set and -get are used
	ret = Config("dolt config", []string{"-global", "-set", "-get", "title"}, cliEnv)

	if ret == 0 {
		t.Error("Invalid commands should fail. Command is missing local/global")
	}
}
