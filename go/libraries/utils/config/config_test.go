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

package config

import (
	"strings"
	"testing"
)

func TestConfigGetters(t *testing.T) {
	mc := NewMapConfig(map[string]string{
		"string":    "this is a string",
		"int":       "-15",
		"uint":      "1234567",
		"float":     "3.1415",
		"bad_int":   "1a2b3c",
		"bad_float": "1.2.3.4",
		"bad_uint":  "-123456",
	})

	if _, err := GetString(mc, "missing"); err != ErrConfigParamNotFound {
		t.Error("missing failure")
	}

	if v, err := GetString(mc, "string"); v != "this is a string" || err != nil {
		t.Error("string failure")
	}

	if v, err := GetInt(mc, "int"); v != -15 || err != nil {
		t.Error("int failure")
	}

	if v, err := GetFloat(mc, "float"); v != 3.1415 || err != nil {
		t.Error("float failure")
	}

	if v, err := GetUint(mc, "uint"); v != 1234567 || err != nil {
		t.Error("uint failure")
	}

	if _, err := GetInt(mc, "bad_int"); err == nil {
		t.Error("bad_int failure")
	}

	if _, err := GetFloat(mc, "bad_float"); err == nil {
		t.Error("baf_float failure")
	}

	if _, err := GetUint(mc, "bad_uint"); err == nil {
		t.Error("bad_uint failure")
	}
}

func TestConfigSetters(t *testing.T) {
	mc := NewMapConfig(map[string]string{
		"string": "initial",
	})

	err := SetStrings(mc, map[string]string{
		"string":     "updated",
		"new_string": "new_value",
	})

	if err != nil {
		t.Error("Error setting strings")
	}

	SetInt(mc, "int", -15)
	SetFloat(mc, "float", 3.1415)
	SetUint(mc, "uint", 1234567)

	if str, err := mc.GetString("string"); err != nil || str != "updated" {
		t.Error("string failure")
	}

	if str, err := mc.GetString("new_string"); err != nil || str != "new_value" {
		t.Error("new_string failure")
	}

	if str, err := mc.GetString("int"); err != nil || str != "-15" {
		t.Error("int failure")
	}

	if str, err := mc.GetString("float"); err != nil || !strings.HasPrefix(str, "3.1415") {
		t.Error("float failure")
	}

	if str, err := mc.GetString("uint"); err != nil || str != "1234567" {
		t.Error("uint failure")
	}
}

func testIteration(t *testing.T, expected map[string]string, cfg ReadableConfig) {
	cfg.Iter(func(name string, value string) (stop bool) {
		if expectedVal, ok := expected[name]; ok {
			if expectedVal == value {
				delete(expected, name)
			} else {
				t.Error("When iterating value of " + name + " had an unexpected value.")
			}
		} else if !ok {
			t.Error("Iterated over unexpected value " + name)
		}

		return false
	})

	if len(expected) != 0 {
		t.Error("Iteration did not iterate over all expected results.")
	}
}
