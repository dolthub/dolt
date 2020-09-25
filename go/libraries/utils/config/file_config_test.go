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
	"testing"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const (
	cfgPath = "/home/bheni/.ld/config.json"
)

func TestGetAndSet(t *testing.T) {
	fs := filesys.NewInMemFS([]string{}, map[string][]byte{}, "/")
	cfg, err := NewFileConfig(cfgPath, fs, map[string]string{})

	if err != nil {
		t.Fatal("Failed to create empty config")
	}

	params := map[string]string{
		"string": "this is a string",
		"int":    "-15",
		"uint":   "1234567",
		"float":  "3.1415",
	}

	err = cfg.SetStrings(params)

	if err != nil {
		t.Fatal("Failed to set values")
	}

	if exists, isDir := fs.Exists(cfgPath); !exists || isDir {
		t.Fatal("File not written after SetStrings was called")
	}

	cfg, err = FromFile(cfgPath, fs)

	if err != nil {
		t.Fatal("Error reading config")
	}

	if str, err := cfg.GetString("string"); err != nil || str != "this is a string" {
		t.Error("Failed to read back string after setting it")
	}

	testIteration(t, params, cfg)

	err = cfg.Unset([]string{"int", "float"})

	if err != nil {
		t.Error("Failed to unset properties")
	}

	testIteration(t, map[string]string{"string": "this is a string", "uint": "1234567"}, cfg)
}
