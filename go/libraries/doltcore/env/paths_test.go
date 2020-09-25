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
	"path/filepath"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
)

func TestGetGlobalCfgPath(t *testing.T) {
	homeDir := "/user/bheni"
	expected := filepath.Join(homeDir, dbfactory.DoltDir, globalConfig)
	actual, _ := getGlobalCfgPath(func() (string, error) {
		return homeDir, nil
	})

	if actual != expected {
		t.Error(actual, "!=", expected)
	}
}
