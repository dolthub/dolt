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

import "testing"

const (
	email = "bigbillieb@fake.horse"
	name  = "Billy Bob"
)

func TestConfig(t *testing.T) {
	dEnv := createTestEnv(true, true)

	lCfg, _ := dEnv.Config.GetConfig(LocalConfig)
	gCfg, _ := dEnv.Config.GetConfig(GlobalConfig)

	lCfg.SetStrings(map[string]string{UserEmailKey: email})
	gCfg.SetStrings(map[string]string{UserNameKey: name})

	if *dEnv.Config.GetStringOrDefault(UserEmailKey, "no") != email {
		t.Error("Should return", email)
	}

	if *dEnv.Config.GetStringOrDefault("bad_key", "yes") != "yes" {
		t.Error("Should return default value of yes")
	}

	if dEnv.Config.IfEmptyUseConfig("", UserEmailKey) != email {
		t.Error("Should return", email)
	}

	if dEnv.Config.IfEmptyUseConfig("not empty", UserEmailKey) != "not empty" {
		t.Error("Should return default value")
	}

	if dEnv.Config.IfEmptyUseConfig("", "missing") != "" {
		t.Error("Should return empty string")
	}
}
