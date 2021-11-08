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

package env

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/config"
)

const (
	email = "bigbillieb@fake.horse"
	name  = "Billy Bob"
)

func TestConfig(t *testing.T) {
	dEnv, _ := createTestEnv(true, true)

	lCfg, _ := dEnv.Config.GetConfig(LocalConfig)
	gCfg, _ := dEnv.Config.GetConfig(GlobalConfig)

	lCfg.SetStrings(map[string]string{UserEmailKey: email, UserNameKey: "local_override"})
	gCfg.SetStrings(map[string]string{UserNameKey: name})

	assert.Equal(t, email, dEnv.Config.GetStringOrDefault(UserEmailKey, "no"))
	assert.Equal(t, "local_override", dEnv.Config.GetStringOrDefault(UserNameKey, "no"))
	assert.Equal(t, "yes", dEnv.Config.GetStringOrDefault("bad_key", "yes"))

	assert.Equal(t, email, dEnv.Config.IfEmptyUseConfig("", UserEmailKey))
	assert.Equal(t, "not empty", dEnv.Config.IfEmptyUseConfig("not empty", UserEmailKey))

	assert.Equal(t, "", dEnv.Config.IfEmptyUseConfig("", "missing"))

	_, err := dEnv.Config.GetString("missing")
	assert.Equal(t, config.ErrConfigParamNotFound, err)
}

func TestFailsafes(t *testing.T) {
	dEnv, _ := createTestEnv(true, true)

	lCfg, _ := dEnv.Config.GetConfig(LocalConfig)
	require.NoError(t, lCfg.Unset([]string{UserNameKey}))

	dEnv.Config.SetFailsafes(DefaultFailsafeConfig)

	assert.Equal(t, DefaultEmail, dEnv.Config.GetStringOrDefault(UserEmailKey, "none"))
	assert.Equal(t, DefaultName, dEnv.Config.GetStringOrDefault(UserNameKey, "none"))

	dEnv.Config.SetFailsafes(map[string]string{UserEmailKey: "new", "abc": "def"})

	assert.Equal(t, "new", dEnv.Config.GetStringOrDefault(UserEmailKey, "none"))
	assert.Equal(t, DefaultName, dEnv.Config.GetStringOrDefault(UserNameKey, "none"))
	assert.Equal(t, "def", dEnv.Config.GetStringOrDefault("abc", "none"))
}
