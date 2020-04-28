// Copyright 2020 Liquidata, Inc.
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

package sqlserver

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestUnmarshall(t *testing.T) {
	testStr := `
log_level: info

behavior:
    read_only: false
    autocommit: true

user:
    name: root
    password: ""

listener:
    host: localhost
    port: 3306
    max_connections: 1
    read_timeout_millis: 30000
    write_timeout_millis: 30000
    
databases:
    - name: irs_soi
      path: ./datasets/irs-soi
    - name: noaa
      path: /Users/brian/datasets/noaa
`

	expected := serverConfigAsYAMLConfig(DefaultServerConfig())
	expected.DatabaseConfig = []DatabaseYAMLConfig{
		{
			Name: "irs_soi",
			Path: "./datasets/irs-soi",
		},
		{
			Name: "noaa",
			Path: "/Users/brian/datasets/noaa",
		},
	}

	config := YAMLConfig{}
	err := yaml.Unmarshal([]byte(testStr), &config)
	require.NoError(t, err)
	assert.Equal(t, expected, config)
}

func TestYAMLConfigDefaults(t *testing.T) {
	var cfg YAMLConfig
	err := yaml.Unmarshal([]byte{}, &cfg)
	require.NoError(t, err)

	assert.Equal(t, defaultHost, cfg.Host())
	assert.Equal(t, defaultPort, cfg.Port())
	assert.Equal(t, defaultUser, cfg.User())
	assert.Equal(t, defaultPass, cfg.Password())
	assert.Equal(t, uint64(defaultTimeout), cfg.WriteTimeout())
	assert.Equal(t, uint64(defaultTimeout), cfg.ReadTimeout())
	assert.Equal(t, defaultReadOnly, cfg.ReadOnly())
	assert.Equal(t, defaultLogLevel, cfg.LogLevel())
	assert.Equal(t, defaultAutoCommit, cfg.AutoCommit())
	assert.Equal(t, uint64(defaultMaxConnections), cfg.MaxConnections())
}
