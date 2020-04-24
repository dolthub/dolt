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
log_level: debug

behavior:
    read_only: false
    autocommit: true

user:
    name: root
    password: 1234

listener:
    host: 0.0.0.0
    port: 3306
    max_connections: 100
    read_timeout_millis: 0
    write_timeout_millis: 0
    
databases:
    - name: irs_soi
      path: ./datasets/irs-soi
    - name: noaa
      path: /Users/brian/datasets/noaa
`

	expected := YAMLConfig{
		LogLevelStr: "debug",
		BehaviorConfig: BehaviorYAMLConfig{
			ReadOnly:   false,
			AutoCommit: true,
		},
		UserConfig: UserYAMLConfig{
			Name:     "root",
			Password: "1234",
		},
		ListenerConfig: ListenerYAMLConfig{
			HostStr:            "0.0.0.0",
			PortNumber:         3306,
			MaxConnectoins:     100,
			ReadTimeoutMillis:  0,
			WriteTimeoutMillis: 0,
		},
		DatabaseConfig: []DatabaseYAMLConfig{
			{
				Name: "irs_soi",
				Path: "./datasets/irs-soi",
			},
			{
				Name: "noaa",
				Path: "/Users/brian/datasets/noaa",
			},
		},
	}

	config := YAMLConfig{}
	err := yaml.Unmarshal([]byte(testStr), &config)
	require.NoError(t, err)
	assert.Equal(t, expected, config)
}
