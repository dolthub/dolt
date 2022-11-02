// Copyright 2020 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
)

func TestUnmarshall(t *testing.T) {
	testStr := `
log_level: info

behavior:
    read_only: false
    autocommit: true
    persistence_behavior: load
    disable_client_multi_statements: false

user:
    name: ""
    password: ""

listener:
    host: localhost
    port: 3306
    max_connections: 100
    read_timeout_millis: 28800000
    write_timeout_millis: 28800000
    
databases:
    - name: irs_soi
      path: ./datasets/irs-soi
    - name: noaa
      path: /Users/brian/datasets/noaa

data_dir: some nonsense

metrics:
    host: 123.45.67.89
    port: 9091
    labels:
        label1: value1
        label2: 2
        label3: true

user_session_vars:
    - name: user0
      vars:
          var1: val0_1
          var2: val0_2
          var3: val0_3
    - name: user1
      vars:
          var1: val1_1
          var2: val1_2
          var4: val1_4

jwks:
  - name: jwks_name
    location_url: https://website.com
    claims: 
      field1: a
      field2: b
    fields_to_log: [field1, field2]
  - name: jwks_name2
    location_url: https://website.com
    claims: 
      field1: a
    fields_to_log:
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
	expected.MetricsConfig = MetricsYAMLConfig{
		Host: strPtr("123.45.67.89"),
		Port: intPtr(9091),
		Labels: map[string]string{
			"label1": "value1",
			"label2": "2",
			"label3": "true",
		},
	}
	expected.DataDirStr = strPtr("some nonsense")
	expected.Vars = []UserSessionVars{
		{
			Name: "user0",
			Vars: map[string]string{
				"var1": "val0_1",
				"var2": "val0_2",
				"var3": "val0_3",
			},
		},
		{
			Name: "user1",
			Vars: map[string]string{
				"var1": "val1_1",
				"var2": "val1_2",
				"var4": "val1_4",
			},
		},
	}
	expected.Jwks = []engine.JwksConfig{
		{
			Name:        "jwks_name",
			LocationUrl: "https://website.com",
			Claims: map[string]string{
				"field1": "a",
				"field2": "b",
			},
			FieldsToLog: []string{"field1", "field2"},
		},
		{
			Name:        "jwks_name2",
			LocationUrl: "https://website.com",
			Claims: map[string]string{
				"field1": "a",
			},
			FieldsToLog: nil,
		},
	}

	config, err := NewYamlConfig([]byte(testStr))
	require.NoError(t, err)
	assert.Equal(t, expected, config)
}

func TestUnmarshallRemotesapiPort(t *testing.T) {
	testStr := `
remotesapi:
  port: 8000
`
	config, err := NewYamlConfig([]byte(testStr))
	require.NoError(t, err)
	require.NotNil(t, config.RemotesapiPort())
	require.Equal(t, 8000, *config.RemotesapiPort())
}

func TestUnmarshallCluster(t *testing.T) {
	testStr := `
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://doltdb-1.doltdb:50051/{database}
  bootstrap_role: primary
  bootstrap_epoch: 0
  remotesapi:
    port: 50051
`
	config, err := NewYamlConfig([]byte(testStr))
	require.NoError(t, err)
	require.NotNil(t, config.ClusterConfig())
	require.NotNil(t, config.ClusterConfig().RemotesAPIConfig())
	require.Equal(t, 50051, config.ClusterConfig().RemotesAPIConfig().Port())
	require.Len(t, config.ClusterConfig().StandbyRemotes(), 1)
	require.Equal(t, "primary", config.ClusterConfig().BootstrapRole())
	require.Equal(t, 0, config.ClusterConfig().BootstrapEpoch())
	require.Equal(t, "standby", config.ClusterConfig().StandbyRemotes()[0].Name())
	require.Equal(t, "http://doltdb-1.doltdb:50051/{database}", config.ClusterConfig().StandbyRemotes()[0].RemoteURLTemplate())
}

func TestValidateClusterConfig(t *testing.T) {
	cases := []struct {
		Name   string
		Config string
		Error  bool
	}{
		{
			Name:   "no cluster: config",
			Config: "",
			Error:  false,
		},
		{
			Name: "all fields valid",
			Config: `
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:50051/{database}
  bootstrap_role: primary
  bootstrap_epoch: 0
  remotesapi:
    port: 50051
`,
			Error: false,
		},
		{
			Name: "bad bootstrap_role",
			Config: `
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:50051/{database}
  bootstrap_role: backup
  bootstrap_epoch: 0
  remotesapi:
    port: 50051
`,
			Error: true,
		},
		{
			Name: "negative bootstrap_epoch",
			Config: `
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:50051/{database}
  bootstrap_role: primary
  bootstrap_epoch: -1
  remotesapi:
    port: 50051
`,
			Error: true,
		},
		{
			Name: "negative remotesapi port",
			Config: `
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:50051/{database}
  bootstrap_role: primary
  bootstrap_epoch: 0
  remotesapi:
    port: -5
`,
			Error: true,
		},
		{
			Name: "bad remote_url_template",
			Config: `
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:50051/{database
  bootstrap_role: primary
  bootstrap_epoch: 0
  remotesapi:
    port: 50051
`,
			Error: true,
		},
		{
			Name: "no standby remotes",
			Config: `
cluster:
  standby_remotes:
  bootstrap_role: primary
  bootstrap_epoch: 0
  remotesapi:
    port: 50051
`,
			Error: true,
		},
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			cfg, err := NewYamlConfig([]byte(c.Config))
			require.NoError(t, err)
			if c.Error {
				require.Error(t, ValidateClusterConfig(cfg.ClusterConfig()))
			} else {
				require.NoError(t, ValidateClusterConfig(cfg.ClusterConfig()))
			}
		})
	}
}

// Tests that a common YAML error (incorrect indentation) throws an error
func TestUnmarshallError(t *testing.T) {
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
    read_timeout_millis: 28800000
    write_timeout_millis: 28800000
    
databases:
    - name: irs_soi
      path: ./datasets/irs-soi
    - name: noaa
      path: /Users/brian/datasets/noaa
`
	_, err := NewYamlConfig([]byte(testStr))
	assert.Error(t, err)
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
	assert.Equal(t, "", cfg.TLSKey())
	assert.Equal(t, "", cfg.TLSCert())
	assert.Equal(t, false, cfg.RequireSecureTransport())
	assert.Equal(t, false, cfg.AllowCleartextPasswords())
	assert.Equal(t, false, cfg.DisableClientMultiStatements())
	assert.Equal(t, defaultMetricsHost, cfg.MetricsHost())
	assert.Equal(t, defaultMetricsPort, cfg.MetricsPort())
	assert.Nil(t, cfg.MetricsConfig.Labels)
	assert.Equal(t, defaultAllowCleartextPasswords, cfg.AllowCleartextPasswords())
	assert.Nil(t, cfg.RemotesapiPort())

	c, err := LoadTLSConfig(cfg)
	assert.NoError(t, err)
	assert.Nil(t, c)
}

func TestYAMLConfigTLS(t *testing.T) {
	var cfg YAMLConfig
	err := yaml.Unmarshal([]byte(`
listener:
  tls_key: testdata/selfsigned_key.pem
  tls_cert: testdata/selfsigned_cert.pem
`), &cfg)
	require.NoError(t, err)

	c, err := LoadTLSConfig(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Len(t, c.Certificates, 1)
	assert.Len(t, c.Certificates[0].Certificate, 1)

	err = yaml.Unmarshal([]byte(`
listener:
  tls_key: testdata/chain_key.pem
  tls_cert: testdata/chain_cert.pem
`), &cfg)
	require.NoError(t, err)

	c, err = LoadTLSConfig(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Len(t, c.Certificates, 1)
	assert.Len(t, c.Certificates[0].Certificate, 1)

	cfg = YAMLConfig{}
	err = yaml.Unmarshal([]byte(`
listener:
  tls_key: testdata/chain_key.pem
`), &cfg)
	require.NoError(t, err)
	c, err = LoadTLSConfig(cfg)
	assert.Error(t, err)

	cfg = YAMLConfig{}
	err = yaml.Unmarshal([]byte(`
listener:
  tls_cert: testdata/chain_cert.pem
`), &cfg)
	require.NoError(t, err)
	c, err = LoadTLSConfig(cfg)
	assert.Error(t, err)

	cfg = YAMLConfig{}
	err = yaml.Unmarshal([]byte(`
listener:
  tls_cert: testdata/doesnotexist_cert.pem
  tls_key: testdata/doesnotexist_key.pem
`), &cfg)
	require.NoError(t, err)
	c, err = LoadTLSConfig(cfg)
	assert.Error(t, err)

	cfg = YAMLConfig{}
	err = yaml.Unmarshal([]byte(`
listener:
  require_secure_transport: true
`), &cfg)
	require.NoError(t, err)
	err = ValidateConfig(cfg)
	assert.Error(t, err)
}
