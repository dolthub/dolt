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

package servercfg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

var trueValue = true

func TestUnmarshall(t *testing.T) {
	testStr := `
log_level: info

behavior:
    read_only: false
    autocommit: true
    dolt_transaction_commit: true
    disable_client_multi_statements: false
    event_scheduler: ON

user:
    name: ""
    password: ""

listener:
    host: localhost
    port: 3306
    max_connections: 100
    read_timeout_millis: 28800000
    write_timeout_millis: 28800000
    
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

privilege_file: some other nonsense

branch_control_file: third nonsense

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
	expected := ServerConfigAsYAMLConfig(DefaultServerConfig())

	expected.BehaviorConfig.DoltTransactionCommit = &trueValue
	expected.CfgDirStr = nillableStrPtr("")
	expected.PrivilegeFile = ptr("some other nonsense")
	expected.BranchControlFile = ptr("third nonsense")

	expected.MetricsConfig = MetricsYAMLConfig{
		Host: ptr("123.45.67.89"),
		Port: ptr(9091),
		Labels: map[string]string{
			"label1": "value1",
			"label2": "2",
			"label3": "true",
		},
	}
	expected.DataDirStr = ptr("some nonsense")
	expected.SystemVars_ = nil
	expected.Vars = []UserSessionVars{
		{
			Name: "user0",
			Vars: map[string]interface{}{
				"var1": "val0_1",
				"var2": "val0_2",
				"var3": "val0_3",
			},
		},
		{
			Name: "user1",
			Vars: map[string]interface{}{
				"var1": "val1_1",
				"var2": "val1_2",
				"var4": "val1_4",
			},
		},
	}
	expected.Jwks = []JwksConfig{
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
	assert.Equal(t, expected, config, "Expected:\n%v\nActual:\n%v", expected, config)
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

	assert.Equal(t, DefaultHost, cfg.Host())
	assert.Equal(t, DefaultPort, cfg.Port())
	assert.Equal(t, DefaultUser, cfg.User())
	assert.Equal(t, DefaultPass, cfg.Password())
	assert.Equal(t, uint64(DefaultTimeout), cfg.WriteTimeout())
	assert.Equal(t, uint64(DefaultTimeout), cfg.ReadTimeout())
	assert.Equal(t, DefaultReadOnly, cfg.ReadOnly())
	assert.Equal(t, DefaultLogLevel, cfg.LogLevel())
	assert.Equal(t, DefaultAutoCommit, cfg.AutoCommit())
	assert.Equal(t, DefaultDoltTransactionCommit, cfg.DoltTransactionCommit())
	assert.Equal(t, uint64(DefaultMaxConnections), cfg.MaxConnections())
	assert.Equal(t, "", cfg.TLSKey())
	assert.Equal(t, "", cfg.TLSCert())
	assert.Equal(t, false, cfg.RequireSecureTransport())
	assert.Equal(t, false, cfg.AllowCleartextPasswords())
	assert.Equal(t, false, cfg.DisableClientMultiStatements())
	assert.Equal(t, DefaultMetricsHost, cfg.MetricsHost())
	assert.Equal(t, DefaultMetricsPort, cfg.MetricsPort())
	assert.Nil(t, cfg.MetricsConfig.Labels)
	assert.Equal(t, DefaultAllowCleartextPasswords, cfg.AllowCleartextPasswords())
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

func TestYAMLConfigMetrics(t *testing.T) {
	var cfg YAMLConfig
	err := yaml.Unmarshal([]byte(`
metrics:
  host: localhost
  port: null
`), &cfg)
	require.NoError(t, err)

	assert.Equal(t, "localhost", cfg.MetricsHost())
	assert.Equal(t, -1, cfg.MetricsPort())
}

func TestCommentNullYAMLValues(t *testing.T) {
	toComment := `
value1: value
value2: null
null: value
nest1:
  value1: null
  value2: value
  nest2:
    value1: "null"
    nest3:
	           with_many_spaces: null
`

	withComments := `
value1: value
# value2: null
null: value
nest1:
  # value1: null
  value2: value
  nest2:
    value1: "null"
    nest3:
	           # with_many_spaces: null
`

	assert.Equal(t, withComments, commentNullYAMLValues(toComment))
}
