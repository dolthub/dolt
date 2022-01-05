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
    name: root
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

	config, err := NewYamlConfig([]byte(testStr))
	require.NoError(t, err)
	assert.Equal(t, expected, config)
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
	assert.Equal(t, false, cfg.DisableClientMultiStatements())
	assert.Equal(t, defaultMetricsHost, cfg.MetricsHost())
	assert.Equal(t, defaultMetricsPort, cfg.MetricsPort())
	assert.Nil(t, cfg.MetricsConfig.Labels)

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
	assert.Len(t, c.Certificates[0].Certificate, 2)

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
