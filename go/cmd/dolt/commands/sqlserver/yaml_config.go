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
	"strings"
	"unicode"
	"unicode/utf8"

	"gopkg.in/yaml.v2"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

func strPtr(s string) *string {
	return &s
}

func nillableStrPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nillableBoolPtr(b bool) *bool {
	if b == false {
		return nil
	}
	return &b
}

func boolPtr(b bool) *bool {
	return &b
}

func uint64Ptr(n uint64) *uint64 {
	return &n
}

func intPtr(n int) *int {
	return &n
}

// BehaviorYAMLConfig contains server configuration regarding how the server should behave
type BehaviorYAMLConfig struct {
	ReadOnly   *bool `yaml:"read_only"`
	AutoCommit *bool
	// PersistenceBehavior regulates loading persisted system variable configuration.
	PersistenceBehavior *string `yaml:"persistence_behavior"`
	// Disable processing CLIENT_MULTI_STATEMENTS support on the
	// sql server.  Dolt's handling of CLIENT_MULTI_STATEMENTS is currently
	// broken. If a client advertises to support it (mysql cli client
	// does), and then sends statements that contain embedded unquoted ';'s
	// (such as a CREATE TRIGGER), then those incoming queries will be
	// misprocessed.
	DisableClientMultiStatements *bool `yaml:"disable_client_multi_statements"`
}

// UserYAMLConfig contains server configuration regarding the user account clients must use to connect
type UserYAMLConfig struct {
	Name     *string
	Password *string
}

// DatabaseYAMLConfig contains information on a database that this server will provide access to
type DatabaseYAMLConfig struct {
	Name string
	Path string
}

// ListenerYAMLConfig contains information on the network connection that the server will open
type ListenerYAMLConfig struct {
	HostStr            *string `yaml:"host"`
	PortNumber         *int    `yaml:"port"`
	MaxConnections     *uint64 `yaml:"max_connections"`
	ReadTimeoutMillis  *uint64 `yaml:"read_timeout_millis"`
	WriteTimeoutMillis *uint64 `yaml:"write_timeout_millis"`
	// TLSKey is a file system path to an unencrypted private TLS key in PEM format.
	TLSKey *string `yaml:"tls_key"`
	// TLSCert is a file system path to a TLS certificate chain in PEM format.
	TLSCert *string `yaml:"tls_cert"`
	// RequireSecureTransport can enable a mode where non-TLS connections are turned away.
	RequireSecureTransport *bool `yaml:"require_secure_transport"`
}

// PerformanceYAMLConfig contains configuration parameters for performance tweaking
type PerformanceYAMLConfig struct {
	QueryParallelism *int `yaml:"query_parallelism"`
}

type MetricsYAMLConfig struct {
	Labels map[string]string `yaml:"labels"`
	Host   *string           `yaml:"host"`
	Port   *int              `yaml:"port"`
}

// YAMLConfig is a ServerConfig implementation which is read from a yaml file
type YAMLConfig struct {
	LogLevelStr       *string               `yaml:"log_level"`
	BehaviorConfig    BehaviorYAMLConfig    `yaml:"behavior"`
	UserConfig        UserYAMLConfig        `yaml:"user"`
	ListenerConfig    ListenerYAMLConfig    `yaml:"listener"`
	DatabaseConfig    []DatabaseYAMLConfig  `yaml:"databases"`
	PerformanceConfig PerformanceYAMLConfig `yaml:"performance"`
	DataDirStr        *string               `yaml:"data_dir"`
	MetricsConfig     MetricsYAMLConfig     `yaml:"metrics"`
	PrivilegeFile     *string               `yaml:"privilege_file"`
	MySQLDbFile       *string               `yaml:"mysql_db_file"`
}

var _ ServerConfig = YAMLConfig{}

func NewYamlConfig(configFileData []byte) (YAMLConfig, error) {
	var cfg YAMLConfig
	err := yaml.UnmarshalStrict(configFileData, &cfg)
	return cfg, err
}

func serverConfigAsYAMLConfig(cfg ServerConfig) YAMLConfig {
	return YAMLConfig{
		LogLevelStr: strPtr(string(cfg.LogLevel())),
		BehaviorConfig: BehaviorYAMLConfig{
			boolPtr(cfg.ReadOnly()),
			boolPtr(cfg.AutoCommit()),
			strPtr(cfg.PersistenceBehavior()),
			boolPtr(cfg.DisableClientMultiStatements()),
		},
		UserConfig: UserYAMLConfig{strPtr(cfg.User()), strPtr(cfg.Password())},
		ListenerConfig: ListenerYAMLConfig{
			strPtr(cfg.Host()),
			intPtr(cfg.Port()),
			uint64Ptr(cfg.MaxConnections()),
			uint64Ptr(cfg.ReadTimeout()),
			uint64Ptr(cfg.WriteTimeout()),
			nillableStrPtr(cfg.TLSKey()),
			nillableStrPtr(cfg.TLSCert()),
			nillableBoolPtr(cfg.RequireSecureTransport()),
		},
		DatabaseConfig: nil,
	}
}

// String returns the YAML representation of the config
func (cfg YAMLConfig) String() string {
	data, err := yaml.Marshal(cfg)

	if err != nil {
		return "Failed to marshal as yaml: " + err.Error()
	}

	unformatted := string(data)

	// format the yaml to be easier to read.
	lines := strings.Split(unformatted, "\n")

	var formatted []string
	formatted = append(formatted, lines[0])
	for i := 1; i < len(lines); i++ {
		if len(lines[i]) == 0 {
			continue
		}

		r, _ := utf8.DecodeRuneInString(lines[i])
		if !unicode.IsSpace(r) {
			formatted = append(formatted, "")
		}

		formatted = append(formatted, lines[i])
	}

	result := strings.Join(formatted, "\n")
	return result
}

// Host returns the domain that the server will run on. Accepts an IPv4 or IPv6 address, in addition to localhost.
func (cfg YAMLConfig) Host() string {
	if cfg.ListenerConfig.HostStr == nil {
		return defaultHost
	}

	return *cfg.ListenerConfig.HostStr
}

// Port returns the port that the server will run on. The valid range is [1024, 65535].
func (cfg YAMLConfig) Port() int {
	if cfg.ListenerConfig.PortNumber == nil {
		return defaultPort
	}

	return *cfg.ListenerConfig.PortNumber
}

// ReadTimeout returns the read timeout in milliseconds.
func (cfg YAMLConfig) ReadTimeout() uint64 {
	if cfg.ListenerConfig.ReadTimeoutMillis == nil {
		return defaultTimeout
	}

	return *cfg.ListenerConfig.ReadTimeoutMillis
}

// WriteTimeout returns the write timeout in milliseconds.
func (cfg YAMLConfig) WriteTimeout() uint64 {
	if cfg.ListenerConfig.WriteTimeoutMillis == nil {
		return defaultTimeout
	}

	return *cfg.ListenerConfig.WriteTimeoutMillis
}

// User returns the username that connecting clients must use.
func (cfg YAMLConfig) User() string {
	if cfg.UserConfig.Name == nil {
		return defaultUser
	}

	return *cfg.UserConfig.Name
}

// Password returns the password that connecting clients must use.
func (cfg YAMLConfig) Password() string {
	if cfg.UserConfig.Password == nil {
		return defaultPass
	}

	return *cfg.UserConfig.Password
}

// ReadOnly returns whether the server will only accept read statements or all statements.
func (cfg YAMLConfig) ReadOnly() bool {
	if cfg.BehaviorConfig.ReadOnly == nil {
		return defaultReadOnly
	}

	return *cfg.BehaviorConfig.ReadOnly
}

// Autocommit defines the value of the @@autocommit session variable used on every connection
func (cfg YAMLConfig) AutoCommit() bool {
	if cfg.BehaviorConfig.AutoCommit == nil {
		return defaultAutoCommit
	}

	return *cfg.BehaviorConfig.AutoCommit
}

// LogLevel returns the level of logging that the server will use.
func (cfg YAMLConfig) LogLevel() LogLevel {
	if cfg.LogLevelStr == nil {
		return defaultLogLevel
	}

	return LogLevel(*cfg.LogLevelStr)
}

// DatabaseNamesAndPaths returns an array of env.EnvNameAndPathObjects corresponding to the databases to be loaded in
// a multiple db configuration. If nil is returned the server will look for a database in the current directory and
// give it a name automatically.
func (cfg YAMLConfig) DatabaseNamesAndPaths() []env.EnvNameAndPath {
	var dbNamesAndPaths []env.EnvNameAndPath
	for _, dbConfig := range cfg.DatabaseConfig {
		dbNamesAndPaths = append(dbNamesAndPaths, env.EnvNameAndPath{Name: dbConfig.Name, Path: dbConfig.Path})
	}

	return dbNamesAndPaths
}

// MaxConnections returns the maximum number of simultaneous connections the server will allow.  The default is 1
func (cfg YAMLConfig) MaxConnections() uint64 {
	if cfg.ListenerConfig.MaxConnections == nil {
		return defaultMaxConnections
	}

	return *cfg.ListenerConfig.MaxConnections
}

// DisableClientMultiStatements returns true if the server should run in a mode
// where the CLIENT_MULTI_STATEMENTS option are ignored and every incoming
// ComQuery packet is assumed to be a standalone query.
func (cfg YAMLConfig) DisableClientMultiStatements() bool {
	if cfg.BehaviorConfig.DisableClientMultiStatements == nil {
		return false
	}

	return *cfg.BehaviorConfig.DisableClientMultiStatements
}

func (cfg YAMLConfig) MetricsLabels() map[string]string {
	return cfg.MetricsConfig.Labels
}

func (cfg YAMLConfig) MetricsHost() string {
	if cfg.MetricsConfig.Host == nil {
		return defaultMetricsHost
	}

	return *cfg.MetricsConfig.Host
}

func (cfg YAMLConfig) MetricsPort() int {
	if cfg.MetricsConfig.Host == nil {
		return defaultMetricsPort
	}

	return *cfg.MetricsConfig.Port
}

func (cfg YAMLConfig) PrivilegeFilePath() string {
	if cfg.PrivilegeFile != nil {
		return *cfg.PrivilegeFile
	}
	return ""
}

func (cfg YAMLConfig) MySQLDbFilePath() string {
	if cfg.MySQLDbFile != nil {
		return *cfg.MySQLDbFile
	}
	return ""
}

// QueryParallelism returns the parallelism that should be used by the go-mysql-server analyzer
func (cfg YAMLConfig) QueryParallelism() int {
	if cfg.PerformanceConfig.QueryParallelism == nil {
		return defaultQueryParallelism
	}

	return *cfg.PerformanceConfig.QueryParallelism
}

func (cfg YAMLConfig) TLSKey() string {
	if cfg.ListenerConfig.TLSKey == nil {
		return ""
	}
	return *cfg.ListenerConfig.TLSKey
}

func (cfg YAMLConfig) TLSCert() string {
	if cfg.ListenerConfig.TLSCert == nil {
		return ""
	}
	return *cfg.ListenerConfig.TLSCert
}

func (cfg YAMLConfig) RequireSecureTransport() bool {
	if cfg.ListenerConfig.RequireSecureTransport == nil {
		return false
	}
	return *cfg.ListenerConfig.RequireSecureTransport
}

func (cfg YAMLConfig) PersistenceBehavior() string {
	if cfg.BehaviorConfig.PersistenceBehavior == nil {
		return loadPerisistentGlobals
	}
	return *cfg.BehaviorConfig.PersistenceBehavior
}

func (cfg YAMLConfig) DataDir() string {
	if cfg.DataDirStr != nil {
		return *cfg.DataDirStr
	}
	return ""
}
