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
	"fmt"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"gopkg.in/yaml.v2"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

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

func nillableIntPtr(n int) *int {
	if n == 0 {
		return nil
	}
	return &n
}

// BehaviorYAMLConfig contains server configuration regarding how the server should behave
type BehaviorYAMLConfig struct {
	ReadOnly   *bool `yaml:"read_only"`
	AutoCommit *bool `yaml:"autocommit"`
	// PersistenceBehavior is unused, but still present to prevent breaking any YAML configs that still use it.
	PersistenceBehavior *string `yaml:"persistence_behavior,omitempty"`
	// Disable processing CLIENT_MULTI_STATEMENTS support on the
	// sql server.  Dolt's handling of CLIENT_MULTI_STATEMENTS is currently
	// broken. If a client advertises to support it (mysql cli client
	// does), and then sends statements that contain embedded unquoted ';'s
	// (such as a CREATE TRIGGER), then those incoming queries will be
	// misprocessed.
	DisableClientMultiStatements *bool `yaml:"disable_client_multi_statements"`
	// DoltTransactionCommit enables the @@dolt_transaction_commit system variable, which
	// automatically creates a Dolt commit when any SQL transaction is committed.
	DoltTransactionCommit *bool `yaml:"dolt_transaction_commit"`

	EventSchedulerStatus *string `yaml:"event_scheduler,omitempty" minver:"1.17.0"`
}

// UserYAMLConfig contains server configuration regarding the user account clients must use to connect
type UserYAMLConfig struct {
	Name     *string `yaml:"name"`
	Password *string `yaml:"password"`
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
	// AllowCleartextPasswords enables use of cleartext passwords.
	AllowCleartextPasswords *bool `yaml:"allow_cleartext_passwords"`
	// Socket is unix socket file path
	Socket *string `yaml:"socket,omitempty"`
}

// PerformanceYAMLConfig contains configuration parameters for performance tweaking
type PerformanceYAMLConfig struct {
	// QueryParallelism is deprecated but still present to prevent breaking YAML config that still uses it
	QueryParallelism *int `yaml:"query_parallelism,omitempty"`
}

type MetricsYAMLConfig struct {
	Labels map[string]string `yaml:"labels"`
	Host   *string           `yaml:"host"`
	Port   *int              `yaml:"port"`
}

type RemotesapiYAMLConfig struct {
	Port_     *int  `yaml:"port,omitempty"`
	ReadOnly_ *bool `yaml:"read_only,omitempty" minver:"1.30.5"`
}

func (r RemotesapiYAMLConfig) Port() int {
	return *r.Port_
}

func (r RemotesapiYAMLConfig) ReadOnly() bool {
	return *r.ReadOnly_
}

type UserSessionVars struct {
	Name string                 `yaml:"name"`
	Vars map[string]interface{} `yaml:"vars"`
}

// YAMLConfig is a ServerConfig implementation which is read from a yaml file
type YAMLConfig struct {
	LogLevelStr       *string                `yaml:"log_level,omitempty"`
	MaxQueryLenInLogs *int                   `yaml:"max_logged_query_len,omitempty"`
	EncodeLoggedQuery *bool                  `yaml:"encode_logged_query,omitempty"`
	BehaviorConfig    BehaviorYAMLConfig     `yaml:"behavior"`
	UserConfig        UserYAMLConfig         `yaml:"user"`
	ListenerConfig    ListenerYAMLConfig     `yaml:"listener"`
	PerformanceConfig *PerformanceYAMLConfig `yaml:"performance,omitempty"`
	DataDirStr        *string                `yaml:"data_dir,omitempty"`
	CfgDirStr         *string                `yaml:"cfg_dir,omitempty"`
	MetricsConfig     MetricsYAMLConfig      `yaml:"metrics"`
	RemotesapiConfig  RemotesapiYAMLConfig   `yaml:"remotesapi"`
	ClusterCfg        *ClusterYAMLConfig     `yaml:"cluster,omitempty"`
	PrivilegeFile     *string                `yaml:"privilege_file,omitempty"`
	BranchControlFile *string                `yaml:"branch_control_file,omitempty"`
	// TODO: Rename to UserVars_
	Vars            []UserSessionVars      `yaml:"user_session_vars"`
	SystemVars_     map[string]interface{} `yaml:"system_variables,omitempty" minver:"1.11.1"`
	Jwks            []JwksConfig           `yaml:"jwks"`
	GoldenMysqlConn *string                `yaml:"golden_mysql_conn,omitempty"`
}

var _ ServerConfig = YAMLConfig{}
var _ ValidatingServerConfig = YAMLConfig{}
var _ WritableServerConfig = &YAMLConfig{}

func NewYamlConfig(configFileData []byte) (*YAMLConfig, error) {
	var cfg YAMLConfig
	err := yaml.UnmarshalStrict(configFileData, &cfg)
	if cfg.LogLevelStr != nil {
		loglevel := strings.ToLower(*cfg.LogLevelStr)
		cfg.LogLevelStr = &loglevel
	}
	return &cfg, err
}

// YamlConfigFromFile returns server config variables with values defined in yaml file.
func YamlConfigFromFile(fs filesys.Filesys, path string) (ServerConfig, error) {
	data, err := fs.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to read file '%s'. Error: %s", path, err.Error())
	}

	cfg, err := NewYamlConfig(data)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse yaml file '%s'. Error: %s", path, err.Error())
	}

	return cfg, nil
}

func ServerConfigAsYAMLConfig(cfg ServerConfig) *YAMLConfig {
	systemVars := cfg.SystemVars()
	return &YAMLConfig{
		LogLevelStr:       ptr(string(cfg.LogLevel())),
		MaxQueryLenInLogs: nillableIntPtr(cfg.MaxLoggedQueryLen()),
		EncodeLoggedQuery: nillableBoolPtr(cfg.ShouldEncodeLoggedQuery()),
		BehaviorConfig: BehaviorYAMLConfig{
			ReadOnly:                     ptr(cfg.ReadOnly()),
			AutoCommit:                   ptr(cfg.AutoCommit()),
			DisableClientMultiStatements: ptr(cfg.DisableClientMultiStatements()),
			DoltTransactionCommit:        ptr(cfg.DoltTransactionCommit()),
			EventSchedulerStatus:         ptr(cfg.EventSchedulerStatus()),
		},
		UserConfig: UserYAMLConfig{
			Name:     ptr(cfg.User()),
			Password: ptr(cfg.Password()),
		},
		ListenerConfig: ListenerYAMLConfig{
			HostStr:                 ptr(cfg.Host()),
			PortNumber:              ptr(cfg.Port()),
			MaxConnections:          ptr(cfg.MaxConnections()),
			ReadTimeoutMillis:       ptr(cfg.ReadTimeout()),
			WriteTimeoutMillis:      ptr(cfg.WriteTimeout()),
			TLSKey:                  nillableStrPtr(cfg.TLSKey()),
			TLSCert:                 nillableStrPtr(cfg.TLSCert()),
			RequireSecureTransport:  nillableBoolPtr(cfg.RequireSecureTransport()),
			AllowCleartextPasswords: nillableBoolPtr(cfg.AllowCleartextPasswords()),
			Socket:                  nillableStrPtr(cfg.Socket()),
		},
		DataDirStr: ptr(cfg.DataDir()),
		CfgDirStr:  ptr(cfg.CfgDir()),
		MetricsConfig: MetricsYAMLConfig{
			Labels: cfg.MetricsLabels(),
			Host:   nillableStrPtr(cfg.MetricsHost()),
			Port:   ptr(cfg.MetricsPort()),
		},
		RemotesapiConfig: RemotesapiYAMLConfig{
			Port_:     cfg.RemotesapiPort(),
			ReadOnly_: cfg.RemotesapiReadOnly(),
		},
		ClusterCfg:        clusterConfigAsYAMLConfig(cfg.ClusterConfig()),
		PrivilegeFile:     ptr(cfg.PrivilegeFilePath()),
		BranchControlFile: ptr(cfg.BranchControlFilePath()),
		SystemVars_:       systemVars,
		Vars:              cfg.UserVars(),
		Jwks:              cfg.JwksConfig(),
	}
}

func clusterConfigAsYAMLConfig(config ClusterConfig) *ClusterYAMLConfig {
	if config == nil {
		return nil
	}

	return &ClusterYAMLConfig{
		StandbyRemotes_: nil,
		BootstrapRole_:  config.BootstrapRole(),
		BootstrapEpoch_: config.BootstrapEpoch(),
		RemotesAPI: ClusterRemotesAPIYAMLConfig{
			Addr_:      config.RemotesAPIConfig().Address(),
			Port_:      config.RemotesAPIConfig().Port(),
			TLSKey_:    config.RemotesAPIConfig().TLSKey(),
			TLSCert_:   config.RemotesAPIConfig().TLSCert(),
			TLSCA_:     config.RemotesAPIConfig().TLSCA(),
			URLMatches: config.RemotesAPIConfig().ServerNameURLMatches(),
			DNSMatches: config.RemotesAPIConfig().ServerNameDNSMatches(),
		},
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
		return DefaultHost
	}

	return *cfg.ListenerConfig.HostStr
}

// Port returns the port that the server will run on. The valid range is [1024, 65535].
func (cfg YAMLConfig) Port() int {
	if cfg.ListenerConfig.PortNumber == nil {
		return DefaultPort
	}

	return *cfg.ListenerConfig.PortNumber
}

// ReadTimeout returns the read timeout in milliseconds.
func (cfg YAMLConfig) ReadTimeout() uint64 {
	if cfg.ListenerConfig.ReadTimeoutMillis == nil {
		return DefaultTimeout
	}

	return *cfg.ListenerConfig.ReadTimeoutMillis
}

// WriteTimeout returns the write timeout in milliseconds.
func (cfg YAMLConfig) WriteTimeout() uint64 {
	if cfg.ListenerConfig.WriteTimeoutMillis == nil {
		return DefaultTimeout
	}

	return *cfg.ListenerConfig.WriteTimeoutMillis
}

// User returns the username that connecting clients must use.
func (cfg YAMLConfig) User() string {
	if cfg.UserConfig.Name == nil {
		return DefaultUser
	}

	return *cfg.UserConfig.Name
}

func (cfg *YAMLConfig) SetUserName(s string) {
	cfg.UserConfig.Name = &s
}

func (cfg *YAMLConfig) SetPassword(s string) {
	cfg.UserConfig.Password = &s
}

// Password returns the password that connecting clients must use.
func (cfg YAMLConfig) Password() string {
	if cfg.UserConfig.Password == nil {
		return DefaultPass
	}

	return *cfg.UserConfig.Password
}

// ReadOnly returns whether the server will only accept read statements or all statements.
func (cfg YAMLConfig) ReadOnly() bool {
	if cfg.BehaviorConfig.ReadOnly == nil {
		return DefaultReadOnly
	}

	return *cfg.BehaviorConfig.ReadOnly
}

// AutoCommit defines the value of the @@autocommit session variable used on every connection
func (cfg YAMLConfig) AutoCommit() bool {
	if cfg.BehaviorConfig.AutoCommit == nil {
		return DefaultAutoCommit
	}

	return *cfg.BehaviorConfig.AutoCommit
}

// DoltTransactionCommit defines the value of the @@dolt_transaction_commit session variable that enables Dolt
// commits to be automatically created when a SQL transaction is committed.
func (cfg YAMLConfig) DoltTransactionCommit() bool {
	if cfg.BehaviorConfig.DoltTransactionCommit == nil {
		return DefaultDoltTransactionCommit
	}

	return *cfg.BehaviorConfig.DoltTransactionCommit
}

// LogLevel returns the level of logging that the server will use.
func (cfg YAMLConfig) LogLevel() LogLevel {
	if cfg.LogLevelStr == nil {
		return DefaultLogLevel
	}

	return LogLevel(*cfg.LogLevelStr)
}

// MaxConnections returns the maximum number of simultaneous connections the server will allow.  The default is 1
func (cfg YAMLConfig) MaxConnections() uint64 {
	if cfg.ListenerConfig.MaxConnections == nil {
		return DefaultMaxConnections
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

// MetricsLabels returns labels that are applied to all prometheus metrics
func (cfg YAMLConfig) MetricsLabels() map[string]string {
	return cfg.MetricsConfig.Labels
}

func (cfg YAMLConfig) MetricsHost() string {
	if cfg.MetricsConfig.Host == nil {
		return DefaultMetricsHost
	}

	return *cfg.MetricsConfig.Host
}

func (cfg YAMLConfig) MetricsPort() int {
	if cfg.MetricsConfig.Host == nil {
		return DefaultMetricsPort
	}
	if cfg.MetricsConfig.Port == nil {
		return DefaultMetricsPort
	}

	return *cfg.MetricsConfig.Port
}

func (cfg YAMLConfig) RemotesapiPort() *int {
	return cfg.RemotesapiConfig.Port_
}

func (cfg YAMLConfig) RemotesapiReadOnly() *bool {
	return cfg.RemotesapiConfig.ReadOnly_
}

// PrivilegeFilePath returns the path to the file which contains all needed privilege information in the form of a
// JSON string.
func (cfg YAMLConfig) PrivilegeFilePath() string {
	if cfg.PrivilegeFile != nil {
		return *cfg.PrivilegeFile
	}
	return filepath.Join(cfg.CfgDir(), DefaultPrivilegeFilePath)
}

// BranchControlFilePath returns the path to the file which contains the branch control permissions.
func (cfg YAMLConfig) BranchControlFilePath() string {
	if cfg.BranchControlFile != nil {
		return *cfg.BranchControlFile
	}
	return filepath.Join(cfg.CfgDir(), DefaultBranchControlFilePath)
}

// UserVars is an array containing user specific session variables
func (cfg YAMLConfig) UserVars() []UserSessionVars {
	if cfg.Vars != nil {
		return cfg.Vars
	}

	return nil
}

func (cfg YAMLConfig) SystemVars() map[string]interface{} {
	if cfg.SystemVars_ == nil {
		return map[string]interface{}{}
	}

	return cfg.SystemVars_
}

// wksConfig is JSON Web Key Set config, and used to validate a user authed with a jwt (JSON Web Token).
func (cfg YAMLConfig) JwksConfig() []JwksConfig {
	if cfg.Jwks != nil {
		return cfg.Jwks
	}
	return nil
}

func (cfg YAMLConfig) AllowCleartextPasswords() bool {
	if cfg.ListenerConfig.AllowCleartextPasswords == nil {
		return DefaultAllowCleartextPasswords
	}
	return *cfg.ListenerConfig.AllowCleartextPasswords
}

// TLSKey returns a path to the servers PEM-encoded private TLS key. "" if there is none.
func (cfg YAMLConfig) TLSKey() string {
	if cfg.ListenerConfig.TLSKey == nil {
		return ""
	}
	return *cfg.ListenerConfig.TLSKey
}

// TLSCert returns a path to the servers PEM-encoded TLS certificate chain. "" if there is none.
func (cfg YAMLConfig) TLSCert() string {
	if cfg.ListenerConfig.TLSCert == nil {
		return ""
	}
	return *cfg.ListenerConfig.TLSCert
}

// RequireSecureTransport is true if the server should reject non-TLS connections.
func (cfg YAMLConfig) RequireSecureTransport() bool {
	if cfg.ListenerConfig.RequireSecureTransport == nil {
		return false
	}
	return *cfg.ListenerConfig.RequireSecureTransport
}

// MaxLoggedQueryLen is the max length of queries written to the logs.  Queries longer than this number are truncated.
// If this value is 0 then the query is not truncated and will be written to the logs in its entirety.  If the value
// is less than 0 then the queries will be omitted from the logs completely
func (cfg YAMLConfig) MaxLoggedQueryLen() int {
	if cfg.MaxQueryLenInLogs == nil {
		return DefaultMaxLoggedQueryLen
	}

	return *cfg.MaxQueryLenInLogs
}

func (cfg YAMLConfig) ShouldEncodeLoggedQuery() bool {
	if cfg.EncodeLoggedQuery == nil {
		return DefaultEncodeLoggedQuery
	}

	return *cfg.EncodeLoggedQuery
}

// DataDir is the path to a directory to use as the data dir, both to create new databases and locate existing ones.
func (cfg YAMLConfig) DataDir() string {
	if cfg.DataDirStr != nil {
		return *cfg.DataDirStr
	}
	return DefaultDataDir
}

// CfgDir is the path to a directory to use to store the dolt configuration files.
func (cfg YAMLConfig) CfgDir() string {
	if cfg.CfgDirStr != nil {
		return *cfg.CfgDirStr
	}
	return filepath.Join(cfg.DataDir(), DefaultCfgDir)
}

// Socket is a path to the unix socket file
func (cfg YAMLConfig) Socket() string {
	if cfg.ListenerConfig.Socket == nil {
		return ""
	}
	// if defined but empty -> default
	if *cfg.ListenerConfig.Socket == "" {
		return DefaultUnixSocketFilePath
	}
	return *cfg.ListenerConfig.Socket
}

func (cfg YAMLConfig) GoldenMysqlConnectionString() (s string) {
	if cfg.GoldenMysqlConn != nil {
		s = *cfg.GoldenMysqlConn
	}
	return
}

func (cfg YAMLConfig) ClusterConfig() ClusterConfig {
	if cfg.ClusterCfg == nil {
		return nil
	}
	return cfg.ClusterCfg
}

func (cfg YAMLConfig) EventSchedulerStatus() string {
	if cfg.BehaviorConfig.EventSchedulerStatus == nil {
		return "ON"
	}
	switch *cfg.BehaviorConfig.EventSchedulerStatus {
	case "1":
		return "ON"
	case "0":
		return "OFF"
	default:
		return strings.ToUpper(*cfg.BehaviorConfig.EventSchedulerStatus)
	}
}

type ClusterYAMLConfig struct {
	StandbyRemotes_ []StandbyRemoteYAMLConfig   `yaml:"standby_remotes"`
	BootstrapRole_  string                      `yaml:"bootstrap_role"`
	BootstrapEpoch_ int                         `yaml:"bootstrap_epoch"`
	RemotesAPI      ClusterRemotesAPIYAMLConfig `yaml:"remotesapi"`
}

type StandbyRemoteYAMLConfig struct {
	Name_              string `yaml:"name"`
	RemoteURLTemplate_ string `yaml:"remote_url_template"`
}

func (c StandbyRemoteYAMLConfig) Name() string {
	return c.Name_
}

func (c StandbyRemoteYAMLConfig) RemoteURLTemplate() string {
	return c.RemoteURLTemplate_
}

func (c *ClusterYAMLConfig) StandbyRemotes() []ClusterStandbyRemoteConfig {
	ret := make([]ClusterStandbyRemoteConfig, len(c.StandbyRemotes_))
	for i := range c.StandbyRemotes_ {
		ret[i] = c.StandbyRemotes_[i]
	}
	return ret
}

func (c *ClusterYAMLConfig) BootstrapRole() string {
	return c.BootstrapRole_
}

func (c *ClusterYAMLConfig) BootstrapEpoch() int {
	return c.BootstrapEpoch_
}

func (c *ClusterYAMLConfig) RemotesAPIConfig() ClusterRemotesAPIConfig {
	return c.RemotesAPI
}

type ClusterRemotesAPIYAMLConfig struct {
	Addr_      string   `yaml:"address"`
	Port_      int      `yaml:"port"`
	TLSKey_    string   `yaml:"tls_key"`
	TLSCert_   string   `yaml:"tls_cert"`
	TLSCA_     string   `yaml:"tls_ca"`
	URLMatches []string `yaml:"server_name_urls"`
	DNSMatches []string `yaml:"server_name_dns"`
}

func (c ClusterRemotesAPIYAMLConfig) Address() string {
	return c.Addr_
}

func (c ClusterRemotesAPIYAMLConfig) Port() int {
	return c.Port_
}

func (c ClusterRemotesAPIYAMLConfig) TLSKey() string {
	return c.TLSKey_
}

func (c ClusterRemotesAPIYAMLConfig) TLSCert() string {
	return c.TLSCert_
}

func (c ClusterRemotesAPIYAMLConfig) TLSCA() string {
	return c.TLSCA_
}

func (c ClusterRemotesAPIYAMLConfig) ServerNameURLMatches() []string {
	return c.URLMatches
}

func (c ClusterRemotesAPIYAMLConfig) ServerNameDNSMatches() []string {
	return c.DNSMatches
}

func (cfg YAMLConfig) ValueSet(value string) bool {
	switch value {
	case ReadTimeoutKey:
		return cfg.ListenerConfig.ReadTimeoutMillis != nil
	case WriteTimeoutKey:
		return cfg.ListenerConfig.WriteTimeoutMillis != nil
	case MaxConnectionsKey:
		return cfg.ListenerConfig.MaxConnections != nil
	case EventSchedulerKey:
		return cfg.BehaviorConfig.EventSchedulerStatus != nil
	}
	return false
}
