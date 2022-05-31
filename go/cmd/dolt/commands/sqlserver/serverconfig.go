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

package sqlserver

import (
	"crypto/tls"
	"fmt"
	"net"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

// LogLevel defines the available levels of logging for the server.
type LogLevel string

const (
	LogLevel_Trace   LogLevel = "trace"
	LogLevel_Debug   LogLevel = "debug"
	LogLevel_Info    LogLevel = "info"
	LogLevel_Warning LogLevel = "warning"
	LogLevel_Error   LogLevel = "error"
	LogLevel_Fatal   LogLevel = "fatal"
)

const (
	defaultHost                = "localhost"
	defaultPort                = 3306
	defaultUser                = "root"
	defaultPass                = ""
	defaultTimeout             = 8 * 60 * 60 * 1000 // 8 hours, same as MySQL
	defaultReadOnly            = false
	defaultLogLevel            = LogLevel_Info
	defaultAutoCommit          = true
	defaultMaxConnections      = 100
	defaultQueryParallelism    = 2
	defaultPersistenceBahavior = loadPerisistentGlobals
	defaultDataDir             = "."
	defaultMetricsHost         = ""
	defaultMetricsPort         = -1
	defaultMySQLDbFilePath     = "mysql.db"
	defaultPrivilegeFilePath   = "privs.json"
)

const (
	ignorePeristentGlobals = "ignore"
	loadPerisistentGlobals = "load"
)

// String returns the string representation of the log level.
func (level LogLevel) String() string {
	switch level {
	case LogLevel_Trace:
		fallthrough
	case LogLevel_Debug:
		fallthrough
	case LogLevel_Info:
		fallthrough
	case LogLevel_Warning:
		fallthrough
	case LogLevel_Error:
		fallthrough
	case LogLevel_Fatal:
		return string(level)
	default:
		return "unknown"
	}
}

// ServerConfig contains all of the configurable options for the MySQL-compatible server.
type ServerConfig interface {
	// Host returns the domain that the server will run on. Accepts an IPv4 or IPv6 address, in addition to localhost.
	Host() string
	// Port returns the port that the server will run on. The valid range is [1024, 65535].
	Port() int
	// User returns the username that connecting clients must use.
	User() string
	// Password returns the password that connecting clients must use.
	Password() string
	// ReadTimeout returns the read timeout in milliseconds
	ReadTimeout() uint64
	// WriteTimeout returns the write timeout in milliseconds
	WriteTimeout() uint64
	// ReadOnly returns whether the server will only accept read statements or all statements.
	ReadOnly() bool
	// LogLevel returns the level of logging that the server will use.
	LogLevel() LogLevel
	// Autocommit defines the value of the @@autocommit session variable used on every connection
	AutoCommit() bool
	// DatabaseNamesAndPaths returns an array of env.EnvNameAndPathObjects corresponding to the databases to be loaded in
	// a multiple db configuration. If nil is returned the server will look for a database in the current directory and
	// give it a name automatically.
	DatabaseNamesAndPaths() []env.EnvNameAndPath
	// DataDir is the path to a directory to use as the data dir, both to create new databases and locate existing ones.
	DataDir() string
	// MaxConnections returns the maximum number of simultaneous connections the server will allow.  The default is 1
	MaxConnections() uint64
	// QueryParallelism returns the parallelism that should be used by the go-mysql-server analyzer
	QueryParallelism() int
	// TLSKey returns a path to the servers PEM-encoded private TLS key. "" if there is none.
	TLSKey() string
	// TLSCert returns a path to the servers PEM-encoded TLS certificate chain. "" if there is none.
	TLSCert() string
	// RequireSecureTransport is true if the server should reject non-TLS connections.
	RequireSecureTransport() bool
	// PersistenceBehavior is "load" if we include persisted system globals on server init
	PersistenceBehavior() string
	// DisableClientMultiStatements is true if we want the server to not
	// process incoming ComQuery packets as if they had multiple queries in
	// them, even if the client advertises support for MULTI_STATEMENTS.
	DisableClientMultiStatements() bool
	// MetricsLabels returns labels that are applied to all prometheus metrics
	MetricsLabels() map[string]string
	MetricsHost() string
	MetricsPort() int
	// PrivilegeFilePath returns the path to the file which contains all needed privilege information in the form of a
	// JSON string.
	PrivilegeFilePath() string
	// MySQLDbFilePath returns the path to the file which contains the information for a MySQL db.
	MySQLDbFilePath() string
}

type commandLineServerConfig struct {
	host                   string
	port                   int
	user                   string
	password               string
	timeout                uint64
	readOnly               bool
	logLevel               LogLevel
	dbNamesAndPaths        []env.EnvNameAndPath
	dataDir                string
	autoCommit             bool
	maxConnections         uint64
	queryParallelism       int
	tlsKey                 string
	tlsCert                string
	requireSecureTransport bool
	persistenceBehavior    string
	privilegeFilePath      string
	mysqlDbFilePath        string
}

var _ ServerConfig = (*commandLineServerConfig)(nil)

// Host returns the domain that the server will run on. Accepts an IPv4 or IPv6 address, in addition to localhost.
func (cfg *commandLineServerConfig) Host() string {
	return cfg.host
}

// Port returns the port that the server will run on. The valid range is [1024, 65535].
func (cfg *commandLineServerConfig) Port() int {
	return cfg.port
}

// User returns the username that connecting clients must use.
func (cfg *commandLineServerConfig) User() string {
	return cfg.user
}

// Password returns the password that connecting clients must use.
func (cfg *commandLineServerConfig) Password() string {
	return cfg.password
}

// ReadTimeout returns the read and write timeouts.
func (cfg *commandLineServerConfig) ReadTimeout() uint64 {
	return cfg.timeout
}

// WriteTimeout returns the read and write timeouts.
func (cfg *commandLineServerConfig) WriteTimeout() uint64 {
	return cfg.timeout
}

// ReadOnly returns whether the server will only accept read statements or all statements.
func (cfg *commandLineServerConfig) ReadOnly() bool {
	return cfg.readOnly
}

// LogLevel returns the level of logging that the server will use.
func (cfg *commandLineServerConfig) LogLevel() LogLevel {
	return cfg.logLevel
}

// Autocommit defines the value of the @@autocommit session variable used on every connection
func (cfg *commandLineServerConfig) AutoCommit() bool {
	return cfg.autoCommit
}

// MaxConnections returns the maximum number of simultaneous connections the server will allow.  The default is 1
func (cfg *commandLineServerConfig) MaxConnections() uint64 {
	return cfg.maxConnections
}

// QueryParallelism returns the parallelism that should be used by the go-mysql-server analyzer
func (cfg *commandLineServerConfig) QueryParallelism() int {
	return cfg.queryParallelism
}

// PersistenceBehavior returns whether to autoload persisted server configuration
func (cfg *commandLineServerConfig) PersistenceBehavior() string {
	return cfg.persistenceBehavior
}

func (cfg *commandLineServerConfig) TLSKey() string {
	return cfg.tlsKey
}

func (cfg *commandLineServerConfig) TLSCert() string {
	return cfg.tlsCert
}

func (cfg *commandLineServerConfig) RequireSecureTransport() bool {
	return cfg.requireSecureTransport
}

func (cfg *commandLineServerConfig) DisableClientMultiStatements() bool {
	return false
}

func (cfg *commandLineServerConfig) MetricsLabels() map[string]string {
	return nil
}

func (cfg *commandLineServerConfig) MetricsHost() string {
	return defaultMetricsHost
}

func (cfg *commandLineServerConfig) MetricsPort() int {
	return defaultMetricsPort
}

func (cfg *commandLineServerConfig) PrivilegeFilePath() string {
	return cfg.privilegeFilePath
}

func (cfg *commandLineServerConfig) MySQLDbFilePath() string {
	return cfg.mysqlDbFilePath
}

// DatabaseNamesAndPaths returns an array of env.EnvNameAndPathObjects corresponding to the databases to be loaded in
// a multiple db configuration. If nil is returned the server will look for a database in the current directory and
// give it a name automatically.
func (cfg *commandLineServerConfig) DatabaseNamesAndPaths() []env.EnvNameAndPath {
	return cfg.dbNamesAndPaths
}

func (cfg *commandLineServerConfig) DataDir() string {
	return cfg.dataDir
}

// withHost updates the host and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) withHost(host string) *commandLineServerConfig {
	cfg.host = host
	return cfg
}

// withPort updates the port and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) withPort(port int) *commandLineServerConfig {
	cfg.port = port
	return cfg
}

// withUser updates the user and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) withUser(user string) *commandLineServerConfig {
	cfg.user = user
	return cfg
}

// withPassword updates the password and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) withPassword(password string) *commandLineServerConfig {
	cfg.password = password
	return cfg
}

// withTimeout updates the timeout and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) withTimeout(timeout uint64) *commandLineServerConfig {
	cfg.timeout = timeout
	return cfg
}

// withReadOnly updates the read only flag and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) withReadOnly(readonly bool) *commandLineServerConfig {
	cfg.readOnly = readonly
	return cfg
}

// withLogLevel updates the log level and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) withLogLevel(loglevel LogLevel) *commandLineServerConfig {
	cfg.logLevel = loglevel
	return cfg
}

// withMaxConnections updates the maximum number of concurrent connections and returns the called
// `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) withMaxConnections(maxConnections uint64) *commandLineServerConfig {
	cfg.maxConnections = maxConnections
	return cfg
}

// withQueryParallelism updates the query parallelism and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) withQueryParallelism(queryParallelism int) *commandLineServerConfig {
	cfg.queryParallelism = queryParallelism
	return cfg
}

func (cfg *commandLineServerConfig) withDBNamesAndPaths(dbNamesAndPaths []env.EnvNameAndPath) *commandLineServerConfig {
	cfg.dbNamesAndPaths = dbNamesAndPaths
	return cfg
}

func (cfg *commandLineServerConfig) withDataDir(dataDir string) *commandLineServerConfig {
	cfg.dataDir = dataDir
	return cfg
}

func (cfg *commandLineServerConfig) withPersistenceBehavior(persistenceBehavior string) *commandLineServerConfig {
	cfg.persistenceBehavior = persistenceBehavior
	return cfg
}

func (cfg *commandLineServerConfig) withPrivilegeFilePath(privFilePath string) *commandLineServerConfig {
	cfg.privilegeFilePath = privFilePath
	return cfg
}

// DefaultServerConfig creates a `*ServerConfig` that has all of the options set to their default values.
func DefaultServerConfig() *commandLineServerConfig {
	return &commandLineServerConfig{
		host:                defaultHost,
		port:                defaultPort,
		user:                defaultUser,
		password:            defaultPass,
		timeout:             defaultTimeout,
		readOnly:            defaultReadOnly,
		logLevel:            defaultLogLevel,
		autoCommit:          defaultAutoCommit,
		maxConnections:      defaultMaxConnections,
		queryParallelism:    defaultQueryParallelism,
		persistenceBehavior: defaultPersistenceBahavior,
		dataDir:             defaultDataDir,
		privilegeFilePath:   defaultPrivilegeFilePath,
		mysqlDbFilePath:     defaultMySQLDbFilePath,
	}
}

// Validate returns an `error` if any field is not valid.
func ValidateConfig(config ServerConfig) error {
	if config.Host() != "localhost" {
		ip := net.ParseIP(config.Host())
		if ip == nil {
			return fmt.Errorf("address is not a valid IP: %v", config.Host())
		}
	}
	if config.Port() < 1024 || config.Port() > 65535 {
		return fmt.Errorf("port is not in the range between 1024-65535: %v\n", config.Port())
	}
	if len(config.User()) == 0 {
		return fmt.Errorf("user cannot be empty")
	}
	if config.LogLevel().String() == "unknown" {
		return fmt.Errorf("loglevel is invalid: %v\n", string(config.LogLevel()))
	}
	if config.RequireSecureTransport() && config.TLSCert() == "" && config.TLSKey() == "" {
		return fmt.Errorf("require_secure_transport can only be `true` when a tls_key and tls_cert are provided.")
	}
	return nil
}

// ConnectionString returns a Data Source Name (DSN) to be used by go clients for connecting to a running server.
func ConnectionString(config ServerConfig) string {
	return fmt.Sprintf("%v:%v@tcp(%v:%v)/", config.User(), config.Password(), config.Host(), config.Port())
}

// ConfigInfo returns a summary of some of the config which contains some of the more important information
func ConfigInfo(config ServerConfig) string {
	return fmt.Sprintf(`HP="%v:%v"|T="%v"|R="%v"|L="%v"`, config.Host(), config.Port(),
		config.ReadTimeout(), config.ReadOnly(), config.LogLevel())
}

// LoadTLSConfig loads the certificate chain from config.TLSKey() and config.TLSCert() and returns
// a *tls.Config configured for its use. Returns `nil` if key and cert are `""`.
func LoadTLSConfig(cfg ServerConfig) (*tls.Config, error) {
	if cfg.TLSKey() == "" && cfg.TLSCert() == "" {
		return nil, nil
	}
	c, err := tls.LoadX509KeyPair(cfg.TLSCert(), cfg.TLSKey())
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{
			c,
		},
	}, nil
}
