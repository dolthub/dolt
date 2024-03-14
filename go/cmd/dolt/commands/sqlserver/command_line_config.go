// Copyright 2024 Dolthub, Inc.
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
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/cluster"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

type commandLineServerConfig struct {
	host                    string
	port                    int
	user                    string
	password                string
	timeout                 uint64
	readOnly                bool
	logLevel                LogLevel
	dataDir                 string
	cfgDir                  string
	autoCommit              bool
	doltTransactionCommit   bool
	maxConnections          uint64
	queryParallelism        int
	tlsKey                  string
	tlsCert                 string
	requireSecureTransport  bool
	maxLoggedQueryLen       int
	shouldEncodeLoggedQuery bool
	persistenceBehavior     string
	privilegeFilePath       string
	branchControlFilePath   string
	allowCleartextPasswords bool
	socket                  string
	remotesapiPort          *int
	remotesapiReadOnly      *bool
	goldenMysqlConn         string
	eventSchedulerStatus    string
}

var _ ServerConfig = (*commandLineServerConfig)(nil)

// DefaultServerConfig creates a `*ServerConfig` that has all of the options set to their default values.
func DefaultServerConfig() *commandLineServerConfig {
	return &commandLineServerConfig{
		host:                    defaultHost,
		port:                    defaultPort,
		password:                defaultPass,
		timeout:                 defaultTimeout,
		readOnly:                defaultReadOnly,
		logLevel:                defaultLogLevel,
		autoCommit:              defaultAutoCommit,
		maxConnections:          defaultMaxConnections,
		queryParallelism:        defaultQueryParallelism,
		persistenceBehavior:     defaultPersistenceBahavior,
		dataDir:                 defaultDataDir,
		cfgDir:                  filepath.Join(defaultDataDir, defaultCfgDir),
		privilegeFilePath:       filepath.Join(defaultDataDir, defaultCfgDir, defaultPrivilegeFilePath),
		branchControlFilePath:   filepath.Join(defaultDataDir, defaultCfgDir, defaultBranchControlFilePath),
		allowCleartextPasswords: defaultAllowCleartextPasswords,
		maxLoggedQueryLen:       defaultMaxLoggedQueryLen,
	}
}

// NewCommandLineConfig returns server config based on the credentials and command line arguments given.
func NewCommandLineConfig(creds *cli.UserPassword, apr *argparser.ArgParseResults) (ServerConfig, error) {
	config := DefaultServerConfig()

	if sock, ok := apr.GetValue(socketFlag); ok {
		// defined without value gets default
		if sock == "" {
			sock = defaultUnixSocketFilePath
		}
		config.WithSocket(sock)
	}

	if host, ok := apr.GetValue(hostFlag); ok {
		config.WithHost(host)
	}

	if port, ok := apr.GetInt(portFlag); ok {
		config.WithPort(port)
	}

	if creds == nil {
		if user, ok := apr.GetValue(cli.UserFlag); ok {
			config.withUser(user)
		}
		if password, ok := apr.GetValue(cli.PasswordFlag); ok {
			config.withPassword(password)
		}
	} else {
		config.withUser(creds.Username)
		config.withPassword(creds.Password)
	}

	if port, ok := apr.GetInt(remotesapiPortFlag); ok {
		config.WithRemotesapiPort(&port)
	}
	if apr.Contains(remotesapiReadOnlyFlag) {
		val := true
		config.WithRemotesapiReadOnly(&val)
	}

	if persistenceBehavior, ok := apr.GetValue(persistenceBehaviorFlag); ok {
		config.withPersistenceBehavior(persistenceBehavior)
	}

	if timeoutStr, ok := apr.GetValue(timeoutFlag); ok {
		timeout, err := strconv.ParseUint(timeoutStr, 10, 64)

		if err != nil {
			return nil, fmt.Errorf("invalid value for --timeout '%s'", timeoutStr)
		}

		config.withTimeout(timeout * 1000)
	}

	if _, ok := apr.GetValue(readonlyFlag); ok {
		config.withReadOnly(true)
		val := true
		config.WithRemotesapiReadOnly(&val)
	}

	if logLevel, ok := apr.GetValue(logLevelFlag); ok {
		config.withLogLevel(LogLevel(strings.ToLower(logLevel)))
	}

	if dataDir, ok := apr.GetValue(commands.MultiDBDirFlag); ok {
		config.withDataDir(dataDir)
	}

	if dataDir, ok := apr.GetValue(commands.DataDirFlag); ok {
		config.withDataDir(dataDir)
	}

	if queryParallelism, ok := apr.GetInt(queryParallelismFlag); ok {
		config.withQueryParallelism(queryParallelism)
	}

	if maxConnections, ok := apr.GetInt(maxConnectionsFlag); ok {
		config.withMaxConnections(uint64(maxConnections))
	}

	config.autoCommit = !apr.Contains(noAutoCommitFlag)
	config.allowCleartextPasswords = apr.Contains(allowCleartextPasswordsFlag)

	if connStr, ok := apr.GetValue(goldenMysqlConn); ok {
		cli.Println(connStr)
		config.withGoldenMysqlConnectionString(connStr)
	}

	if esStatus, ok := apr.GetValue(eventSchedulerStatus); ok {
		// make sure to assign eventSchedulerStatus first here
		config.withEventScheduler(strings.ToUpper(esStatus))
	}

	return config, nil
}

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

// AutoCommit defines the value of the @@autocommit session variable used on every connection
func (cfg *commandLineServerConfig) AutoCommit() bool {
	return cfg.autoCommit
}

// DoltTransactionCommit defines the value of the @@dolt_transaction_commit session variable that enables Dolt
// commits to be automatically created when a SQL transaction is committed. The default is false.
func (cfg *commandLineServerConfig) DoltTransactionCommit() bool {
	return cfg.doltTransactionCommit
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

// TLSKey returns a path to the servers PEM-encoded private TLS key. "" if there is none.
func (cfg *commandLineServerConfig) TLSKey() string {
	return cfg.tlsKey
}

// TLSCert returns a path to the servers PEM-encoded TLS certificate chain. "" if there is none.
func (cfg *commandLineServerConfig) TLSCert() string {
	return cfg.tlsCert
}

// RequireSecureTransport is true if the server should reject non-TLS connections.
func (cfg *commandLineServerConfig) RequireSecureTransport() bool {
	return cfg.requireSecureTransport
}

// MaxLoggedQueryLen is the max length of queries written to the logs.  Queries longer than this number are truncated.
// If this value is 0 then the query is not truncated and will be written to the logs in its entirety.  If the value
// is less than 0 then the queries will be omitted from the logs completely
func (cfg *commandLineServerConfig) MaxLoggedQueryLen() int {
	return cfg.maxLoggedQueryLen
}

// ShouldEncodeLoggedQuery determines if logged queries are base64 encoded.
// If true, queries will be logged as base64 encoded strings.
// If false (default behavior), queries will be logged as strings, but newlines and tabs will be replaced with spaces.
func (cfg *commandLineServerConfig) ShouldEncodeLoggedQuery() bool {
	return cfg.shouldEncodeLoggedQuery
}

// DisableClientMultiStatements is true if we want the server to not
// process incoming ComQuery packets as if they had multiple queries in
// them, even if the client advertises support for MULTI_STATEMENTS.
func (cfg *commandLineServerConfig) DisableClientMultiStatements() bool {
	return false
}

// MetricsLabels returns labels that are applied to all prometheus metrics
func (cfg *commandLineServerConfig) MetricsLabels() map[string]string {
	return nil
}

func (cfg *commandLineServerConfig) MetricsHost() string {
	return defaultMetricsHost
}

func (cfg *commandLineServerConfig) MetricsPort() int {
	return defaultMetricsPort
}

func (cfg *commandLineServerConfig) RemotesapiPort() *int {
	return cfg.remotesapiPort
}

func (cfg *commandLineServerConfig) RemotesapiReadOnly() *bool {
	return cfg.remotesapiReadOnly
}

func (cfg *commandLineServerConfig) ClusterConfig() cluster.Config {
	return nil
}

// PrivilegeFilePath returns the path to the file which contains all needed privilege information in the form of a
// JSON string.
func (cfg *commandLineServerConfig) PrivilegeFilePath() string {
	return cfg.privilegeFilePath
}

// BranchControlFilePath returns the path to the file which contains the branch control permissions.
func (cfg *commandLineServerConfig) BranchControlFilePath() string {
	return cfg.branchControlFilePath
}

// UserVars is an array containing user specific session variables.
func (cfg *commandLineServerConfig) UserVars() []UserSessionVars {
	return nil
}

func (cfg *commandLineServerConfig) SystemVars() engine.SystemVariables {
	return nil
}

func (cfg *commandLineServerConfig) JwksConfig() []engine.JwksConfig {
	return nil
}

func (cfg *commandLineServerConfig) AllowCleartextPasswords() bool {
	return cfg.allowCleartextPasswords
}

// DataDir is the path to a directory to use as the data dir, both to create new databases and locate existing ones.
func (cfg *commandLineServerConfig) DataDir() string {
	return cfg.dataDir
}

// CfgDir is the path to a directory to use to store the dolt configuration files.
func (cfg *commandLineServerConfig) CfgDir() string {
	return cfg.cfgDir
}

// Socket is a path to the unix socket file
func (cfg *commandLineServerConfig) Socket() string {
	return cfg.socket
}

// WithHost updates the host and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) WithHost(host string) *commandLineServerConfig {
	cfg.host = host
	return cfg
}

// WithPort updates the port and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) WithPort(port int) *commandLineServerConfig {
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

// withDataDir updates the path to a directory to use as the data dir.
func (cfg *commandLineServerConfig) withDataDir(dataDir string) *commandLineServerConfig {
	cfg.dataDir = dataDir
	return cfg
}

// withCfgDir updates the path to a directory to use to store the dolt configuration files.
func (cfg *commandLineServerConfig) withCfgDir(cfgDir string) *commandLineServerConfig {
	cfg.cfgDir = cfgDir
	return cfg
}

// withPersistenceBehavior updates persistence behavior of system globals on server init
func (cfg *commandLineServerConfig) withPersistenceBehavior(persistenceBehavior string) *commandLineServerConfig {
	cfg.persistenceBehavior = persistenceBehavior
	return cfg
}

// withPrivilegeFilePath updates the path to the file which contains all needed privilege information in the form of a JSON string
func (cfg *commandLineServerConfig) withPrivilegeFilePath(privFilePath string) *commandLineServerConfig {
	cfg.privilegeFilePath = privFilePath
	return cfg
}

// withBranchControlFilePath updates the path to the file which contains the branch control permissions
func (cfg *commandLineServerConfig) withBranchControlFilePath(branchControlFilePath string) *commandLineServerConfig {
	cfg.branchControlFilePath = branchControlFilePath
	return cfg
}

func (cfg *commandLineServerConfig) withAllowCleartextPasswords(allow bool) *commandLineServerConfig {
	cfg.allowCleartextPasswords = allow
	return cfg
}

// WithSocket updates the path to the unix socket file
func (cfg *commandLineServerConfig) WithSocket(sockFilePath string) *commandLineServerConfig {
	cfg.socket = sockFilePath
	return cfg
}

// WithRemotesapiPort sets the remotesapi port to use.
func (cfg *commandLineServerConfig) WithRemotesapiPort(port *int) *commandLineServerConfig {
	cfg.remotesapiPort = port
	return cfg
}

func (cfg *commandLineServerConfig) WithRemotesapiReadOnly(readonly *bool) *commandLineServerConfig {
	cfg.remotesapiReadOnly = readonly
	return cfg
}

func (cfg *commandLineServerConfig) goldenMysqlConnectionString() string {
	return cfg.goldenMysqlConn
}

func (cfg *commandLineServerConfig) withGoldenMysqlConnectionString(cs string) *commandLineServerConfig {
	cfg.goldenMysqlConn = cs
	return cfg
}

func (cfg *commandLineServerConfig) EventSchedulerStatus() string {
	switch cfg.eventSchedulerStatus {
	case "", "1":
		return "ON"
	case "0":
		return "OFF"
	default:
		return strings.ToUpper(cfg.eventSchedulerStatus)
	}
}

func (cfg *commandLineServerConfig) withEventScheduler(es string) *commandLineServerConfig {
	cfg.eventSchedulerStatus = es
	return cfg
}
