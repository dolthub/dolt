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

	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

type commandLineServerConfig struct {
	host                    string
	port                    int
	user                    string
	password                string
	timeout                 uint64
	readOnly                bool
	logLevel                servercfg.LogLevel
	logFormat				servercfg.LogFormat
	dataDir                 string
	cfgDir                  string
	autoCommit              bool
	doltTransactionCommit   bool
	maxConnections          uint64
	tlsKey                  string
	tlsCert                 string
	requireSecureTransport  bool
	maxLoggedQueryLen       int
	shouldEncodeLoggedQuery bool
	privilegeFilePath       string
	branchControlFilePath   string
	allowCleartextPasswords bool
	socket                  string
	remotesapiPort          *int
	remotesapiReadOnly      *bool
	goldenMysqlConn         string
	eventSchedulerStatus    string
	valuesSet               map[string]struct{}
}

var _ servercfg.ServerConfig = (*commandLineServerConfig)(nil)

// DefaultCommandLineServerConfig creates a `*ServerConfig` that has all of the options set to their default values.
func DefaultCommandLineServerConfig() *commandLineServerConfig {
	return &commandLineServerConfig{
		host:                    servercfg.DefaultHost,
		port:                    servercfg.DefaultPort,
		password:                servercfg.DefaultPass,
		timeout:                 servercfg.DefaultTimeout,
		readOnly:                servercfg.DefaultReadOnly,
		logLevel:                servercfg.DefaultLogLevel,
		logFormat:               servercfg.DefaultLogFormat
		autoCommit:              servercfg.DefaultAutoCommit,
		maxConnections:          servercfg.DefaultMaxConnections,
		dataDir:                 servercfg.DefaultDataDir,
		cfgDir:                  filepath.Join(servercfg.DefaultDataDir, servercfg.DefaultCfgDir),
		privilegeFilePath:       filepath.Join(servercfg.DefaultDataDir, servercfg.DefaultCfgDir, servercfg.DefaultPrivilegeFilePath),
		branchControlFilePath:   filepath.Join(servercfg.DefaultDataDir, servercfg.DefaultCfgDir, servercfg.DefaultBranchControlFilePath),
		allowCleartextPasswords: servercfg.DefaultAllowCleartextPasswords,
		maxLoggedQueryLen:       servercfg.DefaultMaxLoggedQueryLen,
		valuesSet:               map[string]struct{}{},
	}
}

// NewCommandLineConfig returns server config based on the credentials and command line arguments given. The dataDirOverride
// parameter is used to override the data dir specified in the command line arguments. This comes up when there are
// situations where there are multiple ways to specify the data dir.
func NewCommandLineConfig(creds *cli.UserPassword, apr *argparser.ArgParseResults, dataDirOverride string) (servercfg.ServerConfig, error) {
	config := DefaultCommandLineServerConfig()

	if sock, ok := apr.GetValue(socketFlag); ok {
		// defined without value gets default
		if sock == "" {
			sock = servercfg.DefaultUnixSocketFilePath
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
			config.valuesSet[servercfg.UserKey] = struct{}{}
		}
		if password, ok := apr.GetValue(cli.PasswordFlag); ok {
			config.withPassword(password)
			config.valuesSet[servercfg.PasswordKey] = struct{}{}
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
		config.withLogLevel(servercfg.LogLevel(strings.ToLower(logLevel)))
	}

	if dataDir, ok := apr.GetValue(commands.MultiDBDirFlag); ok {
		config.withDataDir(dataDir)
	}

	// We explicitly don't use the dataDir flag from the APR here. The data dir flag is pulled out early and converted
	// to an absolute path. It is read in the GetDataDirPreStart function, which is called early in dolt.go to get the
	// data dir for any dolt process. This complexity exists because the server's config.yaml config file can contain the
	// dataDir, but we don't execute any server specific logic until after the database environment is initialized.
	if dataDirOverride != "" {
		config.withDataDir(dataDirOverride)
	} else {
		if dd, ok := apr.GetValue(commands.DataDirFlag); ok {
			config.withDataDir(dd)
		}
	}

	if maxConnections, ok := apr.GetInt(maxConnectionsFlag); ok {
		config.withMaxConnections(uint64(maxConnections))
	}

	config.autoCommit = !apr.Contains(noAutoCommitFlag)
	if apr.Contains(noAutoCommitFlag) {
		config.valuesSet[servercfg.AutoCommitKey] = struct{}{}
	}

	config.allowCleartextPasswords = apr.Contains(allowCleartextPasswordsFlag)
	if apr.Contains(allowCleartextPasswordsFlag) {
		config.valuesSet[servercfg.AllowCleartextPasswordsKey] = struct{}{}
	}

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

// UserIsSpecified returns true if the configuration explicitly specified a user.
func (cfg *commandLineServerConfig) UserIsSpecified() bool {
	return cfg.user != ""
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
func (cfg *commandLineServerConfig) LogLevel() servercfg.LogLevel {
	return cfg.logLevel
}

// LogFormat returns the format of logging that the server will use.
func (cfg *commandLineServerConfig) LogFormat() servercfg.LogFormat {
	return cfg.logFormat
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
	return servercfg.DefaultMetricsHost
}

func (cfg *commandLineServerConfig) MetricsPort() int {
	return servercfg.DefaultMetricsPort
}

func (cfg *commandLineServerConfig) RemotesapiPort() *int {
	return cfg.remotesapiPort
}

func (cfg *commandLineServerConfig) RemotesapiReadOnly() *bool {
	return cfg.remotesapiReadOnly
}

func (cfg *commandLineServerConfig) ClusterConfig() servercfg.ClusterConfig {
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
func (cfg *commandLineServerConfig) UserVars() []servercfg.UserSessionVars {
	return nil
}

func (cfg *commandLineServerConfig) SystemVars() map[string]interface{} {
	return nil
}

func (cfg *commandLineServerConfig) JwksConfig() []servercfg.JwksConfig {
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
	cfg.valuesSet[servercfg.HostKey] = struct{}{}
	return cfg
}

// WithPort updates the port and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) WithPort(port int) *commandLineServerConfig {
	cfg.port = port
	cfg.valuesSet[servercfg.PortKey] = struct{}{}
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
	cfg.valuesSet[servercfg.ReadTimeoutKey] = struct{}{}
	cfg.valuesSet[servercfg.WriteTimeoutKey] = struct{}{}
	return cfg
}

// withReadOnly updates the read only flag and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) withReadOnly(readonly bool) *commandLineServerConfig {
	cfg.readOnly = readonly
	cfg.valuesSet[servercfg.ReadOnlyKey] = struct{}{}
	return cfg
}

// withLogLevel updates the log level and returns the called `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) withLogLevel(loglevel servercfg.LogLevel) *commandLineServerConfig {
	cfg.logLevel = loglevel
	cfg.valuesSet[servercfg.LogLevelKey] = struct{}{}
	return cfg
}

// withMaxConnections updates the maximum number of concurrent connections and returns the called
// `*commandLineServerConfig`, which is useful for chaining calls.
func (cfg *commandLineServerConfig) withMaxConnections(maxConnections uint64) *commandLineServerConfig {
	cfg.maxConnections = maxConnections
	cfg.valuesSet[servercfg.MaxConnectionsKey] = struct{}{}
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
	cfg.valuesSet[servercfg.AllowCleartextPasswordsKey] = struct{}{}
	return cfg
}

// WithSocket updates the path to the unix socket file
func (cfg *commandLineServerConfig) WithSocket(sockFilePath string) *commandLineServerConfig {
	cfg.socket = sockFilePath
	cfg.valuesSet[servercfg.SocketKey] = struct{}{}
	return cfg
}

// WithRemotesapiPort sets the remotesapi port to use.
func (cfg *commandLineServerConfig) WithRemotesapiPort(port *int) *commandLineServerConfig {
	cfg.remotesapiPort = port
	cfg.valuesSet[servercfg.RemotesapiPortKey] = struct{}{}
	return cfg
}

func (cfg *commandLineServerConfig) WithRemotesapiReadOnly(readonly *bool) *commandLineServerConfig {
	cfg.remotesapiReadOnly = readonly
	cfg.valuesSet[servercfg.RemotesapiReadOnlyKey] = struct{}{}
	return cfg
}

func (cfg *commandLineServerConfig) GoldenMysqlConnectionString() string {
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
	cfg.valuesSet[servercfg.EventSchedulerKey] = struct{}{}
	return cfg
}

func (cfg *commandLineServerConfig) ValueSet(value string) bool {
	_, ok := cfg.valuesSet[value]
	return ok
}

func (cfg *commandLineServerConfig) AutoGCBehavior() servercfg.AutoGCBehavior {
	return stubAutoGCBehavior{}
}

// DoltServerConfigReader is the default implementation of ServerConfigReader suitable for parsing Dolt config files
// and command line options.
type DoltServerConfigReader struct{}

// ServerConfigReader is an interface for reading a ServerConfig from a file or command line arguments.
type ServerConfigReader interface {
	// ReadConfigFile reads a config file and returns a ServerConfig for it
	ReadConfigFile(cwdFS filesys.Filesys, file string) (servercfg.ServerConfig, error)
	// ReadConfigArgs reads command line arguments and returns a ServerConfig for them
	ReadConfigArgs(args *argparser.ArgParseResults, dataDirOverride string) (servercfg.ServerConfig, error)
}

var _ ServerConfigReader = DoltServerConfigReader{}

func (d DoltServerConfigReader) ReadConfigFile(cwdFS filesys.Filesys, file string) (servercfg.ServerConfig, error) {
	return servercfg.YamlConfigFromFile(cwdFS, file)
}

func (d DoltServerConfigReader) ReadConfigArgs(args *argparser.ArgParseResults, dataDirOverride string) (servercfg.ServerConfig, error) {
	return NewCommandLineConfig(nil, args, dataDirOverride)
}

type stubAutoGCBehavior struct {
}

func (stubAutoGCBehavior) Enable() bool {
	return false
}
