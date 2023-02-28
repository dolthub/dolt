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
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const (
	hostFlag                    = "host"
	portFlag                    = "port"
	passwordFlag                = "password"
	timeoutFlag                 = "timeout"
	readonlyFlag                = "readonly"
	logLevelFlag                = "loglevel"
	noAutoCommitFlag            = "no-auto-commit"
	configFileFlag              = "config"
	queryParallelismFlag        = "query-parallelism"
	maxConnectionsFlag          = "max-connections"
	persistenceBehaviorFlag     = "persistence-behavior"
	allowCleartextPasswordsFlag = "allow-cleartext-passwords"
	socketFlag                  = "socket"
	remotesapiPortFlag          = "remotesapi-port"
	goldenMysqlConn             = "golden"
)

func indentLines(s string) string {
	sb := strings.Builder{}
	lines := strings.Split(s, "\n")
	for _, line := range lines {
		sb.WriteRune('\t')
		sb.WriteString(line)
		sb.WriteRune('\n')
	}
	return sb.String()
}

var sqlServerDocs = cli.CommandDocumentationContent{
	ShortDesc: "Start a MySQL-compatible server.",
	LongDesc: "By default, starts a MySQL-compatible server on the dolt database in the current directory. " +
		"Databases are named after the directories they appear in, with all non-alphanumeric characters replaced by the _ character. " +
		"Parameters can be specified using a yaml configuration file passed to the server via " +
		"{{.EmphasisLeft}}--config <file>{{.EmphasisRight}}, or by using the supported switches and flags to configure " +
		"the server directly on the command line. If {{.EmphasisLeft}}--config <file>{{.EmphasisRight}} is provided all" +
		" other command line arguments are ignored.\n\nThis is an example yaml configuration file showing all supported" +
		" items and their default values:\n\n" +
		indentLines(serverConfigAsYAMLConfig(DefaultServerConfig()).String()) + "\n\n" + `
SUPPORTED CONFIG FILE FIELDS:

{{.EmphasisLeft}}vlog_level{{.EmphasisRight}}: Level of logging provided. Options are: {{.EmphasisLeft}}trace{{.EmphasisRight}}, {{.EmphasisLeft}}debug{{.EmphasisRight}}, {{.EmphasisLeft}}info{{.EmphasisRight}}, {{.EmphasisLeft}}warning{{.EmphasisRight}}, {{.EmphasisLeft}}error{{.EmphasisRight}}, and {{.EmphasisLeft}}fatal{{.EmphasisRight}}.

{{.EmphasisLeft}}behavior.read_only{{.EmphasisRight}}: If true database modification is disabled

{{.EmphasisLeft}}behavior.autocommit{{.EmphasisRight}}: If true write queries will automatically alter the working set. When working with autocommit enabled it is highly recommended that listener.max_connections be set to 1 as concurrency issues will arise otherwise

{{.EmphasisLeft}}user.name{{.EmphasisRight}}: The username that connections should use for authentication

{{.EmphasisLeft}}user.password{{.EmphasisRight}}: The password that connections should use for authentication.

{{.EmphasisLeft}}listener.host{{.EmphasisRight}}: The host address that the server will run on.  This may be {{.EmphasisLeft}}localhost{{.EmphasisRight}} or an IPv4 or IPv6 address

{{.EmphasisLeft}}listener.port{{.EmphasisRight}}: The port that the server should listen on

{{.EmphasisLeft}}listener.max_connections{{.EmphasisRight}}: The number of simultaneous connections that the server will accept

{{.EmphasisLeft}}listener.read_timeout_millis{{.EmphasisRight}}: The number of milliseconds that the server will wait for a read operation

{{.EmphasisLeft}}listener.write_timeout_millis{{.EmphasisRight}}: The number of milliseconds that the server will wait for a write operation

{{.EmphasisLeft}}performance.query_parallelism{{.EmphasisRight}}: Amount of go routines spawned to process each query

{{.EmphasisLeft}}databases{{.EmphasisRight}}: a list of dolt data repositories to make available as SQL databases. If databases is missing or empty then the working directory must be a valid dolt data repository which will be made available as a SQL database

{{.EmphasisLeft}}databases[i].path{{.EmphasisRight}}: A path to a dolt data repository

{{.EmphasisLeft}}databases[i].name{{.EmphasisRight}}: The name that the database corresponding to the given path should be referenced via SQL

If a config file is not provided many of these settings may be configured on the command line.`,
	Synopsis: []string{
		"--config {{.LessThan}}file{{.GreaterThan}}",
		"[-H {{.LessThan}}host{{.GreaterThan}}] [-P {{.LessThan}}port{{.GreaterThan}}] [-u {{.LessThan}}user{{.GreaterThan}}] [-p {{.LessThan}}password{{.GreaterThan}}] [-t {{.LessThan}}timeout{{.GreaterThan}}] [-l {{.LessThan}}loglevel{{.GreaterThan}}] [--data-dir {{.LessThan}}directory{{.GreaterThan}}] [--query-parallelism {{.LessThan}}num-go-routines{{.GreaterThan}}] [-r]",
	},
}

type SqlServerCmd struct {
	VersionStr string
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd SqlServerCmd) Name() string {
	return "sql-server"
}

// Description returns a description of the command
func (cmd SqlServerCmd) Description() string {
	return sqlServerDocs.ShortDesc
}

func (cmd SqlServerCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(sqlServerDocs, ap)
}

func (cmd SqlServerCmd) ArgParser() *argparser.ArgParser {
	serverConfig := DefaultServerConfig()

	ap := argparser.NewArgParser()
	ap.SupportsString(configFileFlag, "", "file", "When provided configuration is taken from the yaml config file and all command line parameters are ignored.")
	ap.SupportsString(hostFlag, "H", "host address", fmt.Sprintf("Defines the host address that the server will run on. Defaults to `%v`.", serverConfig.Host()))
	ap.SupportsUint(portFlag, "P", "port", fmt.Sprintf("Defines the port that the server will run on. Defaults to `%v`.", serverConfig.Port()))
	ap.SupportsString(commands.UserFlag, "u", "user", fmt.Sprintf("Defines the server user. Defaults to `%v`. This should be explicit if desired.", serverConfig.User()))
	ap.SupportsString(passwordFlag, "p", "password", fmt.Sprintf("Defines the server password. Defaults to `%v`.", serverConfig.Password()))
	ap.SupportsInt(timeoutFlag, "t", "connection timeout", fmt.Sprintf("Defines the timeout, in seconds, used for connections\nA value of `0` represents an infinite timeout. Defaults to `%v`.", serverConfig.ReadTimeout()))
	ap.SupportsFlag(readonlyFlag, "r", "Disable modification of the database.")
	ap.SupportsString(logLevelFlag, "l", "log level", fmt.Sprintf("Defines the level of logging provided\nOptions are: `trace`, `debug`, `info`, `warning`, `error`, `fatal`. Defaults to `%v`.", serverConfig.LogLevel()))
	ap.SupportsString(commands.DataDirFlag, "", "directory", "Defines a directory whose subdirectories should all be dolt data repositories accessible as independent databases within. Defaults to the current directory.")
	ap.SupportsString(commands.MultiDBDirFlag, "", "directory", "Defines a directory whose subdirectories should all be dolt data repositories accessible as independent databases within. Defaults to the current directory. This is deprecated, you should use `--data-dir` instead.")
	ap.SupportsString(commands.CfgDirFlag, "", "directory", "Defines a directory that contains configuration files for dolt. Defaults to `$data-dir/.doltcfg`. Will only be created if there is a change that affect configuration settings.")
	ap.SupportsFlag(noAutoCommitFlag, "", "Set @@autocommit = off for the server.")
	ap.SupportsInt(queryParallelismFlag, "", "num-go-routines", fmt.Sprintf("Set the number of go routines spawned to handle each query. Defaults to `%d`.", serverConfig.QueryParallelism()))
	ap.SupportsInt(maxConnectionsFlag, "", "max-connections", fmt.Sprintf("Set the number of connections handled by the server. Defaults to `%d`.", serverConfig.MaxConnections()))
	ap.SupportsString(persistenceBehaviorFlag, "", "persistence-behavior", fmt.Sprintf("Indicate whether to `load` or `ignore` persisted global variables. Defaults to `%s`.", serverConfig.PersistenceBehavior()))
	ap.SupportsString(commands.PrivsFilePathFlag, "", "privilege file", "Path to a file to load and store users and grants. Defaults to `$doltcfg-dir/privileges.db`. Will only be created if there is a change to privileges.")
	ap.SupportsString(commands.BranchCtrlPathFlag, "", "branch control file", "Path to a file to load and store branch control permissions. Defaults to `$doltcfg-dir/branch_control.db`. Will only be created if there is a change to branch control permissions.")
	ap.SupportsString(allowCleartextPasswordsFlag, "", "allow-cleartext-passwords", "Allows use of cleartext passwords. Defaults to false.")
	ap.SupportsOptionalString(socketFlag, "", "socket file", "Path for the unix socket file. Defaults to '/tmp/mysql.sock'.")
	ap.SupportsUint(remotesapiPortFlag, "", "remotesapi port", "Sets the port for a server which can expose the databases in this sql-server over remotesapi.")
	ap.SupportsString(goldenMysqlConn, "", "mysql connection string", "Provides a connection string to a MySQL instance to be user to validate query results")
	return ap
}

// EventType returns the type of the event to log
func (cmd SqlServerCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SQL_SERVER
}

// RequiresRepo indicates that this command does not have to be run from within a dolt data repository directory.
// In this case it is because this command supports the dataDirFlag which can pass in a directory.  In the event that
// that parameter is not provided there is additional error handling within this command to make sure that this was in
// fact run from within a dolt data repository directory.
func (cmd SqlServerCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd SqlServerCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	controller := NewServerController()
	newCtx, cancelF := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		controller.StopServer()
		cancelF()
	}()
	return startServer(newCtx, cmd.VersionStr, commandStr, args, dEnv, controller)
}

func validateSqlServerArgs(apr *argparser.ArgParseResults) error {
	_, multiDbDir := apr.GetValue(commands.MultiDBDirFlag)
	if multiDbDir {
		cli.PrintErrln("WARNING: --multi-db-dir is deprecated, use --data-dir instead")
	}
	return nil
}

func startServer(ctx context.Context, versionStr, commandStr string, args []string, dEnv *env.DoltEnv, serverController *ServerController) int {
	ap := SqlServerCmd{}.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, sqlServerDocs, ap))

	// We need a username and password for many SQL commands, so set defaults if they don't exist
	dEnv.Config.SetFailsafes(env.DefaultFailsafeConfig)

	apr := cli.ParseArgsOrDie(ap, args, help)
	if err := validateSqlServerArgs(apr); err != nil {
		return 1
	}
	serverConfig, err := GetServerConfig(dEnv, apr)
	if err != nil {
		if serverController != nil {
			serverController.StopServer()
			serverController.serverStopped(err)
		}

		cli.PrintErrln(color.RedString("Failed to start server. Bad Configuration"))
		cli.PrintErrln(err.Error())
		return 1
	}
	if err = SetupDoltConfig(dEnv, apr, serverConfig); err != nil {
		if serverController != nil {
			serverController.StopServer()
			serverController.serverStopped(err)
		}

		cli.PrintErrln(color.RedString("Failed to start server. Bad Configuration"))
		cli.PrintErrln(err.Error())
		return 1
	}

	cli.PrintErrf("Starting server with Config %v\n", ConfigInfo(serverConfig))

	if startError, closeError := Serve(ctx, versionStr, serverConfig, serverController, dEnv); startError != nil || closeError != nil {
		if startError != nil {
			cli.PrintErrln(startError)
		}
		if closeError != nil {
			cli.PrintErrln(closeError)
		}
		return 1
	}

	return 0
}

// GetServerConfig returns ServerConfig that is set either from yaml file if given, if not it is set with values defined
// on command line. Server config variables not defined are set to default values.
func GetServerConfig(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (ServerConfig, error) {
	var yamlCfg YAMLConfig
	if cfgFile, ok := apr.GetValue(configFileFlag); ok {
		cfg, err := getYAMLServerConfig(dEnv.FS, cfgFile)
		if err != nil {
			return nil, err
		}
		yamlCfg = cfg.(YAMLConfig)
	} else {
		return getCommandLineServerConfig(dEnv, apr)
	}

	// if command line user argument was given, replace yaml's user and password
	if user, hasUser := apr.GetValue(commands.UserFlag); hasUser {
		pass, _ := apr.GetValue(passwordFlag)
		yamlCfg.UserConfig.Name = &user
		yamlCfg.UserConfig.Password = &pass
	}

	if connStr, ok := apr.GetValue(goldenMysqlConn); ok {
		cli.Println(connStr)
		yamlCfg.GoldenMysqlConn = &connStr
	}

	return yamlCfg, nil
}

// SetupDoltConfig updates the given server config with where to create .doltcfg directory
func SetupDoltConfig(dEnv *env.DoltEnv, apr *argparser.ArgParseResults, config ServerConfig) error {
	if _, ok := apr.GetValue(configFileFlag); ok {
		return nil
	}

	serverConfig := config.(*commandLineServerConfig)

	_, dataDirFlag1 := apr.GetValue(commands.MultiDBDirFlag)
	_, dataDirFlag2 := apr.GetValue(commands.DataDirFlag)
	dataDirSpecified := dataDirFlag1 || dataDirFlag2

	var cfgDirPath string
	dataDir := serverConfig.DataDir()
	cfgDir, cfgDirSpecified := apr.GetValue(commands.CfgDirFlag)
	if cfgDirSpecified {
		cfgDirPath = cfgDir
	} else if dataDirSpecified {
		cfgDirPath = filepath.Join(dataDir, commands.DefaultCfgDirName)
	} else {
		// Look in parent directory for doltcfg
		parentDirCfg := filepath.Join("..", commands.DefaultCfgDirName)
		parentExists, isDir := dEnv.FS.Exists(parentDirCfg)
		parentDirExists := parentExists && isDir

		// Look in data directory (which is necessarily current directory) for doltcfg
		currDirCfg := filepath.Join(dataDir, commands.DefaultCfgDirName)
		currExists, isDir := dEnv.FS.Exists(currDirCfg)
		currDirExists := currExists && isDir

		// Error if both current and parent exist
		if currDirExists && parentDirExists {
			p1, err := dEnv.FS.Abs(cfgDirPath)
			if err != nil {
				return err
			}
			p2, err := dEnv.FS.Abs(parentDirCfg)
			if err != nil {
				return err
			}
			return commands.ErrMultipleDoltCfgDirs.New(p1, p2)
		}

		// Assign the one that exists, defaults to current if neither exist
		if parentDirExists {
			cfgDirPath = parentDirCfg
		} else {
			cfgDirPath = currDirCfg
		}
	}
	serverConfig.withCfgDir(cfgDirPath)

	if privsFp, ok := apr.GetValue(commands.PrivsFilePathFlag); ok {
		serverConfig.withPrivilegeFilePath(privsFp)
	} else {
		path, err := dEnv.FS.Abs(filepath.Join(cfgDirPath, commands.DefaultPrivsName))
		if err != nil {
			return err
		}
		serverConfig.withPrivilegeFilePath(path)
	}

	if branchControlFilePath, ok := apr.GetValue(commands.BranchCtrlPathFlag); ok {
		serverConfig.withBranchControlFilePath(branchControlFilePath)
	} else {
		path, err := dEnv.FS.Abs(filepath.Join(cfgDirPath, commands.DefaultBranchCtrlName))
		if err != nil {
			return err
		}
		serverConfig.withBranchControlFilePath(path)
	}

	return nil
}

// getCommandLineServerConfig sets server config variables and persisted global variables with values defined on command line.
// If not defined, it sets variables to default values.
func getCommandLineServerConfig(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (ServerConfig, error) {
	serverConfig := DefaultServerConfig()

	if sock, ok := apr.GetValue(socketFlag); ok {
		// defined without value gets default
		if sock == "" {
			sock = defaultUnixSocketFilePath
		}
		serverConfig.WithSocket(sock)
	}

	if host, ok := apr.GetValue(hostFlag); ok {
		serverConfig.WithHost(host)
	}

	if port, ok := apr.GetInt(portFlag); ok {
		serverConfig.WithPort(port)
	}

	if user, ok := apr.GetValue(commands.UserFlag); ok {
		serverConfig.withUser(user)
	}

	if password, ok := apr.GetValue(passwordFlag); ok {
		serverConfig.withPassword(password)
	}

	if port, ok := apr.GetInt(remotesapiPortFlag); ok {
		serverConfig.WithRemotesapiPort(&port)
	}

	if persistenceBehavior, ok := apr.GetValue(persistenceBehaviorFlag); ok {
		serverConfig.withPersistenceBehavior(persistenceBehavior)
	}

	if timeoutStr, ok := apr.GetValue(timeoutFlag); ok {
		timeout, err := strconv.ParseUint(timeoutStr, 10, 64)

		if err != nil {
			return nil, fmt.Errorf("invalid value for --timeout '%s'", timeoutStr)
		}

		serverConfig.withTimeout(timeout * 1000)

		err = sql.SystemVariables.SetGlobal("net_read_timeout", timeout*1000)
		if err != nil {
			return nil, fmt.Errorf("failed to set net_read_timeout. Error: %s", err.Error())
		}
		err = sql.SystemVariables.SetGlobal("net_write_timeout", timeout*1000)
		if err != nil {
			return nil, fmt.Errorf("failed to set net_write_timeout. Error: %s", err.Error())
		}
	}

	if _, ok := apr.GetValue(readonlyFlag); ok {
		serverConfig.withReadOnly(true)
	}

	if logLevel, ok := apr.GetValue(logLevelFlag); ok {
		serverConfig.withLogLevel(LogLevel(strings.ToLower(logLevel)))
	}

	if dataDir, ok := apr.GetValue(commands.MultiDBDirFlag); ok {
		serverConfig.withDataDir(dataDir)
	}

	if dataDir, ok := apr.GetValue(commands.DataDirFlag); ok {
		serverConfig.withDataDir(dataDir)
	}

	if queryParallelism, ok := apr.GetInt(queryParallelismFlag); ok {
		serverConfig.withQueryParallelism(queryParallelism)
	}

	if maxConnections, ok := apr.GetInt(maxConnectionsFlag); ok {
		serverConfig.withMaxConnections(uint64(maxConnections))
		err := sql.SystemVariables.SetGlobal("max_connections", uint64(maxConnections))
		if err != nil {
			return nil, fmt.Errorf("failed to set max_connections. Error: %s", err.Error())
		}
	}

	serverConfig.autoCommit = !apr.Contains(noAutoCommitFlag)
	serverConfig.allowCleartextPasswords = apr.Contains(allowCleartextPasswordsFlag)

	if connStr, ok := apr.GetValue(goldenMysqlConn); ok {
		cli.Println(connStr)
		serverConfig.withGoldenMysqlConnectionString(connStr)
	}

	return serverConfig, nil
}

// getYAMLServerConfig returns server config variables with values defined in yaml file.
func getYAMLServerConfig(fs filesys.Filesys, path string) (ServerConfig, error) {
	data, err := fs.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to read file '%s'. Error: %s", path, err.Error())
	}

	cfg, err := NewYamlConfig(data)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse yaml file '%s'. Error: %s", path, err.Error())
	}

	if cfg.ListenerConfig.MaxConnections != nil {
		err = sql.SystemVariables.SetGlobal("max_connections", *cfg.ListenerConfig.MaxConnections)
		if err != nil {
			return nil, fmt.Errorf("Failed to set max_connections from yaml file '%s'. Error: %s", path, err.Error())
		}
	}
	if cfg.ListenerConfig.ReadTimeoutMillis != nil {
		err = sql.SystemVariables.SetGlobal("net_read_timeout", *cfg.ListenerConfig.ReadTimeoutMillis)
		if err != nil {
			return nil, fmt.Errorf("Failed to set net_read_timeout from yaml file '%s'. Error: %s", path, err.Error())
		}
	}
	if cfg.ListenerConfig.WriteTimeoutMillis != nil {
		err = sql.SystemVariables.SetGlobal("net_write_timeout", *cfg.ListenerConfig.WriteTimeoutMillis)
		if err != nil {
			return nil, fmt.Errorf("Failed to set net_write_timeout from yaml file '%s'. Error: %s", path, err.Error())
		}
	}

	return cfg, nil
}
