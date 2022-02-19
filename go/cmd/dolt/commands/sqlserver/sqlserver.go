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
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const (
	hostFlag                = "host"
	portFlag                = "port"
	userFlag                = "user"
	passwordFlag            = "password"
	timeoutFlag             = "timeout"
	readonlyFlag            = "readonly"
	logLevelFlag            = "loglevel"
	multiDBDirFlag          = "multi-db-dir"
	noAutoCommitFlag        = "no-auto-commit"
	configFileFlag          = "config"
	queryParallelismFlag    = "query-parallelism"
	maxConnectionsFlag      = "max-connections"
	persistenceBehaviorFlag = "persistence-behavior"
	privilegeFilePathFlag   = "privilege-file"
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

		{{.EmphasisLeft}}vlog_level{{.EmphasisRight}} - Level of logging provided. Options are: {{.EmphasisLeft}}trace{{.EmphasisRight}}, {{.EmphasisLeft}}debug{{.EmphasisRight}}, {{.EmphasisLeft}}info{{.EmphasisRight}}, {{.EmphasisLeft}}warning{{.EmphasisRight}}, {{.EmphasisLeft}}error{{.EmphasisRight}}, and {{.EmphasisLeft}}fatal{{.EmphasisRight}}.

		{{.EmphasisLeft}}behavior.read_only{{.EmphasisRight}} - If true database modification is disabled

		{{.EmphasisLeft}}behavior.autocommit{{.EmphasisRight}} - If true write queries will automatically alter the working set. When working with autocommit enabled it is highly recommended that listener.max_connections be set to 1 as concurrency issues will arise otherwise

		{{.EmphasisLeft}}user.name{{.EmphasisRight}} - The username that connections should use for authentication

		{{.EmphasisLeft}}user.password{{.EmphasisRight}} - The password that connections should use for authentication.

		{{.EmphasisLeft}}listener.host{{.EmphasisRight}} - The host address that the server will run on.  This may be {{.EmphasisLeft}}localhost{{.EmphasisRight}} or an IPv4 or IPv6 address

		{{.EmphasisLeft}}listener.port{{.EmphasisRight}} - The port that the server should listen on

		{{.EmphasisLeft}}listener.max_connections{{.EmphasisRight}} - The number of simultaneous connections that the server will accept

		{{.EmphasisLeft}}listener.read_timeout_millis{{.EmphasisRight}} - The number of milliseconds that the server will wait for a read operation

		{{.EmphasisLeft}}listener.write_timeout_millis{{.EmphasisRight}} - The number of milliseconds that the server will wait for a write operation

		{{.EmphasisLeft}}performance.query_parallelism{{.EmphasisRight}} - Amount of go routines spawned to process each query

		{{.EmphasisLeft}}databases{{.EmphasisRight}} - a list of dolt data repositories to make available as SQL databases. If databases is missing or empty then the working directory must be a valid dolt data repository which will be made available as a SQL database
		
		{{.EmphasisLeft}}databases[i].path{{.EmphasisRight}} - A path to a dolt data repository
		
		{{.EmphasisLeft}}databases[i].name{{.EmphasisRight}} - The name that the database corresponding to the given path should be referenced via SQL

If a config file is not provided many of these settings may be configured on the command line.`,
	Synopsis: []string{
		"--config {{.LessThan}}file{{.GreaterThan}}",
		"[-H {{.LessThan}}host{{.GreaterThan}}] [-P {{.LessThan}}port{{.GreaterThan}}] [-u {{.LessThan}}user{{.GreaterThan}}] [-p {{.LessThan}}password{{.GreaterThan}}] [-t {{.LessThan}}timeout{{.GreaterThan}}] [-l {{.LessThan}}loglevel{{.GreaterThan}}] [--multi-db-dir {{.LessThan}}directory{{.GreaterThan}}] [--query-parallelism {{.LessThan}}num-go-routines{{.GreaterThan}}] [-r]",
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

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd SqlServerCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return commands.CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, sqlServerDocs, ap))
}

func (cmd SqlServerCmd) ArgParser() *argparser.ArgParser {
	serverConfig := DefaultServerConfig()

	ap := argparser.NewArgParser()
	ap.SupportsString(configFileFlag, "", "file", "When provided configuration is taken from the yaml config file and all command line parameters are ignored.")
	ap.SupportsString(hostFlag, "H", "Host address", fmt.Sprintf("Defines the host address that the server will run on (default `%v`)", serverConfig.Host()))
	ap.SupportsUint(portFlag, "P", "Port", fmt.Sprintf("Defines the port that the server will run on (default `%v`)", serverConfig.Port()))
	ap.SupportsString(userFlag, "u", "User", fmt.Sprintf("Defines the server user (default `%v`)", serverConfig.User()))
	ap.SupportsString(passwordFlag, "p", "Password", fmt.Sprintf("Defines the server password (default `%v`)", serverConfig.Password()))
	ap.SupportsInt(timeoutFlag, "t", "Connection timeout", fmt.Sprintf("Defines the timeout, in seconds, used for connections\nA value of `0` represents an infinite timeout (default `%v`)", serverConfig.ReadTimeout()))
	ap.SupportsFlag(readonlyFlag, "r", "Disables modification of the database")
	ap.SupportsString(logLevelFlag, "l", "Log level", fmt.Sprintf("Defines the level of logging provided\nOptions are: `trace', `debug`, `info`, `warning`, `error`, `fatal` (default `%v`)", serverConfig.LogLevel()))
	ap.SupportsString(multiDBDirFlag, "", "directory", "Defines a directory whose subdirectories should all be dolt data repositories accessible as independent databases.")
	ap.SupportsFlag(noAutoCommitFlag, "", "When provided sessions will not automatically commit their changes to the working set. Anything not manually committed will be lost.")
	ap.SupportsInt(queryParallelismFlag, "", "num-go-routines", fmt.Sprintf("Set the number of go routines spawned to handle each query (default `%d`)", serverConfig.QueryParallelism()))
	ap.SupportsInt(maxConnectionsFlag, "", "max-connections", fmt.Sprintf("Set the number of connections handled by the server (default `%d`)", serverConfig.MaxConnections()))
	ap.SupportsInt(persistenceBehaviorFlag, "", "persistence-behavior", fmt.Sprintf("Indicate whether to `load` or `ignore` persisted global variables (default `%s`)", serverConfig.PersistenceBehavior()))
	ap.SupportsString(privilegeFilePathFlag, "", "Privilege File", "Points to the file that privileges will be loaded from, in addition to being overwritten when privileges have been modified.")
	return ap
}

// EventType returns the type of the event to log
func (cmd SqlServerCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SQL_SERVER
}

// RequiresRepo indicates that this command does not have to be run from within a dolt data repository directory.
// In this case it is because this command supports the multiDBDirFlag which can pass in a directory.  In the event that
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

func startServer(ctx context.Context, versionStr, commandStr string, args []string, dEnv *env.DoltEnv, serverController *ServerController) int {
	ap := SqlServerCmd{}.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, sqlServerDocs, ap))

	// We need a username and password for many SQL commands, so set defaults if they don't exist
	dEnv.Config.SetFailsafes(env.DefaultFailsafeConfig)

	apr := cli.ParseArgsOrDie(ap, args, help)
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

func GetServerConfig(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (ServerConfig, error) {
	if cfgFile, ok := apr.GetValue(configFileFlag); ok {
		return getYAMLServerConfig(dEnv.FS, cfgFile)
	}
	return getCommandLineServerConfig(dEnv, apr)
}

func getCommandLineServerConfig(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (ServerConfig, error) {
	serverConfig := DefaultServerConfig()

	if host, ok := apr.GetValue(hostFlag); ok {
		serverConfig.withHost(host)
	}
	if port, ok := apr.GetInt(portFlag); ok {
		serverConfig.withPort(port)
	}
	if user, ok := apr.GetValue(userFlag); ok {
		serverConfig.withUser(user)
	}
	if password, ok := apr.GetValue(passwordFlag); ok {
		serverConfig.withPassword(password)
	}
	if timeoutStr, ok := apr.GetValue(timeoutFlag); ok {
		timeout, err := strconv.ParseUint(timeoutStr, 10, 64)

		if err != nil {
			return nil, fmt.Errorf("invalid value for --timeout '%s'", timeoutStr)
		}

		serverConfig.withTimeout(timeout * 1000)
	}
	if _, ok := apr.GetValue(readonlyFlag); ok {
		serverConfig.withReadOnly(true)
	}
	if logLevel, ok := apr.GetValue(logLevelFlag); ok {
		serverConfig.withLogLevel(LogLevel(logLevel))
	}
	if multiDBDir, ok := apr.GetValue(multiDBDirFlag); ok {
		dbNamesAndPaths, err := env.DBNamesAndPathsFromDir(dEnv.FS, multiDBDir)

		if err != nil {
			return nil, errors.New("failed to read databases in path specified by --multi-db-dir. error: " + err.Error())
		}

		// We set datadir to multi-db-dir here too
		// TODO: rename multi-db-dir to data_dir
		serverConfig.withDBNamesAndPaths(dbNamesAndPaths).withDataDir(multiDBDir)
	}

	if queryParallelism, ok := apr.GetInt(queryParallelismFlag); ok {
		serverConfig.withQueryParallelism(queryParallelism)
	}

	if maxConnections, ok := apr.GetInt(maxConnectionsFlag); ok {
		serverConfig.withMaxConnections(uint64(maxConnections))
	}

	serverConfig.autoCommit = !apr.Contains(noAutoCommitFlag)
	if persistenceBehavior, ok := apr.GetValue(persistenceBehaviorFlag); ok {
		serverConfig.withPersistenceBehavior(persistenceBehavior)
	}

	if privilegeFilePath, ok := apr.GetValue(privilegeFilePathFlag); ok {
		serverConfig.withPrivilegeFilePath(privilegeFilePath)
	}

	return serverConfig, nil
}

func getYAMLServerConfig(fs filesys.Filesys, path string) (ServerConfig, error) {
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
