// Copyright 2019 Liquidata, Inc.
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
	"strconv"

	"github.com/fatih/color"
	"gopkg.in/yaml.v2"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

const (
	hostFlag         = "host"
	portFlag         = "port"
	userFlag         = "user"
	passwordFlag     = "password"
	timeoutFlag      = "timeout"
	readonlyFlag     = "readonly"
	logLevelFlag     = "loglevel"
	multiDBDirFlag   = "multi-db-dir"
	noAutoCommitFlag = "no-auto-commit"
	configFileFlag   = "config"
)

var sqlServerDocs = cli.CommandDocumentationContent{
	ShortDesc: "Start a MySQL-compatible server.",
	LongDesc:  `Start a MySQL-compatible server which can be connected to by MySQL clients.`,
	Synopsis: []string{
		"[-H {{.LessThan}}host{{.GreaterThan}}] [-P {{.LessThan}}port{{.GreaterThan}}] [-u {{.LessThan}}user{{.GreaterThan}}] [-p {{.LessThan}}password{{.GreaterThan}}] [-t {{.LessThan}}timeout{{.GreaterThan}}] [-l {{.LessThan}}loglevel{{.GreaterThan}}] [--multi-db-dir {{.LessThan}}directory{{.GreaterThan}}] [-r]",
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
	return "Starts a MySQL-compatible server."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd SqlServerCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, sqlServerDocs, ap))
}

func createArgParser() *argparser.ArgParser {
	serverConfig := DefaultServerConfig()

	ap := argparser.NewArgParser()
	ap.SupportsString(hostFlag, "H", "Host address", fmt.Sprintf("Defines the host address that the server will run on (default `%v`)", serverConfig.Host()))
	ap.SupportsUint(portFlag, "P", "Port", fmt.Sprintf("Defines the port that the server will run on (default `%v`)", serverConfig.Port()))
	ap.SupportsString(userFlag, "u", "User", fmt.Sprintf("Defines the server user (default `%v`)", serverConfig.User()))
	ap.SupportsString(passwordFlag, "p", "Password", fmt.Sprintf("Defines the server password (default `%v`)", serverConfig.Password()))
	ap.SupportsInt(timeoutFlag, "t", "Connection timeout", fmt.Sprintf("Defines the timeout, in seconds, used for connections\nA value of `0` represents an infinite timeout (default `%v`)", serverConfig.ReadTimeout()))
	ap.SupportsFlag(readonlyFlag, "r", "Disables modification of the database")
	ap.SupportsString(logLevelFlag, "l", "Log level", fmt.Sprintf("Defines the level of logging provided\nOptions are: `debug`, `info`, `warning`, `error`, `fatal` (default `%v`)", serverConfig.LogLevel()))
	ap.SupportsString(multiDBDirFlag, "", "directory", "Defines a directory whose subdirectories should all be dolt data repositories accessible as independent databases.")
	ap.SupportsFlag(noAutoCommitFlag, "", "When provided sessions will not automatically commit their changes to the working set. Anything not manually committed will be lost.")
	ap.SupportsString(configFileFlag, "", "file", "When provided configuration is taken from the yaml config file and all command line parameters are ignored.")
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
	return startServer(ctx, cmd.VersionStr, commandStr, args, dEnv, nil)
}

func startServer(ctx context.Context, versionStr, commandStr string, args []string, dEnv *env.DoltEnv, serverController *ServerController) int {
	ap := createArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, sqlServerDocs, ap))

	apr := cli.ParseArgs(ap, args, help)
	serverConfig, err := getServerConfig(dEnv, apr)

	if err != nil {
		serverController.StopServer()
		serverController.serverStopped(err)

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

func getServerConfig(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (ServerConfig, error) {
	cfgFile, ok := apr.GetValue(configFileFlag)

	if ok {
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

		serverConfig.withDBNamesAndPaths(dbNamesAndPaths)
	} else {
		if !cli.CheckEnvIsValid(dEnv) {
			return nil, errors.New("not a valid dolt directory")
		}
	}

	serverConfig.autoCommit = !apr.Contains(noAutoCommitFlag)
	return serverConfig, nil
}

func getYAMLServerConfig(fs filesys.Filesys, path string) (ServerConfig, error) {
	data, err := fs.ReadFile(path)

	if err != nil {
		return nil, fmt.Errorf("Failed to read file '%s'. Error: %s", path, err.Error())
	}

	var cfg YAMLConfig
	err = yaml.Unmarshal(data, &cfg)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse yaml file '%s'. Error: %s", path, err.Error())
	}

	return cfg, nil
}
