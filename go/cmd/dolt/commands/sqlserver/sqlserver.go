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
	"fmt"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
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
)

var sqlServerDocs = cli.CommandDocumentationContent{
	ShortDesc: "Start a MySQL-compatible server.",
	LongDesc: `Start a MySQL-compatible server which can be connected to by MySQL clients.

Currently, only {{.EmphasisLeft}}SELECT{{.EmphasisRight}} statements are operational, as support for other statements is still being developed.
`,
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
	ap := createArgParser(DefaultServerConfig())
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, sqlServerDocs, ap))
}

func createArgParser(serverConfig *ServerConfig) *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(hostFlag, "H", "Host address", fmt.Sprintf("Defines the host address that the server will run on (default `%v`)", serverConfig.Host))
	ap.SupportsUint(portFlag, "P", "Port", fmt.Sprintf("Defines the port that the server will run on (default `%v`)", serverConfig.Port))
	ap.SupportsString(userFlag, "u", "User", fmt.Sprintf("Defines the server user (default `%v`)", serverConfig.User))
	ap.SupportsString(passwordFlag, "p", "Password", fmt.Sprintf("Defines the server password (default `%v`)", serverConfig.Password))
	ap.SupportsInt(timeoutFlag, "t", "Connection timeout", fmt.Sprintf("Defines the timeout, in seconds, used for connections\nA value of `0` represents an infinite timeout (default `%v`)", serverConfig.Timeout))
	ap.SupportsFlag(readonlyFlag, "r", "Disables modification of the database")
	ap.SupportsString(logLevelFlag, "l", "Log level", fmt.Sprintf("Defines the level of logging provided\nOptions are: `debug`, `info`, `warning`, `error`, `fatal` (default `%v`)", serverConfig.LogLevel))
	ap.SupportsString(multiDBDirFlag, "", "directory", "Defines a directory whose subdirectories should all be dolt data repositories accessible as independent databases.")
	ap.SupportsFlag(noAutoCommitFlag, "", "When provided sessions will not automatically commit their changes to the working set. Anything not manually committed will be lost.")
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
	return startServer(ctx, commandStr, cmd.VersionStr, args, dEnv, nil)
}

func startServer(ctx context.Context, versionStr, commandStr string, args []string, dEnv *env.DoltEnv, serverController *ServerController) int {
	serverConfig := DefaultServerConfig()
	serverConfig.Version = versionStr

	ap := createArgParser(serverConfig)
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, sqlServerDocs, ap))

	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()

	if host, ok := apr.GetValue(hostFlag); ok {
		serverConfig.Host = host
	}
	if port, ok := apr.GetInt(portFlag); ok {
		serverConfig.Port = port
	}
	if user, ok := apr.GetValue(userFlag); ok {
		serverConfig.User = user
	}
	if password, ok := apr.GetValue(passwordFlag); ok {
		serverConfig.Password = password
	}
	if timeout, ok := apr.GetInt(timeoutFlag); ok {
		serverConfig.Timeout = timeout
	}
	if _, ok := apr.GetValue(readonlyFlag); ok {
		serverConfig.ReadOnly = true
	}
	if logLevel, ok := apr.GetValue(logLevelFlag); ok {
		serverConfig.LogLevel = LogLevel(logLevel)
	}
	if multiDBDir, ok := apr.GetValue(multiDBDirFlag); ok {
		serverConfig.MultiDBDir = multiDBDir
	} else {
		if !cli.CheckEnvIsValid(dEnv) {
			return 2
		}
	}

	serverConfig.AutoCommit = !apr.Contains(noAutoCommitFlag)

	cli.PrintErrf("Starting server on port %d.", serverConfig.Port)

	if startError, closeError := Serve(ctx, serverConfig, serverController, dEnv); startError != nil || closeError != nil {
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
