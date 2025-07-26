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

package credcmds

import (
	"context"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

var rmDocs = cli.CommandDocumentationContent{
	ShortDesc: "Remove a stored public/private keypair.",
	LongDesc:  `Removes an existing keypair from dolt's credential storage.`,
	Synopsis:  []string{"{{.LessThan}}public_key_as_appears_in_ls{{.GreaterThan}}"},
}

type RmCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd RmCmd) Name() string {
	return "rm"
}

// Description returns a description of the command
func (cmd RmCmd) Description() string {
	return rmDocs.ShortDesc
}

func (cmd RmCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(rmDocs, ap)
}

func (cmd RmCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(cmd.Name())
	return ap
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd RmCmd) RequiresRepo() bool {
	return false
}

// EventType returns the type of the event to log
func (cmd RmCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CREDS_RM
}

// Exec executes the command
func (cmd RmCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, rmDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	args = apr.Args

	credsDir, verr := actions.EnsureCredsDir(dEnv)

	if verr == nil {
		for _, arg := range args {
			jwkFilePath, err := dEnv.FindCreds(credsDir, arg)

			if err == nil {
				err = dEnv.FS.DeleteFile(jwkFilePath)
			}

			if err != nil {
				cli.Println(color.YellowString("failed to delete %s: %s", arg, err.Error()))
			}
		}
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}
