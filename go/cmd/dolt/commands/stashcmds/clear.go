// Copyright 2023 Dolthub, Inc.
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

package stashcmds

import (
	"context"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var stashClearDocs = cli.CommandDocumentationContent{
	ShortDesc: "Remove all the stash entries.",
	LongDesc: `Removes all the stash entries from the current stash list. This command cannot be reverted and stash entries may not be recoverable.

This command does not apply the stash on current working directory, use 'dolt stash pop' to apply a stash on current working directory.`,
	Synopsis: []string{
		"",
	},
}

type StashClearCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StashClearCmd) Name() string {
	return "clear"
}

// Description returns a description of the command
func (cmd StashClearCmd) Description() string {
	return "Remove all the stash entries."
}

func (cmd StashClearCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(stashClearDocs, ap)
}

func (cmd StashClearCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	return ap
}

// EventType returns the type of the event to log
func (cmd StashClearCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_STASH_CLEAR
}

// Exec executes the command
func (cmd StashClearCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	if !dEnv.DoltDB(ctx).Format().UsesFlatbuffers() {
		cli.PrintErrln(ErrStashNotSupportedForOldFormat.Error())
		return 1
	}
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, stashClearDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() != 0 {
		usage()
		return 1
	}

	err := dEnv.DoltDB(ctx).RemoveAllStashes(ctx, DoltCliRef)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	return 0
}
