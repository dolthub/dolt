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
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var stashListDocs = cli.CommandDocumentationContent{
	ShortDesc: "List the stash entries that you currently have.",
	LongDesc: `Each stash entry is listed with its name (e.g. stash@{0} is the latest entry, stash@{1} is the one before, etc.), the name of the branch that was current when the entry was made, and a short description of the commit the entry was based on.
`,
	Synopsis: []string{
		"",
	},
}

type StashListCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StashListCmd) Name() string {
	return "list"
}

// Description returns a description of the command
func (cmd StashListCmd) Description() string {
	return "List the stash entries that you currently have."
}

func (cmd StashListCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(stashListDocs, ap)
}

func (cmd StashListCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	return ap
}

// EventType returns the type of the event to log
func (cmd StashListCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_STASH_LIST
}

// Exec executes the command
func (cmd StashListCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	if !dEnv.DoltDB(ctx).Format().UsesFlatbuffers() {
		cli.PrintErrln(ErrStashNotSupportedForOldFormat.Error())
		return 1
	}
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, stashListDocs, ap))
	cli.ParseArgsOrDie(ap, args, help)

	err := listStashes(ctx, dEnv)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	return 0
}

func listStashes(ctx context.Context, dEnv *env.DoltEnv) error {
	stashes, err := dEnv.DoltDB(ctx).GetCommandLineStashes(ctx)
	if err != nil {
		return err
	}

	for _, stash := range stashes {
		commitHash, err := stash.HeadCommit.HashOf()
		if err != nil {
			return err
		}
		cli.Println(fmt.Sprintf("%s: WIP on %s: %s %s", stash.Name, stash.BranchReference, commitHash.String(), stash.Description))
	}
	return nil
}
