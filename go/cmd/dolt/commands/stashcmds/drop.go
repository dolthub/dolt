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
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var stashDropDocs = cli.CommandDocumentationContent{
	ShortDesc: "Remove a single stash entry from the list of stash entries.",
	LongDesc:  ``,
	Synopsis: []string{
		"{{.LessThan}}stash{{.GreaterThan}}",
	},
}

type StashDropCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StashDropCmd) Name() string {
	return "drop"
}

// Description returns a description of the command
func (cmd StashDropCmd) Description() string {
	return "Remove a single stash entry from the list of stash entries."
}

func (cmd StashDropCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(stashDropDocs, ap)
}

func (cmd StashDropCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// EventType returns the type of the event to log
func (cmd StashDropCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_STASH_DROP
}

// Exec executes the command
func (cmd StashDropCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, stashDropDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	if dEnv.IsLocked() {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(env.ErrActiveServerLock.New(dEnv.LockFile())), help)
	}

	if apr.NArg() != 1 {
		usage()
		return 1
	}

	stashName := apr.Args[0]
	stashName = strings.TrimSuffix(strings.TrimPrefix(stashName, "stash@{"), "}")
	idx, err := strconv.Atoi(stashName)
	if err != nil {
		commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	err = dEnv.DoltDB.RemoveStashAtIdx(ctx, idx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	return 0
}
