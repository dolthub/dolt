// Copyright 2025 Dolthub, Inc.
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

package admin

import (
	"context"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
)

type ConjoinCmd struct {
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ConjoinCmd) Name() string {
	return "conjoin"
}

// Description returns a description of the command
func (cmd ConjoinCmd) Description() string {
	return "Conjoin storage files in the database"
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd ConjoinCmd) RequiresRepo() bool {
	return true
}

func (cmd ConjoinCmd) Docs() *cli.CommandDocumentation {
	return &cli.CommandDocumentation{
		CommandStr: "conjoin",
		ShortDesc:  "Conjoin storage files in the database",
		LongDesc:   `Admin command to conjoin storage files in the database. Use --all to conjoin all storage files, or specify individual storage file IDs.`,
		Synopsis: []string{
			"conjoin --all",
			"conjoin <storage_file_id>...",
		},
		ArgParser: cmd.ArgParser(),
	}
}

func (cmd ConjoinCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(cmd.Name())
	ap.SupportsFlag(cli.AllFlag, "", "Conjoin all storage files")
	return ap
}

func (cmd ConjoinCmd) Hidden() bool {
	return true
}

// Exec executes the command
func (cmd ConjoinCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, _ cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cli.CommandDocumentationContent{}, ap))

	apr := cli.ParseArgsOrDie(ap, args, usage)

	allFlag := apr.Contains(cli.AllFlag)
	storageIds := apr.Args

	// Validate that either --all flag is used OR storage file IDs are provided, but not both
	if allFlag && len(storageIds) > 0 {
		verr := errhand.BuildDError("--all flag and storage file IDs are mutually exclusive").SetPrintUsage().Build()
		commands.HandleVErrAndExitCode(verr, usage)
	}

	if !allFlag && len(storageIds) == 0 {
		verr := errhand.BuildDError("must specify either --all flag or storage file IDs").SetPrintUsage().Build()
		commands.HandleVErrAndExitCode(verr, usage)
	}

	// Validate storage file IDs and convert to hash.Hash instances
	var storageIdHashes []hash.Hash
	if len(storageIds) > 0 {
		for _, id := range storageIds {
			fileIdHash, ok := hash.MaybeParse(id)
			if !ok {
				verr := errhand.BuildDError("invalid storage file ID: %s", id).SetPrintUsage().Build()
				commands.HandleVErrAndExitCode(verr, usage)
			}
			storageIdHashes = append(storageIdHashes, fileIdHash)
		}
	}

	verr := errhand.BuildDError("conjoin command not yet implemented").Build()
	commands.HandleVErrAndExitCode(verr, usage)

	return 0
}
