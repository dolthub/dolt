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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

type ConjoinCmd struct {
}

var conjoinDocs = cli.CommandDocumentationContent{
	ShortDesc: "Conjoin storage files in the database",
	LongDesc: `Admin command to conjoin oldgen storage files in the database. Conjoining combines multiple storage files into a single file, which can improve chunk search time.

Use --all to conjoin all storage in oldgen, or specify individual storage file IDs (32-character hex strings) to conjoin only those files.`,
	Synopsis: []string{
		"--all",
		"<storage_file_id>...",
	},
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
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(conjoinDocs, ap)
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
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, conjoinDocs, ap))

	apr := cli.ParseArgsOrDie(ap, args, usage)

	allFlag := apr.Contains(cli.AllFlag)
	storageIds := apr.Args

	// Validate that either --all flag is used OR storage file IDs are provided, but not both
	if allFlag && len(storageIds) > 0 {
		verr := errhand.BuildDError("--all flag and storage file IDs are mutually exclusive").SetPrintUsage().Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	if !allFlag && len(storageIds) == 0 {
		verr := errhand.BuildDError("must specify either --all flag or storage file IDs").SetPrintUsage().Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	// Validate storage file IDs and convert to hash.Hash instances
	var storageIdHashes []hash.Hash
	if len(storageIds) > 0 {
		for _, id := range storageIds {
			fileIdHash, ok := hash.MaybeParse(id)
			if !ok {
				verr := errhand.BuildDError("invalid storage file ID: %s", id).SetPrintUsage().Build()
				return commands.HandleVErrAndExitCode(verr, usage)
			}
			storageIdHashes = append(storageIdHashes, fileIdHash)
		}
	}

	// Get the ChunkStore from DoltDB
	ddb := dEnv.DoltDB(ctx)
	db := doltdb.HackDatasDatabaseFromDoltDB(ddb)
	cs := datas.ChunkStoreFromDatabase(db)

	var targetNBS *nbs.NomsBlockStore
	if gnbs, ok := cs.(*nbs.GenerationalNBS); ok {
		targetNBS, ok = gnbs.OldGen().(*nbs.NomsBlockStore)
		if !ok {
			verr := errhand.BuildDError("ChunkStore is not a NomsBlockStore").Build()
			return commands.HandleVErrAndExitCode(verr, usage)
		}
	} else {
		verr := errhand.BuildDError("ChunkStore is not a supported type for conjoin operation").Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	var targetStorageIds []hash.Hash
	if allFlag {
		targetStorageIds = nil // Empty slice will trigger "conjoin all" behavior
	} else {
		targetStorageIds = storageIdHashes
	}

	conjoinedHash, err := targetNBS.ConjoinTableFiles(ctx, targetStorageIds)
	if err != nil {
		if err.Error() == "no table files to conjoin" {
			cli.Printf("No table files to conjoin.\n")
			return 0
		}
		verr := errhand.BuildDError("failed to conjoin table files: %v", err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	if allFlag {
		cli.Printf("Successfully conjoined all table files. New table file: %s\n", conjoinedHash.String())
	} else {
		cli.Printf("Successfully conjoined table files. New table file: %s\n", conjoinedHash.String())
	}
	return 0
}
