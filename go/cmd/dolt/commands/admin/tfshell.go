// Copyright 2024 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

type TFShellCmd struct {
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd TFShellCmd) Name() string {
	return "tf-shell"
}

// Description returns a description of the command
func (cmd TFShellCmd) Description() string {
	return "Drops into a shell allowing the inspection of the table files in a dolt database"
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd TFShellCmd) RequiresRepo() bool {
	return true
}

func (cmd TFShellCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd TFShellCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	return ap
}

func (cmd TFShellCmd) Hidden() bool {
	return true
}

// Version displays the version of the running dolt client
// Exec executes the command
func (cmd TFShellCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cli.CommandDocumentationContent{}, ap))

	cli.ParseArgsOrDie(ap, args, usage)

	db := doltdb.HackDatasDatabaseFromDoltDB(dEnv.DoltDB)
	cs := datas.ChunkStoreFromDatabase(db)
	shell := nbs.NewShell(cs, func(chunkBytes []byte) string {
		v, err := types.DecodeValue(chunks.NewChunk(chunkBytes), dEnv.DoltDB.ValueReadWriter())
		if err != nil {
			panic(err)
		}
		return v.HumanReadableString()
	})
	shell.Run()
	return 0
}
