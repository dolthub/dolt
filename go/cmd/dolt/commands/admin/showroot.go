// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/store/hash"
)

type ShowRootCmd struct {
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ShowRootCmd) Name() string {
	return "show-root"
}

// Description returns a description of the command
func (cmd ShowRootCmd) Description() string {
	return "Prints every entry in the root map of the database"
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd ShowRootCmd) RequiresRepo() bool {
	return true
}

func (cmd ShowRootCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd ShowRootCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	return ap
}

func (cmd ShowRootCmd) Hidden() bool {
	return true
}

// Version displays the version of the running dolt client
// Exec executes the command
func (cmd ShowRootCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cli.CommandDocumentationContent{}, ap))

	cli.ParseArgsOrDie(ap, args, usage)

	db := doltdb.HackDatasDatabaseFromDoltDB(dEnv.DoltDB(ctx))
	dss, err := db.Datasets(ctx)
	if err != nil {
		verr := errhand.BuildDError("failed to get database datasets").AddCause(err).Build()
		commands.HandleVErrAndExitCode(verr, func() {})
	}
	err = dss.IterAll(ctx, func(key string, addr hash.Hash) error {
		cli.Printf("%-60s%60s\n", key, addr.String())
		return nil
	})
	if err != nil {
		verr := errhand.BuildDError("failed to iterate all ref entries").AddCause(err).Build()
		commands.HandleVErrAndExitCode(verr, func() {})
	}
	return 0
}
