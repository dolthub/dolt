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

package ci

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"

	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/dolt_ci"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var destroyDocs = cli.CommandDocumentationContent{
	ShortDesc: "Drops all database tables used to store continuous integration configuration",
	LongDesc:  "Drops all database tables used to store continuous integration configuration and creates a Dolt commit",
	Synopsis: []string{
		"",
	},
}

type DestroyCmd struct{}

// Name implements cli.Command.
func (cmd DestroyCmd) Name() string {
	return "destroy"
}

// Description implements cli.Command.
func (cmd DestroyCmd) Description() string {
	return destroyDocs.ShortDesc
}

// RequiresRepo implements cli.Command.
func (cmd DestroyCmd) RequiresRepo() bool {
	return true
}

// Docs implements cli.Command.
func (cmd DestroyCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(destroyDocs, ap)
}

// Hidden should return true if this command should be hidden from the help text
func (cmd DestroyCmd) Hidden() bool {
	return false
}

// ArgParser implements cli.Command.
func (cmd DestroyCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	return ap
}

// Exec implements cli.Command.
func (cmd DestroyCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, destroyDocs, ap))
	cli.ParseArgsOrDie(ap, args, help)

	if !cli.CheckEnvIsValid(dEnv) {
		return 1
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()

		_, ok := queryist.(*engine.SqlEngine)
		if !ok {
			msg := fmt.Sprintf(cli.RemoteUnsupportedMsg, commandStr)
			cli.Println(msg)
			return 1
		}
	}

	db, err := newDatabase(sqlCtx, sqlCtx.GetCurrentDatabase(), dEnv, false)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	name, email, err := env.GetNameAndEmail(dEnv.Config)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	var verr errhand.VerboseError
	err = dolt_ci.DestroyDoltCITables(sqlCtx, db, queryist.Query, name, email)
	if err != nil {
		verr = errhand.VerboseErrorFromError(err)
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}
