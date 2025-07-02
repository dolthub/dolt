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

package commands

import (
	"bytes"
	"context"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"
)

var rmDocs = cli.CommandDocumentationContent{
	ShortDesc: `Drops a table and removes it from tracking`,
	LongDesc: `
In it's default mode, this command drops a table and removes it from tracking. Without '--cached', you can only call rm on committed tables.

The option '--cached' can be used to untrack tables, but leave them in the working set. You can restage them with 'dolt add'.

The dolt status command can be used to obtain a summary of which tables have changes that are staged for the next commit.'`,
	Synopsis: []string{
		`[{{.LessThan}}table{{.GreaterThan}}...]`,
	},
}

type RmCmd struct{}

/*var _ cli.RepoNotRequiredCommand = RmCmd{}

func (cmd RmCmd) RequiresRepo() bool {
	return false
}*/ //TODO DO WE NEED THIS

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd RmCmd) Name() string {
	return "rm"
}

// Description returns a description of the command
func (cmd RmCmd) Description() string {
	return "Drops a table and removes it from tracking"
}

func (cmd RmCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(rmDocs, ap)
}

func (cmd RmCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateRmArgParser()
}

func (cmd RmCmd) Exec(ctx context.Context, commandStr string, args []string, _ *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	apr, _, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, rmDocs)
	if terminate {
		return status
	}

	errorBuilder := errhand.BuildDError("error: failed to create query engine")
	queryEngine, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errorBuilder.AddCause(err).Build(), nil)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	interpolatedQuery, err := generateRmSql(args)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), nil)
	}

	_, rowIter, _, err := queryEngine.Query(sqlCtx, interpolatedQuery)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), nil)
	}

	_, err = sql.RowIterToRows(sqlCtx, rowIter)
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}

	for _, arg := range apr.Args {
		cli.Printf("rm '%s'\n", arg)
	}
	return 0
}

func generateRmSql(args []string) (string, error) {
	var buffer bytes.Buffer
	var first bool
	queryValues := make([]interface{}, 0, len(args))
	first = true
	buffer.WriteString("CALL DOLT_RM(")

	for _, arg := range args {
		if !first {
			buffer.WriteString(", ")
		}
		first = false
		buffer.WriteString("?")
		queryValues = append(queryValues, arg)
	}
	buffer.WriteString(")")

	return dbr.InterpolateForDialect(buffer.String(), queryValues, dialect.MySQL)

}
