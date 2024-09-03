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

package commands

import (
	"bytes"
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var addDocs = cli.CommandDocumentationContent{
	ShortDesc: `Add table contents to the list of staged tables`,
	LongDesc: `
This command updates the list of tables using the current content found in the working root, to prepare the content staged for the next commit. It adds the current content of existing tables as a whole or remove tables that do not exist in the working root anymore.

This command can be performed multiple times before a commit. It only adds the content of the specified table(s) at the time the add command is run; if you want subsequent changes included in the next commit, then you must run dolt add again to add the new content to the index.

The dolt status command can be used to obtain a summary of which tables have changes that are staged for the next commit.`,
	Synopsis: []string{
		`[{{.LessThan}}table{{.GreaterThan}}...]`,
	},
}

type AddCmd struct{}

var _ cli.RepoNotRequiredCommand = AddCmd{}

func (cmd AddCmd) RequiresRepo() bool {
	return false
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd AddCmd) Name() string {
	return "add"
}

// Description returns a description of the command
func (cmd AddCmd) Description() string {
	return "Add table changes to the list of staged table changes."
}

func (cmd AddCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateAddArgParser()
	return cli.NewCommandDocumentation(addDocs, ap)
}

func (cmd AddCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateAddArgParser()
}

// generateAddSql returns the query that will call the `DOLT_ADD` stored proceudre.
// This function assumes that the inputs are validated table names, which cannot contain quotes.
func generateAddSql(apr *argparser.ArgParseResults) string {
	var buffer bytes.Buffer
	var first bool
	first = true
	buffer.WriteString("CALL DOLT_ADD(")

	write := func(s string) {
		if !first {
			buffer.WriteString(", ")
		}
		buffer.WriteString("'")
		buffer.WriteString(s)
		buffer.WriteString("'")
		first = false
	}

	if apr.Contains(cli.AllFlag) {
		write("-A")
	}
	if apr.Contains(cli.ForceFlag) {
		write("-f")
	}
	for _, arg := range apr.Args {
		write(arg)
	}
	buffer.WriteString(")")
	return buffer.String()
}

// Exec executes the command
func (cmd AddCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateAddArgParser()
	apr, _, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, addDocs)
	if terminate {
		return status
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	// Allow staging tables with merge conflicts.
	_, _, _, err = queryist.Query(sqlCtx, "set @@dolt_force_transaction_commit=1;")
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}

	if apr.Contains("patch") {
		return patchWorkflow(sqlCtx, queryist, args, cliCtx)
	} else {
		for _, tableName := range apr.Args {
			if tableName != "." && !doltdb.IsValidTableName(tableName) {
				return HandleVErrAndExitCode(errhand.BuildDError("'%s' is not a valid table name", tableName).Build(), nil)
			}
		}

		_, rowIter, _, err := queryist.Query(sqlCtx, generateAddSql(apr))
		if err != nil {
			cli.PrintErrln(errhand.VerboseErrorFromError(err))
			return 1
		}

		_, err = sql.RowIterToRows(sqlCtx, rowIter)
		if err != nil {
			cli.PrintErrln(errhand.VerboseErrorFromError(err))
			return 1
		}
	}

	return 0
}

}
