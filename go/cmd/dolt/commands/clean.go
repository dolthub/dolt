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

package commands

import (
	"bytes"
	"context"

	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const (
	DryrunCleanParam = "dry-run"
)

var cleanDocContent = cli.CommandDocumentationContent{
	ShortDesc: "Deletes untracked working tables",
	LongDesc: "{{.EmphasisLeft}}dolt clean [--dry-run]{{.EmphasisRight}}\n\n" +
		"The default (parameterless) form clears the values for all untracked working {{.LessThan}}tables{{.GreaterThan}} ." +
		"This command permanently deletes unstaged or uncommitted tables.\n\n" +
		"The {{.EmphasisLeft}}--dry-run{{.EmphasisRight}} flag can be used to test whether the clean can succeed without " +
		"deleting any tables from the current working set.\n\n" +
		"{{.EmphasisLeft}}dolt clean [--dry-run] {{.LessThan}}tables{{.GreaterThan}}...{{.EmphasisRight}}\n\n" +
		"If {{.LessThan}}tables{{.GreaterThan}} is specified, only those table names are considered for deleting.\n\n",
	Synopsis: []string{
		"[--dry-run]",
		"[--dry-run] {{.LessThan}}tables{{.GreaterThan}}...",
	},
}

type CleanCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CleanCmd) Name() string {
	return "clean"
}

// Description returns a description of the command
func (cmd CleanCmd) Description() string {
	return "Remove untracked tables from working set."
}

func (cmd CleanCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateCleanArgParser()
	return cli.NewCommandDocumentation(cleanDocContent, ap)
}

func (cmd CleanCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCleanArgParser()
}

func (cmd CleanCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd CleanCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateCleanArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cleanDocContent, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}

	var params []interface{}

	firstParamDone := false
	var buffer bytes.Buffer
	buffer.WriteString("CALL DOLT_CLEAN(")
	if apr.Contains(cli.DryRunFlag) {
		buffer.WriteString("\"--dry-run\"")
		firstParamDone = true
	}
	if apr.NArg() > 0 {
		// loop over apr.Args() and add them to the buffer
		for i := 0; i < apr.NArg(); i++ {
			if firstParamDone {
				buffer.WriteString(", ")
			}
			buffer.WriteString("?")
			params = append(params, apr.Arg(i))
			firstParamDone = true
		}
	}
	buffer.WriteString(")")
	query := buffer.String()

	if len(params) > 0 {
		query, err = dbr.InterpolateForDialect(query, params, dialect.MySQL)
		if err != nil {
			cli.Println(err.Error())
			return 1
		}
	}

	_, err = cli.GetRowsForSql(queryist.Queryist, queryist.Context, query)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}

	return 0
}
