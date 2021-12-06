// Copyright 2020 Dolthub, Inc.
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

package indexcmds

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var lsDocs = cli.CommandDocumentationContent{
	ShortDesc: `Display the list of indexes`,
	LongDesc: IndexCmdWarning + `
This command displays the list of all indexes. You may append a table name to show indexes for only that table, otherwise indexes for all tables are displayed for the working set.`,
	Synopsis: []string{
		`[{{.LessThan}}table{{.GreaterThan}}]`,
	},
}

type LsCmd struct{}

func (cmd LsCmd) Name() string {
	return "ls"
}

func (cmd LsCmd) Description() string {
	return "Internal debugging command to display the list of indexes."
}

func (cmd LsCmd) CreateMarkdown(_ io.Writer, _ string) error {
	return nil
}

func (cmd LsCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table to display indexes from. If one is not specified, then all tables' indexes are displayed."})
	return ap
}

func (cmd LsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, lsDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() > 1 {
		return HandleErr(errhand.BuildDError("Only one table may be provided at a time.").Build(), usage)
	}

	working, err := dEnv.WorkingRoot(context.Background())
	if err != nil {
		return HandleErr(errhand.BuildDError("Unable to get working.").AddCause(err).Build(), nil)
	}

	var tableNames []string
	if apr.NArg() == 0 {
		tableNames, err = working.GetTableNames(ctx)
		if err != nil {
			return HandleErr(errhand.BuildDError("Unable to get tables.").AddCause(err).Build(), nil)
		}
		sort.Strings(tableNames)
	} else if apr.NArg() == 1 {
		tableNames = []string{apr.Arg(0)}
	}

	if len(tableNames) == 0 {
		cli.Println("No tables in the working set.")
		return 0
	}

	var output []string
	for _, tableName := range tableNames {
		table, ok, err := working.GetTable(ctx, tableName)
		if err != nil {
			return HandleErr(errhand.BuildDError("Unable to get table `%s`.", tableName).AddCause(err).Build(), nil)
		}
		if !ok {
			return HandleErr(errhand.BuildDError("The given table `%s` does not exist.", tableName).Build(), nil)
		}
		sch, err := table.GetSchema(ctx)
		if err != nil {
			return HandleErr(errhand.BuildDError("Unable to get schema for `%s`.", tableName).AddCause(err).Build(), nil)
		}
		if sch.Indexes().Count() == 0 {
			if len(tableNames) == 1 {
				output = append(output, "No indexes on this table")
			}
			continue
		} else {
			if len(tableNames) > 1 {
				output = append(output, fmt.Sprintf("%s:", tableName))
			}
			for _, index := range sch.Indexes().AllIndexes() {
				output = append(output, fmt.Sprintf("    %s(%s)", index.Name(), strings.Join(index.ColumnNames(), ", ")))
			}
		}
	}

	if len(output) > 0 {
		cli.Println(strings.Join(output, "\n"))
	} else {
		cli.Println("No indexes in the working set")
	}

	return 0
}
