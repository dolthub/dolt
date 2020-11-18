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

package schcmds

import (
	"context"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var tblTagsDocs = cli.CommandDocumentationContent{
	ShortDesc: "Shows the column tags of one or more tables.",
	LongDesc: `{{.EmphasisLeft}}dolt schema tags{{.EmphasisRight}} displays the column tags of tables on the working set.

A list of tables can optionally be provided.  If it is omitted then all tables will be shown. If a given table does not exist, then it is ignored.`,
	Synopsis: []string{
		"[-r {{.LessThan}}result format{{.GreaterThan}}] [{{.LessThan}}table{{.GreaterThan}}...]",
	},
}

// WriterCloser type for the tabular writer
type StringBuilderCloser struct {
	strings.Builder
}

func (*StringBuilderCloser) Close() error {
	return nil
}
type TagsCmd struct{}

var _ cli.Command = TagsCmd{}

func (cmd TagsCmd) Name() string {
	return "tags"
}

func (cmd TagsCmd) Description() string {
	return "Shows the column tags of one or more tables."
}

func (cmd TagsCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, tblTagsDocs, ap))
}

func (cmd TagsCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "table(s) whose tags will be displayed."})
	ap.SupportsString(commands.FormatFlag, "r", "result output format", "How to format result output. Valid values are tabular, csv, json. Defaults to tabular.")
	return ap
}

func (cmd TagsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, tblTagsDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	tables := apr.Args()

	root, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	if len(tables) == 0 {
		var err error
		tables, err = root.GetTableNames(ctx)

		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("unable to get table names.").AddCause(err).Build(), usage)
		}

		tables = commands.RemoveDocsTbl(tables)
		if len(tables) == 0 {
			cli.Println("No tables in working set")
			return 0
		}
	}

	var headerSchema = sql.Schema{
		{Name: "table", Type: sql.Text, Default: nil},
		{Name: "column", Type: sql.Text, Default: nil},
		{Name: "tag", Type: sql.Uint64, Default: nil},
	}

	rows := make([]sql.Row, 0)

	for _, tableName := range tables {
		table, ok, err := root.GetTable(ctx, strings.ToLower(tableName)) // TODO: Handle case

		if err != nil {
			return 1
		}

		if ok {
			sch, err := table.GetSchema(ctx)

			if err != nil {
				return 1
			}

			_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
				currRow := sql.NewRow(strings.ToLower(tableName), col.Name, tag)

				rows = append(rows, currRow)

				return false, err
			})
		}
	}

	// Print the results in a SQL Tabular format.
	err := commands.PrettyPrintResults(ctx, 0, headerSchema, sql.RowsToRowIter(rows...))

	if err != nil {
		return 1
	}

	return 0
}
