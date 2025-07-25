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
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var tblTagsDocs = cli.CommandDocumentationContent{
	ShortDesc: "Shows the column tags of one or more tables.",
	LongDesc: `{{.EmphasisLeft}}dolt schema tags{{.EmphasisRight}} displays the column tags of tables on the working set.

A list of tables can optionally be provided.  If it is omitted then all tables will be shown. If a given table does not exist, then it is ignored.`,
	Synopsis: []string{
		"[-r {{.LessThan}}result format{{.GreaterThan}}] [{{.LessThan}}table{{.GreaterThan}}...]",
	},
}

type TagsCmd struct{}

var _ cli.Command = TagsCmd{}

func (cmd TagsCmd) Name() string {
	return "tags"
}

func (cmd TagsCmd) Description() string {
	return "Shows the column tags of one or more tables."
}

func (cmd TagsCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(tblTagsDocs, ap)
}

func (cmd TagsCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(cmd.Name())
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "table(s) whose tags will be displayed."})
	ap.SupportsString(commands.FormatFlag, "r", "result output format", "How to format result output. Valid values are tabular, csv, json. Defaults to tabular.")
	return ap
}

func (cmd TagsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, tblTagsDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	tables := apr.Args

	root, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	if len(tables) == 0 {
		var err error
		tables, err = root.GetTableNames(ctx, doltdb.DefaultSchemaName)

		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("unable to get table names.").AddCause(err).Build(), usage)
		}

		tables = actions.RemoveDocsTable(tables)
		if len(tables) == 0 {
			cli.Println("No tables in working set")
			return 0
		}
	}

	var headerSchema = sql.Schema{
		{Name: "table", Type: types.Text, Default: nil},
		{Name: "column", Type: types.Text, Default: nil},
		{Name: "tag", Type: types.Uint64, Default: nil},
	}

	rows := make([]sql.Row, 0)

	for _, tableName := range tables {
		table, foundTableKey, ok, err := doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: tableName})

		// Return an error if table is not found
		if !ok {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Can't find table %s.", tableName).AddCause(err).Build(), usage)
		}

		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Could not load table %s.", tableName).AddCause(err).Build(), usage)
		}

		sch, err := table.GetSchema(ctx)

		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Could not load %s schema.", tableName).AddCause(err).Build(), usage)
		}

		_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			rows = append(rows, sql.NewRow(foundTableKey, col.Name, tag))

			return false, err
		})

	}

	outputFmt := engine.FormatTabular

	formatSr, ok := apr.GetValue(commands.FormatFlag)

	var err error
	if ok {
		outputFmt, verr = commands.GetResultFormat(formatSr)

		if verr != nil {
			return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(verr), usage)
		}
	}

	sqlCtx := sql.NewContext(ctx)
	err = engine.PrettyPrintResults(sqlCtx, outputFmt, headerSchema, sql.RowsToRowIter(rows...), false, false, false, false)

	return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
}
