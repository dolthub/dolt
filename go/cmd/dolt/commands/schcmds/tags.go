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
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
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

	// Define a schema and table writer for output
	var inCols = []schema.Column{
		{Name: "table", Tag: 0, Kind: types.StringKind, IsPartOfPK: false, Constraints: nil},
		{Name: "column", Tag: 1, Kind: types.StringKind, IsPartOfPK: false, Constraints: nil},
		{Name: "tag", Tag: 2, Kind: types.StringKind, IsPartOfPK: false, Constraints: nil},
	}
	colColl, _ := schema.NewColCollection(inCols...)
	rowSch := schema.UnkeyedSchemaFromCols(colColl)

	_, outSch := untyped.NewUntypedSchema("table", "column", "tag")
	var stringWr StringBuilderCloser
	tableWr, err := tabular.NewTextTableWriter(&stringWr, outSch)

	if err != nil {
		return 1
	}

	// Write the header row
	header, err := row.New(types.Format_LD_1, rowSch, row.TaggedValues{
		0: types.String("table"),
		1: types.String("column"),
		2: types.String("tag"),
	})

	err = tableWr.WriteRow(ctx, header)

	for _, tableName := range tables {
		table, ok, err := root.GetTable(ctx, tableName) // TODO: Handle case

		if !ok {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("unable to find table given in args.").AddCause(err).Build(), usage)
		}

		sch, err := table.GetSchema(ctx)

		if err != nil {
			return 1
		}

		_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			currRow, err := row.New(types.Format_7_18, rowSch, row.TaggedValues{
				0: types.String(tableName),
				1: types.String(col.Name),
				2: types.String(strconv.FormatUint(tag, 10)),
			})

			err = tableWr.WriteRow(ctx, currRow)

			return false, err
		})
	}

	err = tableWr.Close(ctx)

	if err != nil {
		return 1
	}

	cli.Printf(stringWr.String())

	return 0
}
