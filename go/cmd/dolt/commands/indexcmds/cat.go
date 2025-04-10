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
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

var catDocs = cli.CommandDocumentationContent{
	ShortDesc: `Display the contents of an index`,
	LongDesc: IndexCmdWarning + `
This command displays the contents of the given index. If indexes were tables, it would be equivalent to running "select * from {{.LessThan}}index{{.GreaterThan}}".`,
	Synopsis: []string{
		`[-r {{.LessThan}}result format{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}index{{.GreaterThan}}`,
	},
}

const formatFlag = "result-format"

type resultFormat byte

const (
	formatTabular resultFormat = iota
	formatCsv
	formatJson
)

type CatCmd struct {
	resultFormat resultFormat
}

func (cmd CatCmd) Name() string {
	return "cat"
}

func (cmd CatCmd) Description() string {
	return "Internal debugging command to display the contents of an index."
}

func (cmd CatCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd CatCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 2)
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table that the given index belongs to."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"index", "The name of the index that belongs to the given table."})
	ap.SupportsString(formatFlag, "r", "result format", "How to format the resulting output. Valid values are tabular, csv, json. Defaults to tabular.")
	return ap
}

func (cmd CatCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, catDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return 0
	} else if apr.NArg() != 2 {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Both the table and index names must be provided.").Build(), usage)
	}

	cmd.resultFormat = formatTabular
	if formatSr, ok := apr.GetValue(formatFlag); ok {
		switch strings.ToLower(formatSr) {
		case "tabular":
			cmd.resultFormat = formatTabular
		case "csv":
			cmd.resultFormat = formatCsv
		case "json":
			cmd.resultFormat = formatJson
		default:
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Invalid argument for --result-format. Valid values are tabular, csv, json").Build(), usage)
		}
	}

	working, err := dEnv.WorkingRoot(context.Background())
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get working.").AddCause(err).Build(), nil)
	}

	tableName := apr.Arg(0)
	indexName := apr.Arg(1)

	table, ok, err := working.GetTable(ctx, doltdb.TableName{Name: tableName})
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get table `%s`.", tableName).AddCause(err).Build(), nil)
	}
	if !ok {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("The table `%s` does not exist.", tableName).Build(), nil)
	}
	tblSch, err := table.GetSchema(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get schema for `%s`.", tableName).AddCause(err).Build(), nil)
	}
	index := tblSch.Indexes().GetByName(indexName)
	if index == nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("The index `%s` does not exist on table `%s`.", indexName, tableName).Build(), nil)
	}
	indexRowData, err := table.GetIndexRowData(ctx, index.Name())
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("The index `%s` does not have a data map.", indexName).Build(), nil)
	}

	err = cmd.prettyPrintResults(ctx, index.Schema(), indexRowData)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to display data for `%s`.", indexName).AddCause(err).Build(), nil)
	}

	return 0
}

func (cmd CatCmd) prettyPrintResults(ctx context.Context, doltSch schema.Schema, idx durable.Index) error {
	wr, err := getTableWriter(cmd.resultFormat, doltSch)
	if err != nil {
		return err
	}
	defer wr.Close(ctx)

	sqlCtx := sql.NewEmptyContext()

	rowItr, err := table.NewTableIterator(ctx, doltSch, idx)
	if err != nil {
		return err
	}
	defer rowItr.Close(sqlCtx)

	for {
		r, err := rowItr.Next(sqlCtx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = wr.WriteSqlRow(sqlCtx, r)
		if err != nil {
			return err
		}
	}

	return nil
}

func getTableWriter(format resultFormat, sch schema.Schema) (wr table.SqlRowWriter, err error) {
	s, err := sqlutil.FromDoltSchema("", "", sch)
	if err != nil {
		return nil, err
	}
	sqlSch := s.Schema
	cliWR := iohelp.NopWrCloser(cli.OutStream)

	switch format {
	case formatTabular:
		wr = tabular.NewFixedWidthTableWriter(sqlSch, cliWR, 100)
	case formatCsv:
		wr, err = csv.NewCSVWriter(cliWR, sch, csv.NewCSVInfo())
		if err != nil {
			return nil, err
		}
	case formatJson:
		wr, err = json.NewJSONWriter(cliWR, sch)
	default:
		panic("unhandled format")
	}

	return wr, nil
}
