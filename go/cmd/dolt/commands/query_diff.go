// Copyright 2023 Dolthub, Inc.
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
	"context"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
)

var queryDiffDocs = cli.CommandDocumentationContent{
	ShortDesc: "Show chances between two queries",
	LongDesc: "Show chances between two queries",
	Synopsis: []string{
		`[options] [{{.LessThan}}query1{{.GreaterThan}}] [{{.LessThan}}query2{{.GreaterThan}}...]`,
	},
}

type QueryDiff struct{}
var _ cli.Command = QueryDiff{}

func (q QueryDiff) Name() string {
	return "query-diff"
}

func (q QueryDiff) Description() string {
	return "description"
}

func (q QueryDiff) Docs() *cli.CommandDocumentation {
	ap := q.ArgParser()
	return cli.NewCommandDocumentation(queryDiffDocs, ap)
}

func (q QueryDiff) ArgParser() *argparser.ArgParser {
	return argparser.NewArgParserWithVariableArgs(q.Name())
}

func (q QueryDiff) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := q.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, queryDiffDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	if apr == nil {}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	query1 := apr.Arg(0)
	query2 := apr.Arg(1)

	schema1, rowIter1, err := queryist.Query(sqlCtx, query1)
	schema2, rowIter2, err := queryist.Query(sqlCtx, query2)

	if !schema1.Equals(schema2) {}

	cliWR := iohelp.NopWrCloser(cli.OutStream)
	wr := tabular.NewFixedWidthTableWriter(diffSummarySchema, cliWR, 100)
	defer wr.Close(ctx)

	var err1, err2 error
	var row1, row2 sql.Row
	for {
		row1, err1 = rowIter1.Next(sqlCtx)
		if err1 == io.EOF {
			break
		}
		_, rowIter2, _ = queryist.Query(sqlCtx, query2)
		for {
			row2, err2 = rowIter2.Next(sqlCtx)
			if err2 == io.EOF {
				break
			}
			wr.WriteSqlRow(ctx, append(row1, row2...))
		}
	}

	return 0
}


func (q QueryDiff) validateArgs(apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("not enough args").Build()
	}
	return nil
}