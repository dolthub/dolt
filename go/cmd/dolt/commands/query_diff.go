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
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

var queryDiffDocs = cli.CommandDocumentationContent{
	ShortDesc: "Calculates table diff between two queries",
	LongDesc: "Will execute two queries and compare the resulting table sets\n\n" +
		"`<query1>`: A SQL `SELECT` query to be executed.\n\n" +
		"`<query2>`: A SQL `SELECT` query to be executed.\n\n" +
		"**Note**\n\n" +
		"Query diff is performed brute force and thus, will be slow for large result sets.\n" +
		"The algorithm is super linear (`n^2`) on the size of the results sets.\n" +
		"Over time, we will optimize this to use features of the storage engine to improve performance.",
	Synopsis: []string{
		`[options] [{{.LessThan}}query1{{.GreaterThan}}] [{{.LessThan}}query2{{.GreaterThan}}]`,
	},
}

type QueryDiff struct{}

var _ cli.Command = QueryDiff{}

func (q QueryDiff) Name() string {
	return "query-diff"
}

func (q QueryDiff) Description() string {
	return "Shows table diff between two queries."
}

func (q QueryDiff) Docs() *cli.CommandDocumentation {
	ap := q.ArgParser()
	return cli.NewCommandDocumentation(queryDiffDocs, ap)
}

func (q QueryDiff) ArgParser() *argparser.ArgParser {
	return argparser.NewArgParserWithVariableArgs(q.Name())
}

func (q QueryDiff) compareRows(pkOrds []int, row1, row2 sql.Row) (int, bool) {
	var cmp int
	for _, pkOrd := range pkOrds {
		pk1, _ := gmstypes.ConvertToString(row1.GetValue(pkOrd), gmstypes.Text, nil)
		pk2, _ := gmstypes.ConvertToString(row2.GetValue(pkOrd), gmstypes.Text, nil)
		if pk1 < pk2 {
			cmp = -1
		} else if pk1 > pk2 {
			cmp = 1
		} else {
			cmp = 0
		}
	}
	var diff bool
	for i := 0; i < row1.Len(); i++ {
		a, _ := gmstypes.ConvertToString(row1.GetValue(i), gmstypes.Text, nil)
		b, _ := gmstypes.ConvertToString(row2.GetValue(i), gmstypes.Text, nil)
		if a != b {
			diff = true
			break
		}
	}
	return cmp, diff
}

func (q QueryDiff) validateArgs(apr *argparser.ArgParseResults) error {
	if apr.NArg() != 2 {
		return fmt.Errorf("please provide exactly two queries")
	}
	return nil
}

func (q QueryDiff) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := q.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, queryDiffDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	if err := q.validateArgs(apr); err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	// TODO: prevent create, insert, update, delete, etc. queries
	query1, query2 := apr.Arg(0), apr.Arg(1)
	query, err := dbr.InterpolateForDialect("select * from dolt_query_diff(?, ?)", []interface{}{query1, query2}, dialect.MySQL)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	schema, rowIter, _, err := queryist.Query(sqlCtx, query)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	cliWR := iohelp.NopWrCloser(cli.OutStream)
	wr := tabular.NewFixedWidthTableWriter(schema, cliWR, 100)
	defer wr.Close(ctx)

	for {
		row, rerr := rowIter.Next(sqlCtx)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(rerr), usage)
		}
		if werr := wr.WriteSqlRow(ctx, row); werr != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(werr), usage)
		}
	}

	return 0
}
