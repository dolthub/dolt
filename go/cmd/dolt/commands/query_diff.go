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
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
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

func (q QueryDiff) compareRows(pkOrds []int, row1, row2 sql.Row) (int, bool) {
	var cmp int
	for _, pkOrd := range pkOrds {
		pk1, _ := gmstypes.ConvertToString(row1[pkOrd], gmstypes.Text)
		pk2, _ := gmstypes.ConvertToString(row2[pkOrd], gmstypes.Text)
		if pk1 < pk2 {
			cmp = -1
		} else if pk1 > pk2 {
			cmp = 1
		} else {
			cmp = 0
		}
	}
	var diff bool
	for i := 0; i < len(row1); i++ {
		a, _ := gmstypes.ConvertToString(row1[i], gmstypes.Text)
		b, _ := gmstypes.ConvertToString(row2[i], gmstypes.Text)
		if a != b {
			diff = true
			break
		}
	}
	return cmp, diff
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

	if apr.NArg() != 2 {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(fmt.Errorf("please provide exactly two queries")), usage)
	}

	query1 := apr.Arg(0)
	query2 := apr.Arg(1)

	schema1, rowIter1, err := queryist.Query(sqlCtx, query1)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	schema2, rowIter2, err := queryist.Query(sqlCtx, query2)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if schema1.Equals(schema2) {
		var pkOrds []int
		for ord, col := range schema1 {
			if col.PrimaryKey {
				pkOrds = append(pkOrds, ord)
			}
		}

		// TODO: assume both are sorted according to their primary keys
		var results []sql.Row
		row1, err1 := rowIter1.Next(sqlCtx)
		row2, err2 := rowIter2.Next(sqlCtx)
		for err1 == nil && err2 == nil {
			cmp, diff := q.compareRows(pkOrds, row1, row2)
			switch cmp {
			case -1: // deleted
				results = append(results, row1)
				row1, err1 = rowIter1.Next(sqlCtx)
			case 1: // added
				results = append(results, row2)
				row2, err2 = rowIter2.Next(sqlCtx)
			default: // modified or no change
				if diff {
					results = append(results, row1)
					results = append(results, row2)
				}
				row1, err1 = rowIter1.Next(sqlCtx)
				row2, err2 = rowIter2.Next(sqlCtx)
			}
		}
		// Append any remaining rows
		var rowIter sql.RowIter
		if err1 == io.EOF && err2 == io.EOF {
			rowIter1.Close(sqlCtx)
			rowIter2.Close(sqlCtx)
		} else if err1 == io.EOF {
			results = append(results, row2)
			rowIter = rowIter2
			rowIter1.Close(sqlCtx)
		} else if err2 == io.EOF  {
			results = append(results, row1)
			rowIter = rowIter1
			rowIter2.Close(sqlCtx)
		} else {
			if err1 != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err1), usage)
			} else {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err2), usage)
			}
		}
		if rowIter != nil {
			for {
				row, err := rowIter.Next(sqlCtx)
				if err == io.EOF {
					break
				}
				results = append(results, row)
			}
		}

		cliWR := iohelp.NopWrCloser(cli.OutStream)
		wr := tabular.NewFixedWidthTableWriter(append(schema1), cliWR, 100)
		defer wr.Close(ctx)
		for _, row := range results {
			wr.WriteSqlRow(ctx, row)
		}
	} else {
		cliWR := iohelp.NopWrCloser(cli.OutStream)
		wr := tabular.NewFixedWidthTableWriter(append(schema1, schema2...), cliWR, 100)
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
	}

	return 0
}


func (q QueryDiff) validateArgs(apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("not enough args").Build()
	}
	return nil
}