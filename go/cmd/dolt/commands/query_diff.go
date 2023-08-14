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
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
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

	defer rowIter1.Close(sqlCtx)
	defer rowIter2.Close(sqlCtx)

	if schema1.Equals(schema2) {
		var pkOrds []int
		for ord, col := range schema1 {
			if col.PrimaryKey {
				pkOrds = append(pkOrds, ord)
			}
		}

		dw, err := newDiffWriter(TabularDiffOutput)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		err = dw.BeginTable(query1, query2, false, false)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		wr, err := dw.RowWriter(nil, nil, diff.TableDeltaSummary{}, schema1)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		defer wr.Close(ctx)

		// TODO: assume both are sorted according to their primary keys
		row1, err1 := rowIter1.Next(sqlCtx)
		row2, err2 := rowIter2.Next(sqlCtx)
		removedChange := make([]diff.ChangeType, len(schema1))
		for i := range removedChange {
			removedChange[i] = diff.Removed
		}
		addedChange := make([]diff.ChangeType, len(schema1))
		for i := range addedChange {
			addedChange[i] = diff.Added
		}
		for err1 == nil && err2 == nil {
			cmp, d := q.compareRows(pkOrds, row1, row2)
			switch cmp {
			case -1: // deleted
				if err = wr.WriteRow(ctx, row1, diff.Removed, removedChange); err != nil {
					return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
				}
				row1, err1 = rowIter1.Next(sqlCtx)
			case 1: // added
				if err = wr.WriteRow(ctx, row2, diff.Added, addedChange); err != nil {
					return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
				}
				row2, err2 = rowIter2.Next(sqlCtx)
			default: // modified or no change
				if d {
					if err = wr.WriteCombinedRow(ctx, row1, row2, diff.ModeContext); err != nil {
						return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
					}
				}
				row1, err1 = rowIter1.Next(sqlCtx)
				row2, err2 = rowIter2.Next(sqlCtx)
			}
		}

		// Append any remaining rows
		if err1 == io.EOF && err2 == io.EOF {
		} else if err1 == io.EOF {
			if err = wr.WriteRow(ctx, row2, diff.Added, addedChange); err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
			for {
				row2, err2 = rowIter2.Next(sqlCtx)
				if err2 == io.EOF {
					break
				}
				if err = wr.WriteRow(ctx, row2, diff.Added, addedChange); err != nil {
					return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
				}
			}
			rowIter1.Close(sqlCtx)
		} else if err2 == io.EOF  {
			if err = wr.WriteRow(ctx, row1, diff.Removed, removedChange); err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
			for {
				row1, err1 = rowIter1.Next(sqlCtx)
				if err1 == io.EOF {
					break
				}
				if err = wr.WriteRow(ctx, row1, diff.Removed, removedChange); err != nil {
					return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
				}
			}
		} else {
			if err1 != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err1), usage)
			} else {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err2), usage)
			}
		}
	} else {
		//cliWR := iohelp.NopWrCloser(cli.OutStream)
		//wr := tabular.NewFixedWidthTableWriter(append(schema1, schema2...), cliWR, 100)
		//defer wr.Close(ctx)
		//
		//var err1, err2 error
		//var row1, row2 sql.Row
		//for {
		//	row1, err1 = rowIter1.Next(sqlCtx)
		//	if err1 == io.EOF {
		//		break
		//	}
		//	_, rowIter2, _ = queryist.Query(sqlCtx, query2)
		//	for {
		//		row2, err2 = rowIter2.Next(sqlCtx)
		//		if err2 == io.EOF {
		//			break
		//		}
		//		wr.WriteSqlRow(ctx, append(row1, row2...))
		//	}
		//}

		dw, err := newDiffWriter(TabularDiffOutput)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		err = dw.BeginTable(query1, query2, false, false)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		sch := append(schema1, schema2...)
		wr, err := dw.RowWriter(nil, nil, diff.TableDeltaSummary{}, sch)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		defer wr.Close(ctx)

		removedChange := make([]diff.ChangeType, len(sch))
		for {
			row, err := rowIter1.Next(sqlCtx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
			for range schema2 {
				row = append(row, nil)
			}
			if err = wr.WriteRow(ctx, row, diff.Removed, removedChange); err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
		}

		addedChange := make([]diff.ChangeType, len(sch))
		for {
			row, err := rowIter2.Next(sqlCtx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
			for range schema1 {
				row = append(sql.Row{nil}, row...)
			}
			if err = wr.WriteRow(ctx, row, diff.Added, addedChange); err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
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