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

package engine

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/go-mysql-server/sql"
)

type PrintResultFormat byte

const (
	FormatTabular PrintResultFormat = iota
	FormatCsv
	FormatJson
	FormatNull // used for profiling
	FormatVertical
)

type PrintSummaryBehavior byte

const (
	PrintNoSummary PrintSummaryBehavior = 0
	PrintRowCountAndTiming = 1
)

// PrettyPrintResults prints the result of a query in the format provided
func PrettyPrintResults(ctx *sql.Context, resultFormat PrintResultFormat, sqlSch sql.Schema, rowIter sql.RowIter) (rerr error) {
	return prettyPrintResultsWithSummary(ctx, resultFormat, sqlSch, rowIter, PrintNoSummary)
}

// PrettyPrintResultsExtended prints the result of a query in the format provided, including row count and timing info
func PrettyPrintResultsExtended(ctx *sql.Context, resultFormat PrintResultFormat, sqlSch sql.Schema, rowIter sql.RowIter) (rerr error) {
	return prettyPrintResultsWithSummary(ctx, resultFormat, sqlSch, rowIter, PrintRowCountAndTiming)
}

func prettyPrintResultsWithSummary(ctx *sql.Context, resultFormat PrintResultFormat, sqlSch sql.Schema, rowIter sql.RowIter, summary PrintSummaryBehavior) (rerr error) {
	defer func() {
		closeErr := rowIter.Close(ctx)
		if rerr == nil && closeErr != nil {
			rerr = closeErr
		}
	}()

	start := time.Now()

	if isOkResult(sqlSch) {
		return printOKResult(ctx, rowIter)
	}

	var wr table.SqlRowWriter

	switch resultFormat {
	case FormatCsv:
		// TODO: provide a CSV writer that takes a sql schema
		sch, err := sqlutil.ToDoltResultSchema(sqlSch)
		if err != nil {
			return err
		}

		wr, err = csv.NewCSVWriter(iohelp.NopWrCloser(cli.CliOut), sch, csv.NewCSVInfo())
		if err != nil {
			return err
		}
	case FormatJson:
		// TODO: provide a JSON writer that takes a sql schema
		sch, err := sqlutil.ToDoltResultSchema(sqlSch)
		if err != nil {
			return err
		}

		wr, err = json.NewJSONWriter(iohelp.NopWrCloser(cli.CliOut), sch)
		if err != nil {
			return err
		}
	case FormatTabular:
		wr = tabular.NewFixedWidthTableWriter(sqlSch, iohelp.NopWrCloser(cli.CliOut), 100)
	case FormatNull:
		wr = newNullWriter()
	case FormatVertical:
		wr = newVerticalRowWriter(iohelp.NopWrCloser(cli.CliOut), sqlSch)
	}

	numRows, err := writeResultSet(ctx, rowIter, wr)
	if err != nil {
		return err
	}

	if summary == PrintRowCountAndTiming {
		noun := "rows"
		if numRows == 1 {
			noun = "row"
		}

		runTime := time.Since(start)
		seconds := runTime / time.Second
		milliRemainder := (runTime - seconds) / time.Millisecond
		timeDisplay := float64(seconds) + float64(milliRemainder) * .001
		err := iohelp.WriteLine(cli.CliOut, fmt.Sprintf("%d %s in set (%.2f sec)", numRows, noun, timeDisplay))
		if err != nil {
			return err
		}
	}

	return iohelp.WriteLine(cli.CliOut, "")
}


type nullWriter struct {}

func (n nullWriter) WriteRow(ctx context.Context, r row.Row) error {
	return nil
}

func (n nullWriter) Close(ctx context.Context) error {
	return nil
}

func (n nullWriter) WriteSqlRow(ctx context.Context, r sql.Row) error {
	return nil
}

func newNullWriter() nullWriter {
	return nullWriter{}
}

func writeResultSet(ctx *sql.Context, rowIter sql.RowIter, wr table.SqlRowWriter) (int, error) {
	i := 0
	for {
		r, err := rowIter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return 0, err
		}

		err = wr.WriteSqlRow(ctx, r)
		if err != nil {
			return 0, err
		}

		i++
	}

	err := wr.Close(ctx)
	if err != nil {
		return 0, err
	}

	if i == 0 {
		printEmptySetResult(ctx)
	}

	return i, nil
}

func printEmptySetResult(ctx *sql.Context) {
	cli.Printf("Empty set\n") // TODO: timing info
}

func printOKResult(ctx *sql.Context, iter sql.RowIter) error {
	row, err := iter.Next(ctx)
	if err != nil {
		return err
	}

	if okResult, ok := row[0].(sql.OkResult); ok {
		rowNoun := "row"
		if okResult.RowsAffected != 1 {
			rowNoun = "rows"
		}

		cli.Printf("Query OK, %d %s affected\n", okResult.RowsAffected, rowNoun)

		if okResult.Info != nil {
			cli.Printf("%s\n", okResult.Info)
		}
	}

	return nil
}

func isOkResult(sch sql.Schema) bool {
	return sch.Equals(sql.OkResultSchema)
}

type verticalRowWriter struct {
	wr io.WriteCloser
	sch sql.Schema
	idx int
	offsets []int
}

func newVerticalRowWriter(wr io.WriteCloser, sch sql.Schema) *verticalRowWriter {
	return &verticalRowWriter{
		wr:  wr,
		sch: sch,
		offsets: calculateVerticalOffsets(sch),
	}
}

func calculateVerticalOffsets(sch sql.Schema) []int {
	offsets := make([]int, len(sch))

	maxLen := 0
	for i := range sch {
		if len(sch[i].Name) > maxLen {
			maxLen = len(sch[i].Name)
		}
	}

	for i := range sch {
		offsets[i] = maxLen - len(sch[i].Name)
	}

	return offsets
}

func (v *verticalRowWriter) WriteRow(ctx context.Context, r row.Row) error {
	return fmt.Errorf("unimplemented")
}

func (v *verticalRowWriter) Close(ctx context.Context) error {
	return v.wr.Close()
}

var space = []byte{' '}

func (v *verticalRowWriter) WriteSqlRow(ctx context.Context, r sql.Row) error {
	v.idx++
	sep := fmt.Sprintf("*************************** %d. row ***************************\n", v.idx)
	_, err := v.wr.Write([]byte(sep))
	if err != nil {
		return err
	}

	for i := range r {
		for numSpaces := 0; numSpaces < v.offsets[i]; numSpaces++ {
			_, err = v.wr.Write(space)
			if err != nil {
				return err
			}
		}

		var str string

		if r[i] == nil {
			str = "NULL"
		} else {
			str, err = sqlutil.SqlColToStr(v.sch[i].Type, r[i])
			if err != nil {
				return err
			}
		}

		_, err = v.wr.Write([]byte(fmt.Sprintf("%s: %v\n", v.sch[i].Name, str)))
		if err != nil {
			return err
		}
	}

	_, err = v.wr.Write([]byte("\n"))
	if err != nil {
		return err
	}

	return nil
}
