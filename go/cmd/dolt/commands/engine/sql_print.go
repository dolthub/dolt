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
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/parquet"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/util/outputpager"
)

type PrintResultFormat byte

const (
	FormatTabular PrintResultFormat = iota
	FormatCsv
	FormatJson
	FormatNull // used for profiling
	FormatVertical
	FormatParquet
)

type PrintSummaryBehavior byte

const (
	PrintNoSummary         PrintSummaryBehavior = 0
	PrintRowCountAndTiming                      = 1
)

// PrettyPrintResults prints the result of a query in the format provided
func PrettyPrintResults(ctx *sql.Context, resultFormat PrintResultFormat, sqlSch sql.Schema, rowIter sql.RowIter, pageResults, showWarnings, printOkResult, binaryAsHex bool) (rerr error) {
	return prettyPrintResultsWithSummary(ctx, resultFormat, sqlSch, rowIter, PrintNoSummary, pageResults, showWarnings, printOkResult, binaryAsHex)
}

// PrettyPrintResultsExtended prints the result of a query in the format provided, including row count and timing info
func PrettyPrintResultsExtended(ctx *sql.Context, resultFormat PrintResultFormat, sqlSch sql.Schema, rowIter sql.RowIter, pageResults, showWarnings, printOkResult, binaryAsHex bool) (rerr error) {
	return prettyPrintResultsWithSummary(ctx, resultFormat, sqlSch, rowIter, PrintRowCountAndTiming, pageResults, showWarnings, printOkResult, binaryAsHex)
}

func prettyPrintResultsWithSummary(ctx *sql.Context, resultFormat PrintResultFormat, sqlSch sql.Schema, rowIter sql.RowIter, summary PrintSummaryBehavior, pageResults, showWarnings, printOkResult, binaryAsHex bool) (rerr error) {
	defer func() {
		closeErr := rowIter.Close(ctx)
		if rerr == nil && closeErr != nil {
			rerr = closeErr
		}
	}()

	start := ctx.QueryTime()

	// TODO: this isn't appropriate for JSON, CSV, other structured result formats
	if isOkResult(sqlSch) {
		// OkResult is only printed when we are in interactive terminal (TTY)
		if !printOkResult {
			return nil
		}
		return printOKResult(ctx, rowIter, start)
	}

	var wr table.SqlRowWriter
	var err error
	var numRows int

	// Function to print results. A function is required because we need to wrap the whole process in a swap of
	// IO streams. This is done with cli.ExecuteWithStdioRestored, which requires a resultless function. As
	// a result, we need to depend on side effects to numRows and err to determine if it was successful.
	printEm := func() {
		writerStream := cli.CliOut
		if pageResults {
			pager := outputpager.Start()
			defer pager.Stop()
			writerStream = pager.Writer
		}

		switch resultFormat {
		case FormatCsv:
			var err error
			wr, err = csv.NewCSVSqlWriter(iohelp.NopWrCloser(writerStream), sqlSch, csv.NewCSVInfo())
			if err != nil {
				return
			}
		case FormatJson:
			var err error
			wr, err = json.NewJSONSqlWriter(iohelp.NopWrCloser(writerStream), sqlSch)
			if err != nil {
				return
			}
		case FormatTabular:
			wr = tabular.NewFixedWidthTableWriter(sqlSch, iohelp.NopWrCloser(writerStream), 100)
		case FormatNull:
			wr = nullWriter{}
		case FormatVertical:
			wr = newVerticalRowWriter(iohelp.NopWrCloser(writerStream), sqlSch)
		case FormatParquet:
			var err error
			wr, err = parquet.NewParquetRowWriter(sqlSch, iohelp.NopWrCloser(writerStream))
			if err != nil {
				return
			}
		}

		// Wrap iterator with binary-to-hex transformation if needed
		if binaryAsHex {
			rowIter = newBinaryHexIterator(rowIter, sqlSch)
		}

		numRows, err = writeResultSet(ctx, rowIter, wr)
	}

	if pageResults {
		cli.ExecuteWithStdioRestored(printEm)
	} else {
		printEm()
	}
	if err != nil {
		return err
	}

	// if there is no row data and result format is JSON, then create empty JSON.
	if resultFormat == FormatJson && numRows == 0 {
		iohelp.WriteLine(cli.CliOut, "{}")
	}

	if summary == PrintRowCountAndTiming {
		warnings := ""
		if showWarnings {
			warnings = "\n"
			for _, warn := range ctx.Session.Warnings() {
				warnings += color.YellowString(fmt.Sprintf("\nWarning (Code %d): %s", warn.Code, warn.Message))
			}
		}

		err = printResultSetSummary(numRows, ctx.WarningCount(), warnings, start)
		if err != nil {
			return err
		}
	}

	// Some output formats need a final newline printed, others do not
	switch resultFormat {
	case FormatJson, FormatTabular, FormatVertical:
		return iohelp.WriteLine(cli.CliOut, "")
	default:
		return nil
	}
}

func printResultSetSummary(numRows int, numWarnings uint16, warningsList string, start time.Time) error {

	warning := ""
	if numWarnings > 0 {
		plural := ""
		if numWarnings > 1 {
			plural = "s"
		}
		warning = fmt.Sprintf(", %d warning%s", numWarnings, plural)
	}

	if numRows == 0 {
		printEmptySetResult(start, warning)
		return nil
	}

	noun := "rows"
	if numRows == 1 {
		noun = "row"
	}

	secondsSinceStart := secondsSince(start, time.Now())
	err := iohelp.WriteLine(cli.CliOut, fmt.Sprintf("%d %s in set%s (%.2f sec) %s", numRows, noun, warning, secondsSinceStart, warningsList))
	if err != nil {
		return err
	}

	return nil
}

// binaryHexIterator wraps a row iterator and transforms binary data to hex format
type binaryHexIterator struct {
	inner  sql.RowIter
	schema sql.Schema
}

var _ sql.RowIter = (*binaryHexIterator)(nil)

// newBinaryHexIterator creates a new iterator that transforms binary data to hex format
func newBinaryHexIterator(inner sql.RowIter, schema sql.Schema) sql.RowIter {
	return &binaryHexIterator{
		inner:  inner,
		schema: schema,
	}
}

// Next returns the next row with binary data transformed to hex format
func (iter *binaryHexIterator) Next(ctx *sql.Context) (sql.Row, error) {
	rowData, err := iter.inner.Next(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: Add support for BLOB types (TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB) and BIT type
	for i, val := range rowData {
		if val != nil && i < len(iter.schema) {
			switch iter.schema[i].Type.Type() {
			case sqltypes.Binary, sqltypes.VarBinary:
				switch v := val.(type) {
				case []byte:
					rowData[i] = sqlutil.BinaryAsHexDisplayValue(fmt.Sprintf("0x%X", v))
				case string: // handles results from sql-server; mysql wire protocol returns strings
					rowData[i] = sqlutil.BinaryAsHexDisplayValue(fmt.Sprintf("0x%X", []byte(v)))
				default:
					return nil, fmt.Errorf("unexpected type %T for binary column %s", val, iter.schema[i].Name)
				}
			}
		}
	}

	return rowData, nil
}

// Close closes the wrapped iterator and releases any resources.
func (iter *binaryHexIterator) Close(ctx *sql.Context) error {
	return iter.inner.Close(ctx)
}

// writeResultSet drains the iterator given, printing rows from it to the writer given. Returns the number of rows.
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

	return i, nil
}

// secondsSince returns the number of full and partial seconds since the time given
func secondsSince(start time.Time, end time.Time) float64 {
	runTime := end.Sub(start)
	seconds := runTime / time.Second
	milliRemainder := (runTime - seconds*time.Second) / time.Millisecond
	timeDisplay := float64(seconds) + float64(milliRemainder)*.001
	return timeDisplay
}

// nullWriter is a no-op SqlRowWriter implementation
type nullWriter struct{}

func (n nullWriter) WriteSqlRow(ctx *sql.Context, r sql.Row) error { return nil }
func (n nullWriter) Close(ctx context.Context) error               { return nil }

func printEmptySetResult(start time.Time, warning string) {
	seconds := secondsSince(start, time.Now())
	cli.Printf("Empty set%s (%.2f sec)\n", warning, seconds)
}

func printOKResult(ctx *sql.Context, iter sql.RowIter, start time.Time) error {
	row, err := iter.Next(ctx)
	if err != nil {
		return err
	}

	if okResult, ok := row[0].(types.OkResult); ok {
		rowNoun := "row"
		if okResult.RowsAffected != 1 {
			rowNoun = "rows"
		}

		seconds := secondsSince(start, time.Now())
		cli.Printf("Query OK, %d %s affected (%.2f sec)\n", okResult.RowsAffected, rowNoun, seconds)

		if okResult.Info != nil {
			cli.Printf("%s\n", okResult.Info)
		}
	}

	return nil
}

func isOkResult(sch sql.Schema) bool {
	return sch.Equals(types.OkResultSchema)
}

type verticalRowWriter struct {
	wr      io.WriteCloser
	sch     sql.Schema
	idx     int
	offsets []int
}

func newVerticalRowWriter(wr io.WriteCloser, sch sql.Schema) *verticalRowWriter {
	return &verticalRowWriter{
		wr:      wr,
		sch:     sch,
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
	return errors.New("unimplemented")
}

func (v *verticalRowWriter) Close(ctx context.Context) error {
	return v.wr.Close()
}

var space = []byte{' '}

func (v *verticalRowWriter) WriteSqlRow(ctx *sql.Context, r sql.Row) error {
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
			str, err = sqlutil.SqlColToStr(ctx, v.sch[i].Type, r[i])
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
