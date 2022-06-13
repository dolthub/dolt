// Copyright 2022 Dolthub, Inc.
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

package tabular

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/acarl005/stripansi"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
)

// fixedWidthTableWriter is a TableWriter that applies a fixed width transform to its fields. All fields are
// expected to be strings.
type fixedWidthTableWriter struct {
	// number of samples to take before beginning to print rows
	numSamples int
	// Max print width for each column
	printWidths []int
	// Max runes per column
	maxRunes []int
	// formatter knows how to format columns in the rows given
	formatter *fwt.FixedWidthFormatter
	// Buffer of rows that have yet to be printed
	rowBuffer [][]string
	// Schema for results
	schema sql.Schema
	// closer is a writer to close when Close is called
	closer        io.Closer
	// wr is where to direct row output
	wr *bufio.Writer
	// writtenHeader returns whether we've written the table header yet
	writtenHeader bool
	// flushedSampleBuffer records whether we've already written buffered rows to output
	flushedSampleBuffer bool
}

func NewFixedWidthTableWriter(schema sql.Schema, wr io.WriteCloser, numSamples int) *fixedWidthTableWriter {
	bwr := bufio.NewWriterSize(wr, writeBufSize)
	return &fixedWidthTableWriter{
		printWidths:         make([]int, len(schema)),
		maxRunes:            make([]int, len(schema)),
		rowBuffer:           make([][]string, numSamples),
		schema:              schema,
		closer:              wr,
		wr:                  bwr,
	}
}

func (w *fixedWidthTableWriter) Close(ctx context.Context) error {
	err := w.flushSampleBuffer()
	if err != nil {
		return err
	}

	err = w.writeFooter()
	if err != nil {
		return err
	}

	err = w.wr.Flush()
	if err != nil {
		return err
	}

	return w.closer.Close()
}

func (w *fixedWidthTableWriter) WriteRow(ctx context.Context, r sql.Row, colors []*color.Color) error {
	if w.numSamples < len(w.rowBuffer) {
		strRow, err := w.sampleRow(r, colors)
		if err != nil {
			return err
		}

		w.rowBuffer[w.numSamples] = strRow
		w.numSamples++
	} else {
		err := w.flushSampleBuffer()
		if err != nil {
			return err
		}

		err = w.writeRow(rowToStrings(r))
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *fixedWidthTableWriter) sampleRow(r sql.Row, colors []*color.Color) ([]string, error) {
	strRow := make([]string, len(r))
	for i := range r {
		str, err := w.stringValue(r[i])
		if err != nil {
			return nil, err
		}

		printWidth := fwt.StringWidth(str)
		numRunes := len([]rune(str))

		if printWidth > w.printWidths[i] {
			w.printWidths[i] = printWidth
		}

		if numRunes > w.maxRunes[i] {
			w.maxRunes[i] = numRunes
		}

		if colors[i] != nil {
			str = colors[i].Sprint(str)
		}

		strRow[i] = str
	}

	return strRow, nil
}

func (w *fixedWidthTableWriter) flushSampleBuffer() error {
	if w.flushedSampleBuffer {
		return nil
	}

	if w.formatter == nil {
		formatter := fwt.NewFixedWidthFormatter(fwt.PrintAllWhenTooLong, w.printWidths, w.maxRunes)
		w.formatter = &formatter
	}

	for i := range w.rowBuffer {
		err := w.writeRow(w.rowBuffer[i])
		if err != nil {
			return err
		}
	}

	w.numSamples = 0
	w.rowBuffer = nil
	w.flushedSampleBuffer = true

	return nil
}

func (w *fixedWidthTableWriter) stringValue(i interface{}) (string, error) {
	str := ""
	if i == nil {
		str = "NULL"
	} else {
		strVal, ok := i.(string)
		if !ok {
			return "", fmt.Errorf("expected string but got %T", i)
		}
		str = strVal
	}
	return str, nil
}

func (w *fixedWidthTableWriter) writeRow(row []string) error {
	if !w.writtenHeader {
		err := w.writeHeader()
		if err != nil {
			return err
		}
		w.writtenHeader = true
	}

	formattedCols, err := w.formatter.Format(row)
	if err != nil {
		return err
	}

	var rowStr strings.Builder
	rowStr.WriteString("|")
	for i := range formattedCols {
		rowStr.WriteString(" ")
		rowStr.WriteString(formattedCols[i])
		rowStr.WriteString(" |")
	}

	return iohelp.WriteLine(w.wr, rowStr.String())
}

func rowToStrings(row sql.Row) []string {
	strs := make([]string, len(row))
	for i := range row {
		var ok bool
		strs[i], ok = row[i].(string)
		if !ok {
			panic(fmt.Sprintf("expected string but got %T", row[i]))
		}
	}

	return strs
}

func (w *fixedWidthTableWriter) writeHeader() error {
	err := w.writeSepararator()
	if err != nil {
		return err
	}

	colNames := make([]string, len(w.schema))
	for i := range w.schema {
		colNames[i] = " "
	}

	formattedColNames, err := w.formatter.Format(colNames)
	if err != nil {
		return err
	}

	var colNameLine strings.Builder
	colNameLine.WriteString("|")
	for _, name := range formattedColNames {
		colNameLine.WriteString("-")
		colNameLine.WriteString(name)
		colNameLine.WriteString("-|")
	}

	err = iohelp.WriteLine(w.wr, colNameLine.String())
	if err != nil {
		return err
	}

	return w.writeSepararator()
}

func (w *fixedWidthTableWriter) writeSepararator() error {
	colNames := make([]string, len(w.schema))
	for i := range w.schema {
		colNames[i] = " "
	}

	formattedColNames, err := w.formatter.Format(colNames)
	if err != nil {
		return err
	}

	var separator strings.Builder
	separator.WriteString("+")
	for _, name := range formattedColNames {
		separator.WriteString("-")
		normalized := stripansi.Strip(name)
		strLen := fwt.StringWidth(normalized)
		for i := 0; i < strLen; i++ {
			separator.WriteString("-")
		}
		separator.WriteString("-+")
	}

	return iohelp.WriteLine(w.wr, separator.String())
}

func (w *fixedWidthTableWriter) writeFooter() error {
	return w.writeSepararator()
}

