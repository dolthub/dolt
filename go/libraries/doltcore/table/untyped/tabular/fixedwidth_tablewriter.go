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

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
)

// FixedWidthTableWriter is a TableWriter that applies a fixed width transform to its fields. All fields are
// expected to be strings.
type FixedWidthTableWriter struct {
	// number of samples taken during the auto fixed width calculation phase
	numSamples int
	// number of rows actually written (not just samples)
	numRowsWritten int
	// Max print width for each column
	printWidths []int
	// Max runes per column
	maxRunes []int
	// formatter knows how to format columns in the rows given
	formatter *fwt.FixedWidthFormatter
	// Buffer of rows that have yet to be printed
	rowBuffer []tableRow
	// Schema for results
	schema sql.Schema
	// closer is a writer to close when Close is called
	closer io.Closer
	// wr is where to direct tableRow output
	wr *bufio.Writer
	// flushedSampleBuffer records whether we've already written buffered rows to output
	flushedSampleBuffer bool
}

type tableRow struct {
	columns []string
	colors  []*color.Color
}

// applyColors rewrites the strings with the colors given
func (r tableRow) applyColors(strs []string) {
	for i, color := range r.colors {
		if color == nil {
			continue
		}
		strs[i] = color.Sprint(strs[i])
	}
}

func NewFixedWidthTableWriter(schema sql.Schema, wr io.WriteCloser, numSamples int) *FixedWidthTableWriter {
	bwr := bufio.NewWriterSize(wr, writeBufSize)
	fwtw := FixedWidthTableWriter{
		printWidths: make([]int, len(schema)),
		maxRunes:    make([]int, len(schema)),
		rowBuffer:   make([]tableRow, numSamples),
		schema:      schema,
		closer:      wr,
		wr:          bwr,
	}
	fwtw.seedColumnWidthsWithColumnNames()
	return &fwtw
}

func (w *FixedWidthTableWriter) seedColumnWidthsWithColumnNames() {
	for i := range w.schema {
		colName := w.schema[i].Name
		printWidth := fwt.StringWidth(colName)
		numRunes := len([]rune(colName))
		w.printWidths[i] = printWidth
		w.maxRunes[i] = numRunes
	}
}

func (w *FixedWidthTableWriter) Close(ctx context.Context) error {
	err := w.flushSampleBuffer()
	if err != nil {
		return err
	}

	if w.numRowsWritten > 0 {
		err = w.writeFooter()
		if err != nil {
			return err
		}

		err = w.wr.Flush()
		if err != nil {
			return err
		}
	}

	return w.closer.Close()
}

var colDiffColors = map[diff.ChangeType]*color.Color{
	diff.Inserted:    color.New(color.Bold, color.FgGreen),
	diff.ModifiedOld: color.New(color.FgRed),
	diff.ModifiedNew: color.New(color.FgGreen),
	diff.Deleted:     color.New(color.Bold, color.FgRed),
}

func (w *FixedWidthTableWriter) WriteRow(ctx context.Context, r sql.Row, colors []*color.Color) error {
	if colors == nil {
		colors = make([]*color.Color, len(r))
	}

	if len(r) != len(colors) {
		return fmt.Errorf("different sizes for row and colors: got %d and %d", len(r), len(colors))
	}

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

		row, err := rowToTableRow(r, colors)
		if err != nil {
			return err
		}

		err = w.writeRow(row)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *FixedWidthTableWriter) sampleRow(r sql.Row, colors []*color.Color) (tableRow, error) {
	row := tableRow{
		columns: make([]string, len(r)),
		colors:  colors,
	}

	for i := range r {
		str, err := stringValue(r[i])
		if err != nil {
			return row, err
		}

		printWidth := fwt.StringWidth(str)
		numRunes := len([]rune(str))

		if printWidth > w.printWidths[i] {
			w.printWidths[i] = printWidth
		}

		if numRunes > w.maxRunes[i] {
			w.maxRunes[i] = numRunes
		}

		row.columns[i] = str
	}

	return row, nil
}

func (w *FixedWidthTableWriter) flushSampleBuffer() error {
	if w.flushedSampleBuffer {
		return nil
	}

	if w.formatter == nil {
		// TODO: a better behavior might be to re-sample after the initial buffer runs out, and just let each buffer range
		//  have its own local set of fixed widths
		formatter := fwt.NewFixedWidthFormatter(fwt.PrintAllWhenTooLong, w.printWidths, w.maxRunes)
		w.formatter = &formatter
	}

	for i := 0; i < w.numSamples; i++ {
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

func stringValue(i interface{}) (string, error) {
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

func (w *FixedWidthTableWriter) writeRow(row tableRow) error {
	if w.numRowsWritten == 0 {
		err := w.writeHeader()
		if err != nil {
			return err
		}
	}

	formattedCols, err := w.formatter.Format(row.columns)
	if err != nil {
		return err
	}

	row.applyColors(formattedCols)

	var rowStr strings.Builder
	rowStr.WriteString("|")
	for i := range formattedCols {
		rowStr.WriteString(" ")
		rowStr.WriteString(formattedCols[i])
		rowStr.WriteString(" |")
	}

	w.numRowsWritten++
	return iohelp.WriteLine(w.wr, rowStr.String())
}

func rowToTableRow(row sql.Row, colors []*color.Color) (tableRow, error) {
	tRow := tableRow{
		columns: make([]string, len(row)),
		colors: colors,
	}

	var err error
	for i := range row {
		tRow.columns[i], err = stringValue(row[i])
		if err != nil {
			return tableRow{}, err
		}
	}

	return tRow, nil
}

func (w *FixedWidthTableWriter) writeHeader() error {
	err := w.writeSeparator()
	if err != nil {
		return err
	}

	colNames := make([]string, len(w.schema))
	for i := range w.schema {
		colNames[i] = w.schema[i].Name
	}

	formattedColNames, err := w.formatter.Format(colNames)
	if err != nil {
		return err
	}

	var colNameLine strings.Builder
	colNameLine.WriteString("|")
	for _, name := range formattedColNames {
		colNameLine.WriteString(" ")
		colNameLine.WriteString(name)
		colNameLine.WriteString(" |")
	}

	err = iohelp.WriteLine(w.wr, colNameLine.String())
	if err != nil {
		return err
	}

	return w.writeSeparator()
}

func (w *FixedWidthTableWriter) writeSeparator() error {
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
		strLen := fwt.StringWidth(name)
		for i := 0; i < strLen; i++ {
			separator.WriteString("-")
		}
		separator.WriteString("-+")
	}

	return iohelp.WriteLine(w.wr, separator.String())
}

func (w *FixedWidthTableWriter) writeFooter() error {
	return w.writeSeparator()
}
