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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

const writeBufSize = 256 * 1024

// FixedWidthTableWriter is a TableWriter that applies a fixed width transform to its fields. All fields are
// expected to be strings.
type FixedWidthTableWriter struct {
	// number of samples taken during the auto fixed width calculation phase
	numSamples int
	// number of rows actually written (not just samples)
	numRowsWritten int
	// Max print width for each column
	printWidths []int
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

var _ table.SqlRowWriter = (*FixedWidthTableWriter)(nil)

type tableRow struct {
	columns []string
	colors  []*color.Color
	widths  []FixedWidthString
	height  int
}

func NewFixedWidthTableWriter(schema sql.Schema, wr io.WriteCloser, numSamples int) *FixedWidthTableWriter {
	bwr := bufio.NewWriterSize(wr, writeBufSize)
	fwtw := FixedWidthTableWriter{
		printWidths: make([]int, len(schema)),
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
		stringWidth := NewFixedWidthString(colName)
		w.printWidths[i] = stringWidth.TotalWidth
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

var (
	colorAdded       = color.New(color.Bold, color.FgGreen)
	colorModifiedOld = color.New(color.FgRed)
	colorModifiedNew = color.New(color.FgGreen)
	colorRemoved     = color.New(color.Bold, color.FgRed)
)

var colDiffColors = map[diff.ChangeType]*color.Color{
	diff.Added:       colorAdded,
	diff.ModifiedOld: colorModifiedOld,
	diff.ModifiedNew: colorModifiedNew,
	diff.Removed:     colorRemoved,
}

func (w *FixedWidthTableWriter) WriteSqlRow(ctx context.Context, r sql.Row) error {
	return w.WriteColoredSqlRow(ctx, r, nil)
}

// WriteColoredSqlRow writes the given SQL row to the buffer. If colors are nil, then uses the default color.
func (w *FixedWidthTableWriter) WriteColoredSqlRow(ctx context.Context, r sql.Row, colors []*color.Color) error {
	strRow := make([]string, len(r))
	for i := range r {
		str, err := w.stringValue(i, r[i])
		if err != nil {
			return err
		}
		strRow[i] = str
	}
	return w.WriteColoredRow(ctx, strRow, nil, colors)
}

// WriteColoredRow writes the given row to the buffer. If widths are nil, then calculates the widths on the given
// strings. If colors are nil, then uses the default color.
func (w *FixedWidthTableWriter) WriteColoredRow(ctx context.Context, r []string, widths []FixedWidthString, colors []*color.Color) error {
	if len(colors) == 0 {
		colors = make([]*color.Color, len(r))
	}

	if len(r) != len(colors) {
		return fmt.Errorf("different sizes for row and colors: got %d and %d", len(r), len(colors))
	}

	if w.numSamples >= len(w.rowBuffer) {
		if err := w.flushSampleBuffer(); err != nil {
			return err
		}
		// We immediately set to false as we're going to write more rows to the buffer
		w.flushedSampleBuffer = false
	}
	strRow, err := w.sampleRow(r, widths, colors)
	if err != nil {
		return err
	}

	w.rowBuffer[w.numSamples] = strRow
	w.numSamples++

	return nil
}

func (w *FixedWidthTableWriter) sampleRow(r []string, widths []FixedWidthString, colors []*color.Color) (tableRow, error) {
	if len(widths) == 0 {
		widths = make([]FixedWidthString, len(r))
	}
	row := tableRow{
		columns: r,
		colors:  colors,
		widths:  widths,
		height:  1,
	}

	for i, str := range r {
		var width FixedWidthString
		if len(widths[i].Lines) == 0 {
			width = NewFixedWidthString(str)
			widths[i] = width
		} else {
			width = widths[i]
		}

		if width.DisplayWidth > w.printWidths[i] {
			w.printWidths[i] = width.DisplayWidth
		}

		if len(width.Lines) > row.height {
			row.height = len(width.Lines)
		}
	}

	return row, nil
}

func (w *FixedWidthTableWriter) flushSampleBuffer() error {
	if w.flushedSampleBuffer {
		return nil
	}

	for i := 0; i < w.numSamples; i++ {
		err := w.writeRow(w.rowBuffer[i])
		if err != nil {
			return err
		}
	}

	w.numSamples = 0
	w.flushedSampleBuffer = true

	return nil
}

func (w *FixedWidthTableWriter) stringValue(idx int, i interface{}) (string, error) {
	if i == nil {
		return "NULL", nil
	}
	return sqlutil.SqlColToStr(w.schema[idx].Type, i)
}

func (w *FixedWidthTableWriter) writeRow(row tableRow) error {
	if w.numRowsWritten == 0 {
		err := w.writeHeader()
		if err != nil {
			return err
		}
	}

	var rowStr strings.Builder
	for lineIndex := 0; lineIndex < row.height; lineIndex++ {
		if lineIndex > 0 {
			rowStr.WriteString("\n|")
		} else {
			rowStr.WriteString("|")
		}
		for i := range row.columns {
			var subsetStr string
			var paddingOffset int
			width := &row.widths[i]
			rowStr.WriteString(" ")
			// If there is a line here, then we set the substring. If there is no line, then this column will end up as padding.
			if lineIndex < len(width.Lines) {
				line := &width.Lines[lineIndex]
				subsetStr = row.columns[i][line.ByteStart:line.ByteEnd]
				paddingOffset = line.Width
				// Apply the color if we have one
				if row.colors[i] != nil {
					subsetStr = row.colors[i].Sprint(subsetStr)
				}
			}
			// This prints the string, then pads the remainder of the space to the right
			rowStr.WriteString(fmt.Sprintf("%s%*s", subsetStr, w.printWidths[i]-paddingOffset, ""))
			rowStr.WriteString(" |")
		}
	}

	w.numRowsWritten++
	return iohelp.WriteLine(w.wr, rowStr.String())
}

func (w *FixedWidthTableWriter) rowToTableRow(row sql.Row, colors []*color.Color) (tableRow, error) {
	tRow := tableRow{
		columns: make([]string, len(row)),
		colors:  colors,
		widths:  make([]FixedWidthString, len(row)),
		height:  1,
	}

	var err error
	for i := range row {
		tRow.columns[i], err = w.stringValue(i, row[i])
		if err != nil {
			return tableRow{}, err
		}

		stringWidth := NewFixedWidthString(tRow.columns[i])
		tRow.widths[i] = stringWidth
		if len(stringWidth.Lines) > tRow.height {
			tRow.height = len(stringWidth.Lines)
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

	var colNameLine strings.Builder
	colNameLine.WriteString("|")
	for i, name := range colNames {
		colNameLine.WriteString(" ")
		width := NewFixedWidthString(name)
		colNameLine.WriteString(fmt.Sprintf("%s%*s", name, w.printWidths[i]-width.TotalWidth, ""))
		colNameLine.WriteString(" |")
	}

	err = iohelp.WriteLine(w.wr, colNameLine.String())
	if err != nil {
		return err
	}

	return w.writeSeparator()
}

func (w *FixedWidthTableWriter) writeSeparator() error {
	var separator strings.Builder
	separator.WriteString("+")
	for _, printWidth := range w.printWidths {
		separator.WriteString("-")
		for i := 0; i < printWidth; i++ {
			separator.WriteString("-")
		}
		separator.WriteString("-+")
	}

	return iohelp.WriteLine(w.wr, separator.String())
}

func (w *FixedWidthTableWriter) writeFooter() error {
	return w.writeSeparator()
}
