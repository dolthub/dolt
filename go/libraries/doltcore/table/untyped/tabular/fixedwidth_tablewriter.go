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

	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/fwt"
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
}

func (w fixedWidthTableWriter) Close(ctx context.Context) error {
	return nil
}

func (w fixedWidthTableWriter) WriteRow(ctx context.Context, r sql.Row, colors []*color.Color) error {
	// if w.lastWritten == nil || w.numHrsWritten < w.numHeaderRows {
	// 	return w.writeTableHeader(r)
	// }

	if w.numSamples < len(w.rowBuffer) {
		strRow, err := w.sampleAndFormatRow(r, colors)
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

	}

	return nil
}

func (w fixedWidthTableWriter) sampleAndFormatRow(r sql.Row, colors []*color.Color) ([]string, error) {
	strRow := make([]string, len(r))
	for i := range r {
		str, err := w.format(r[i])
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

func (w fixedWidthTableWriter) flushSampleBuffer() error {
	for i := range w.rowBuffer {
		err := w.printRow(w.rowBuffer[i])
		if err != nil {
			return err
		}
	}

	w.numSamples = 0
	w.rowBuffer = nil

	return nil
}

func (w fixedWidthTableWriter) format(i interface{}) (string, error) {
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

func (w *fixedWidthTableWriter) printRow(row []string) error {
	if !w.writtenHeader {
		err := w.writeHeader()
		if err != nil {
			return err
		}
		w.writtenHeader = true
	}


}

func (w fixedWidthTableWriter) writeHeader() error {

}

