// Copyright 2019 Dolthub, Inc.
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
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/acarl005/stripansi"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
)

const writeBufSize = 256 * 1024

// TextTableWriter implements TableWriter.  It writes table headers and rows as ascii-art tables.
// The first row written must be the column names for the table to write, and all rows written are assumed to have the
// same width for their respective columns (including the column names themselves). Values for all columns in the
// schema must be set on each row.
type TextTableWriter struct {
	closer        io.Closer
	bWr           *bufio.Writer
	sch           schema.Schema
	lastWritten   *row.Row
	numHeaderRows int
	numHrsWritten int
}

// NewTextTableWriter writes rows to the given WriteCloser based on the Schema provided, with a single table header row.
// The schema must contain only string type columns.
func NewTextTableWriter(wr io.WriteCloser, sch schema.Schema) (*TextTableWriter, error) {
	return NewTextTableWriterWithNumHeaderRows(wr, sch, 1)
}

// NewTextTableWriterWithNumHeaderRows writes rows to the given WriteCloser based on the Schema provided, with the
// first numHeaderRows rows in the table header. The schema must contain only string type columns.
func NewTextTableWriterWithNumHeaderRows(wr io.WriteCloser, sch schema.Schema, numHeaderRows int) (*TextTableWriter, error) {
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Kind != types.StringKind {
			return false, errors.New("only string typed columns can be used to print a table")
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	bwr := bufio.NewWriterSize(wr, writeBufSize)
	return &TextTableWriter{
		closer:        wr,
		bWr:           bwr,
		sch:           sch,
		numHeaderRows: numHeaderRows,
	}, nil
}

// writeTableHeader writes a table header with the column names given in the row provided, which is assumed to be
// string-typed and to have the appropriate fixed width set.
func (ttw *TextTableWriter) writeTableHeader(r row.Row) error {
	allCols := ttw.sch.GetAllCols()

	var separator strings.Builder
	var colnames strings.Builder
	separator.WriteString("+")
	colnames.WriteString("|")
	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		separator.WriteString("-")
		colnames.WriteString(" ")
		colNameVal, ok := r.GetColVal(tag)
		if !ok {
			return false, fmt.Errorf("No column name value for tag %d", tag)
		}
		colName := string(colNameVal.(types.String))

		normalized := stripansi.Strip(colName)
		strLen := fwt.StringWidth(normalized)
		for i := 0; i < strLen; i++ {
			separator.WriteString("-")
		}

		colnames.WriteString(colName)
		separator.WriteString("-+")
		colnames.WriteString(" |")
		return false, nil
	})

	if err != nil {
		return err
	}

	ttw.lastWritten = &r

	// Write the separators and the column headers as necessary
	if ttw.numHrsWritten == 0 {
		if err := iohelp.WriteLines(ttw.bWr, separator.String()); err != nil {
			return err
		}
	}

	if err := iohelp.WriteLines(ttw.bWr, colnames.String()); err != nil {
		return err
	}

	ttw.numHrsWritten++
	if ttw.numHrsWritten == ttw.numHeaderRows {
		if err := iohelp.WriteLines(ttw.bWr, separator.String()); err != nil {
			return err
		}
	}

	return nil
}

// writeTableFooter writes the final separator line for a table
func (ttw *TextTableWriter) writeTableFooter() error {
	if ttw.lastWritten == nil {
		return errors.New("No rows written, cannot write footer")
	}

	allCols := ttw.sch.GetAllCols()

	var separator strings.Builder
	separator.WriteString("+")
	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		separator.WriteString("-")
		val, ok := (*ttw.lastWritten).GetColVal(tag)
		if !ok {
			panic("No column name value for tag " + strconv.FormatUint(tag, 10))
		}
		sval := string(val.(types.String))
		normalized := stripansi.Strip(sval)
		strLen := fwt.StringWidth(normalized)
		for i := 0; i < strLen; i++ {
			separator.WriteString("-")
		}
		separator.WriteString("-+")
		return false, nil
	})

	if err != nil {
		return err
	}

	return iohelp.WriteLine(ttw.bWr, separator.String())
}

// GetSchema gets the schema of the rows that this writer writes
func (ttw *TextTableWriter) GetSchema() schema.Schema {
	return ttw.sch
}

// WriteRow will write a row to a table
func (ttw *TextTableWriter) WriteRow(ctx context.Context, r row.Row) error {
	// Handle writing header rows as asked for
	if ttw.lastWritten == nil || ttw.numHrsWritten < ttw.numHeaderRows {
		return ttw.writeTableHeader(r)
	}

	allCols := ttw.sch.GetAllCols()

	var rowVals strings.Builder
	rowVals.WriteString("|")
	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		rowVals.WriteString(" ")
		val, _ := r.GetColVal(tag)
		if !types.IsNull(val) && val.Kind() == types.StringKind {
			rowVals.WriteString(string(val.(types.String)))
		} else {
			return false, errors.New(fmt.Sprintf("Non-string value encountered: %v", val))
		}

		rowVals.WriteString(" |")
		return false, nil
	})

	if err != nil {
		return err
	}

	ttw.lastWritten = &r
	return iohelp.WriteLine(ttw.bWr, rowVals.String())
}

// Close should flush all writes, release resources being held
func (ttw *TextTableWriter) Close(ctx context.Context) error {
	if ttw.closer != nil {
		// Write the table footer to finish the table off
		errFt := ttw.writeTableFooter()
		if errFt != nil {
			return errFt
		}

		errFl := ttw.bWr.Flush()
		errCl := ttw.closer.Close()
		ttw.closer = nil

		if errCl != nil {
			return errCl
		}

		return errFl
	} else {
		return errors.New("Already closed.")
	}
}
