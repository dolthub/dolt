package tabular

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/acarl005/stripansi"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
	"io"
	"strconv"
	"strings"
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
func NewTextTableWriter(wr io.WriteCloser, sch schema.Schema) *TextTableWriter {
	return NewTextTableWriterWithNumHeaderRows(wr, sch, 1)
}

// NewTextTableWriterWithNumHeaderRows writes rows to the given WriteCloser based on the Schema provided, with the
// first numHeaderRows rows in the table header. The schema must contain only string type columns.
func NewTextTableWriterWithNumHeaderRows(wr io.WriteCloser, sch schema.Schema, numHeaderRows int) *TextTableWriter {
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		if col.Kind != types.StringKind {
			panic("Only string typed columns can be used to print a table")
		}
		return false
	})
	bwr := bufio.NewWriterSize(wr, writeBufSize)
	return &TextTableWriter{wr, bwr, sch, nil, numHeaderRows, 0}
}

// writeTableHeader writes a table header with the column names given in the row provided, which is assumed to be
// string-typed and to have the appropriate fixed width set.
func (ttw *TextTableWriter) writeTableHeader(r row.Row) error {
	allCols := ttw.sch.GetAllCols()

	var separator strings.Builder
	var colnames strings.Builder
	separator.WriteString("+")
	colnames.WriteString("|")
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		separator.WriteString("-")
		colnames.WriteString(" ")
		colNameVal, ok := r.GetColVal(tag)
		if !ok {
			panic("No column name value for tag " + string(tag))
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
		return false
	})

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
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
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
		return false
	})

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
	var err error
	rowVals.WriteString("|")
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		rowVals.WriteString(" ")
		val, _ := r.GetColVal(tag)
		if !types.IsNull(val) && val.Kind() == types.StringKind {
			rowVals.WriteString(string(val.(types.String)))
		} else {
			err = errors.New(fmt.Sprintf("Non-string value encountered: %v", val))
			return true
		}

		rowVals.WriteString(" |")
		return false
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
