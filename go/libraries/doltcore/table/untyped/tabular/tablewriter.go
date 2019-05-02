package tabular

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/acarl005/stripansi"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
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
}

// NewCSVWriter writes rows to the given WriteCloser based on the Schema provided. The schema must contain only
func NewTextTableWriter(wr io.WriteCloser, sch schema.Schema) *TextTableWriter {
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		if col.Kind != types.StringKind {
			panic("Only string typed columns can be used to print a table")
		}
		return false
	})
	bwr := bufio.NewWriterSize(wr, writeBufSize)
	return &TextTableWriter{wr, bwr, sch, nil}
}

// writeTableHeader writes a table header with the column names given in the row provided, which is assumed to be
// string-typed and to have the appropriate fixed width set. Should be called exactly once.
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

		for i := 0; i < len(colName); i++ {
			separator.WriteString("-")
		}
		colnames.WriteString(colName)
		separator.WriteString("-+")
		colnames.WriteString(" |")
		return false
	})

	ttw.lastWritten = &r
	return iohelp.WriteLines(ttw.bWr, separator.String(), colnames.String(), separator.String())
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
		for i := 0; i < len(normalized); i++ {
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
	// If this is the first row we've written, assume it's the column list
	if ttw.lastWritten == nil {
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
