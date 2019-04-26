package tabular

import (
	"bufio"
	"context"
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"io"
	"strings"
)

// WriteBufSize is the size of the buffer used when writing a csv file.  It is set at the package level and all
// writers create their own buffer's using the value of this variable at the time they create their buffers.
var WriteBufSize = 256 * 1024

// TextTableWriter implements TableWriter.  It writes table headers and rows as ascii-art tables.
// The first row written must be the column names for the table to write, and all rows written are assumed to have the
// same width for their respective columns (including the column names themselves). Unset columns will get the NULL
// string outputted. Clients must allow for the output of "NULL" as necessary.
type TextTableWriter struct {
	closer    io.Closer
	bWr       *bufio.Writer
	sch       schema.Schema
	colWidths map[uint64]int
}

// NewCSVWriter writes rows to the given WriteCloser based on the Schema and CSVFileInfo provided
func NewTextTableWriter(wr io.WriteCloser, sch schema.Schema) *TextTableWriter {
	bwr := bufio.NewWriterSize(wr, WriteBufSize)
	return &TextTableWriter{wr, bwr, sch, nil}
}

// writeTableHeader writes a table header with the column names given in the row provided, which is assumed to be
// string-typed. Should be called exactly once. Also has the side effect of filling in a column width for every tag.
// Therefore, it's vital that the row passed to this function has a value for every tag in the schema, and that it's
// the correct width for all values in that column.
func (ttw *TextTableWriter) writeTableHeader(r row.Row) error {
	allCols := ttw.sch.GetAllCols()

	ttw.colWidths = make(map[uint64]int, len(allCols.GetColumns()))

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
		ttw.colWidths[tag] = len(colName)

		for i := 0; i < len(colName); i++ {
			separator.WriteString("-")
		}
		colnames.WriteString(colName)
		separator.WriteString("-+")
		colnames.WriteString(" |")
		return false
	})

	return iohelp.WriteLines(ttw.bWr, separator.String(), colnames.String(), separator.String())
}

// writeTableFooter writes the final separator line for a table
func (ttw *TextTableWriter) writeTableFooter() error {
	allCols := ttw.sch.GetAllCols()

	var separator strings.Builder
	separator.WriteString("+")
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		separator.WriteString("-")
		colNameLen, ok := ttw.colWidths[tag]
		if !ok {
			panic("No column width recorded for tag " + string(tag))
		}

		for i := 0; i < colNameLen; i++ {
			separator.WriteString("-")
		}
		separator.WriteString("-+")
		return false
	})

	return iohelp.WriteLines(ttw.bWr, separator.String())
}

// GetSchema gets the schema of the rows that this writer writes
func (ttw *TextTableWriter) GetSchema() schema.Schema {
	return ttw.sch
}

// WriteRow will write a row to a table
func (ttw *TextTableWriter) WriteRow(ctx context.Context, r row.Row) error {
	// If this is the first row we've written, assume it's the column list
	if ttw.colWidths == nil {
		return ttw.writeTableHeader(r)
	}

	allCols := ttw.sch.GetAllCols()

	var rowVals strings.Builder
	rowVals.WriteString("|")
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		rowVals.WriteString(" ")
		val, _ := r.GetColVal(tag)
		if types.IsNull(val) {
			rowVals.WriteString(sql.PRINTED_NULL)
			colWidth, ok := ttw.colWidths[tag]
			if !ok {
				panic("No column width recorded for tag " + string(tag))
			}
			for i := 0; i < colWidth-len(sql.PRINTED_NULL); i++ {
				rowVals.WriteString(" ")
			}
		} else {
			if val.Kind() == types.StringKind {
				rowVals.WriteString(string(val.(types.String)))
			} else {
				rowVals.WriteString(types.EncodedValue(ctx, val))
			}
		}

		rowVals.WriteString(" |")
		return false
	})

	err := iohelp.WriteLines(ttw.bWr, rowVals.String())
	return err
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
