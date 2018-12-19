package table

import (
	"io"

	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
)

type SchemaValidation int

const (
	ValidateSameInstance SchemaValidation = iota
	SlowlyCheckEachField
)

// InMemTable holds a simple list of rows that can be retrieved, or appended to.  It is meant primarily for testing.
type InMemTable struct {
	sch        *schema.Schema
	rows       []*Row
	validation SchemaValidation
}

// NewInMemTable creates an empty Table with the expectation that any rows added will have the given
// Schema
func NewInMemTable(sch *schema.Schema) *InMemTable {
	return NewInMemTableWithData(sch, []*Row{})
}

// NewInMemTableWithData creates a Table with the riven rows
func NewInMemTableWithData(sch *schema.Schema, rows []*Row) *InMemTable {
	return NewInMemTableWithDataAndValidationType(sch, rows, SlowlyCheckEachField)
}

func NewInMemTableWithDataAndValidationType(sch *schema.Schema, rows []*Row, validation SchemaValidation) *InMemTable {
	return &InMemTable{sch, rows, validation}
}

// AppendRow appends a row.  Appended rows must have the correct columns.
func (imt *InMemTable) AppendRow(row *Row) error {
	rowSch := row.GetSchema()
	if rowSch != imt.sch {

		invalid := true
		if imt.validation == SlowlyCheckEachField {
			invalid = rowSch.Equals(imt.sch)
		}

		if invalid {
			panic("Can't write a row to a table if it has different columns.")
		}
	}

	imt.rows = append(imt.rows, row)

	return nil
}

// GetRow gets a row by index
func (imt *InMemTable) GetRow(index int) (*Row, error) {
	return imt.rows[index], nil
}

// NumRows returns the number of rows in the table
func (imt *InMemTable) NumRows() int {
	return len(imt.rows)
}

// InMemTableReader is an implementation of a TableReader for an InMemTable
type InMemTableReader struct {
	tt      *InMemTable
	current int
}

// NewInMemTableReader creates an instance of a TableReader from an InMemTable
func NewInMemTableReader(imt *InMemTable) *InMemTableReader {
	return &InMemTableReader{imt, 0}
}

// ReadRow reads a row from a table.  If there is a bad row ErrBadRow will be returned. This is a potentially
// non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (r *InMemTableReader) ReadRow() (*Row, error) {
	numRows := r.tt.NumRows()

	if r.current < numRows {
		row := r.tt.rows[r.current]
		r.current++

		return &Row{row.data, nil}, nil
	}

	return nil, io.EOF
}

// Close should release resources being held
func (r *InMemTableReader) Close() error {
	r.current = -1
	return nil
}

// GetSchema gets the schema of the rows that this reader will return
func (r *InMemTableReader) GetSchema() *schema.Schema {
	return r.tt.sch
}

// InMemTableWriter is an implementation of a TableWriter for an InMemTable
type InMemTableWriter struct {
	tt *InMemTable
}

// NewInMemTableWriter creates an instance of a TableWriter from an InMemTable
func NewInMemTableWriter(imt *InMemTable) *InMemTableWriter {
	return &InMemTableWriter{imt}
}

// WriteRow will write a row to a table
func (w *InMemTableWriter) WriteRow(row *Row) error {
	return w.tt.AppendRow(row)
}

// Close should flush all writes, release resources being held
func (w *InMemTableWriter) Close() error {
	return nil
}

// GetSchema gets the schema of the rows that this writer writes
func (w *InMemTableWriter) GetSchema() *schema.Schema {
	return w.tt.sch
}
