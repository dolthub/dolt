package table

import (
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"io"
)

// ErrBadRow is the error that should be returned when ReadRow results in a row that isn't valid for the schema.  The
// caller can then decide if the program should continue on a bad row, or fail.
var ErrBadRow = errors.New("Bad Row")

// TableReader is an interface for reading rows from a table
type TableReader interface {
	// GetSchema gets the schema of the rows that this reader will return
	GetSchema() *schema.Schema

	// ReadRow reads a row from a table.  If there is a bad row ErrBadRow will be returned. This is a potentially
	// non-fatal error and callers can decide if they want to continue on a bad row, or fail.
	ReadRow() (*Row, error)
}

// TableWriteCloser is an interface for writing rows to a table
type TableWriter interface {
	// GetSchema gets the schema of the rows that this writer writes
	GetSchema() *schema.Schema

	// WriteRow will write a row to a table
	WriteRow(row *Row) error
}

// TableCloser is an interface for a table stream that can be closed to release resources
type TableCloser interface {
	// Close should release resources being held
	Close() error
}

// TableReadCloser is an interface for reading rows from a table, that can be closed.
type TableReadCloser interface {
	TableReader
	TableCloser
}

// TableWriteCloser is an interface for writing rows to a table, that can be closed
type TableWriteCloser interface {
	TableWriter
	TableCloser
}

// PipeRows will read a row from given TableReadCloser and write it to the provided TableWriteCloser.  It will do this
// for every row until the TableReadCloser's ReadRow method returns io.EOF or encounters an error in either reading
// or writing.  The caller will need to handle
func PipeRows(rd TableReader, wr TableWriter, contOnBadRow bool) (int, int, error) {
	var numBad, numGood int
	for {
		row, err := rd.ReadRow()

		if err != nil && err != io.EOF {
			if err == ErrBadRow && contOnBadRow {
				numBad++
				continue
			}

			return -1, -1, err
		} else if err == io.EOF && row == nil {
			break
		} else if row == nil {
			// row equal to nil should
			return -1, -1, errors.New("reader returned nil row with err==nil")
		}

		err = wr.WriteRow(row)

		if err != nil {
			return -1, -1, err
		} else {
			numGood++
		}
	}

	return numGood, numBad, nil
}

// ReadAllRows reads all rows from a TableReader and returns a slice containing those rows.  Usually this is used
// for testing, or with very small data sets.
func ReadAllRows(rd TableReader, contOnBadRow bool) ([]*Row, int, error) {
	var rows []*Row
	var err error

	badRowCount := 0
	for {
		var row *Row
		row, err = rd.ReadRow()

		if err != nil && err != io.EOF || row == nil {
			if err == ErrBadRow {
				badRowCount++

				if contOnBadRow {
					continue
				}
			}

			break
		}

		rows = append(rows, row)
	}

	if err == nil || err == io.EOF {
		return rows, badRowCount, nil
	}

	return nil, badRowCount, err
}

// ReadAllRowsToMap reads all rows from a TableReader and returns a map containing those rows keyed off of the index
// provided.
func ReadAllRowsToMap(rd TableReader, keyIndex int, contOnBadRow bool) (map[types.Value][]*Row, int, error) {
	if keyIndex < 0 || keyIndex >= rd.GetSchema().NumFields() {
		panic("Invalid index is out of range of fields.")
	}

	var err error
	rows := make(map[types.Value][]*Row)

	badRowCount := 0
	for {
		var row *Row
		row, err = rd.ReadRow()

		if err != nil && err != io.EOF || row == nil {
			if err == ErrBadRow {
				badRowCount++

				if contOnBadRow {
					continue
				}
			}

			break
		}

		keyVal, _ := row.CurrData().GetField(keyIndex)
		rowsForThisKey := rows[keyVal]
		rowsForThisKey = append(rowsForThisKey, row)
		rows[keyVal] = rowsForThisKey
	}

	if err == nil || err == io.EOF {
		return rows, badRowCount, nil
	}

	return nil, badRowCount, err
}
