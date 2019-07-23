// Copyright 2019 Liquidata, Inc.
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

package table

import (
	"context"
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"io"
)

// TableReader is an interface for reading rows from a table
type TableReader interface {
	// GetSchema gets the schema of the rows that this reader will return
	GetSchema() schema.Schema

	// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
	// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
	ReadRow(ctx context.Context) (row.Row, error)
}

// TableWriteCloser is an interface for writing rows to a table
type TableWriter interface {
	// GetSchema gets the schema of the rows that this writer writes
	GetSchema() schema.Schema

	// WriteRow will write a row to a table
	WriteRow(ctx context.Context, r row.Row) error
}

// TableCloser is an interface for a table stream that can be closed to release resources
type TableCloser interface {
	// Close should release resources being held
	Close(ctx context.Context) error
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

// PipeRows will read a row from given TableReader and write it to the provided TableWriter.  It will do this
// for every row until the TableReader's ReadRow method returns io.EOF or encounters an error in either reading
// or writing.  The caller will need to handle closing the tables as necessary. If contOnBadRow is true, errors reading
// or writing will be ignored and the pipe operation will continue.
//
// Returns a tuple: (number of rows written, number of errors ignored, error). In the case that err is non-nil, the
// row counter fields in the tuple will be set to -1.
func PipeRows(ctx context.Context, rd TableReader, wr TableWriter, contOnBadRow bool) (int, int, error) {
	var numBad, numGood int
	for {
		r, err := rd.ReadRow(ctx)

		if err != nil && err != io.EOF {
			if IsBadRow(err) && contOnBadRow {
				numBad++
				continue
			}

			return -1, -1, err
		} else if err == io.EOF && r == nil {
			break
		} else if r == nil {
			// row equal to nil should
			return -1, -1, errors.New("reader returned nil row with err==nil")
		}

		err = wr.WriteRow(ctx, r)

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
func ReadAllRows(ctx context.Context, rd TableReader, contOnBadRow bool) ([]row.Row, int, error) {
	var rows []row.Row
	var err error

	badRowCount := 0
	for {
		var r row.Row
		r, err = rd.ReadRow(ctx)

		if err != nil && err != io.EOF || r == nil {
			if IsBadRow(err) {
				badRowCount++

				if contOnBadRow {
					continue
				}
			}

			break
		}

		rows = append(rows, r)
	}

	if err == nil || err == io.EOF {
		return rows, badRowCount, nil
	}

	return nil, badRowCount, err
}

// ReadAllRowsToMap reads all rows from a TableReader and returns a map containing those rows keyed off of the index
// provided.
/*func ReadAllRowsToMap(rd TableReader, keyIndex int, contOnBadRow bool) (map[types.Value][]row.Row, int, error) {
	if keyIndex < 0 || keyIndex >= rd.GetSchema().NumFields() {
		panic("Invalid index is out of range of fields.")
	}

	var err error
	rows := make(map[types.Value][]row.Row)

	badRowCount := 0
	for {
		var r row.Row
		r, err = rd.ReadRow()

		if err != nil && err != io.EOF || r == nil {
			if IsBadRow(err) {
				badRowCount++

				if contOnBadRow {
					continue
				}
			}

			break
		}

		keyVal, _ := row.GetField(keyIndex)
		rowsForThisKey := rows[keyVal]
		rowsForThisKey = append(rowsForThisKey, r)
		rows[keyVal] = rowsForThisKey
	}

	if err == nil || err == io.EOF {
		return rows, badRowCount, nil
	}

	return nil, badRowCount, err
}*/
