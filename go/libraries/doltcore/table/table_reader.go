// Copyright 2020 Dolthub, Inc.
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
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// Reader is an interface for reading rows from a table
type Reader interface {
	// GetSchema gets the schema of the rows that this reader will return
	GetSchema() schema.Schema

	// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and calling
	// IsBadRow(err) will be return true. This is a potentially non-fatal error and callers can decide if they want to
	// continue on a bad row, or fail.
	ReadRow(ctx context.Context) (row.Row, error)
}

// Closer is an interface for a writer that can be closed to release resources
type Closer interface {
	// Close should release resources being held
	Close(ctx context.Context) error
}

// ReadCloser is an interface for reading rows from a table, that can be closed.
type ReadCloser interface {
	Reader
	Closer
}

type SqlRowReader interface {
	ReadCloser

	ReadSqlRow(ctx context.Context) (sql.Row, error)
}

// SqlTableReader is a Reader that can read rows as sql.Row.
type SqlTableReader interface {
	// GetSchema gets the schema of the rows that this reader will return
	GetSchema() schema.Schema

	// ReadSqlRow reads a row from a table as go-mysql-server sql.Row.
	ReadSqlRow(ctx context.Context) (sql.Row, error)
}

// PipeRows will read a row from given TableReader and write it to the provided RowWriter.  It will do this
// for every row until the TableReader's ReadRow method returns io.EOF or encounters an error in either reading
// or writing.  The caller will need to handle closing the tables as necessary. If contOnBadRow is true, errors reading
// or writing will be ignored and the pipe operation will continue.
//
// Returns a tuple: (number of rows written, number of errors ignored, error). In the case that err is non-nil, the
// row counter fields in the tuple will be set to -1.
func PipeRows(ctx context.Context, rd SqlRowReader, wr SqlRowWriter, contOnBadRow bool) (int, int, error) {
	var numBad, numGood int
	for {
		r, err := rd.ReadSqlRow(ctx)

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

		err = wr.WriteSqlRow(ctx, r)

		if err != nil {
			return -1, -1, err
		} else {
			numGood++
		}
	}

	return numGood, numBad, nil
}
