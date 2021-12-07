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
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// CompositeTableReader is a TableReader implementation which will concatenate the results
// of multiple TableReader instances into a single set of results.
type CompositeTableReader struct {
	sch     schema.Schema
	readers []TableReadCloser
	idx     int
}

// NewCompositeTableReader creates a new CompositeTableReader instance from a slice of TableReadClosers.
func NewCompositeTableReader(readers []TableReadCloser) (*CompositeTableReader, error) {
	if len(readers) == 0 {
		panic("nothing to iterate")
	}

	sch := readers[0].GetSchema()
	for i := 1; i < len(readers); i++ {
		otherSch := readers[i].GetSchema()
		if !schema.SchemasAreEqual(sch, otherSch) {
			panic("readers must have the same schema")
		}
	}

	return &CompositeTableReader{sch: sch, readers: readers, idx: 0}, nil
}

// GetSchema gets the schema of the rows that this reader will return
func (rd *CompositeTableReader) GetSchema() schema.Schema {
	return rd.sch
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and calling
// IsBadRow(err) will be return true. This is a potentially non-fatal error and callers can decide if they want to
// continue on a bad row, or fail.
func (rd *CompositeTableReader) ReadRow(ctx context.Context) (row.Row, error) {
	for rd.idx < len(rd.readers) {
		r, err := rd.readers[rd.idx].ReadRow(ctx)

		if err == io.EOF {
			rd.idx++
			continue
		} else if err != nil {
			return nil, err
		}

		return r, nil
	}

	return nil, io.EOF
}

// VerifySchema checks that the incoming schema matches the schema from the existing table
func (rd *CompositeTableReader) VerifySchema(outSch schema.Schema) (bool, error) {
	return schema.VerifyInSchema(rd.sch, outSch)
}

// Close should release resources being held
func (rd *CompositeTableReader) Close(ctx context.Context) error {
	var firstErr error
	for _, rdr := range rd.readers {
		err := rdr.Close(ctx)

		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (rd *CompositeTableReader) GetSqlSchema() sql.PrimaryKeySchema {
	panic("todo")
}

func (rd *CompositeTableReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	panic("todo")
}
