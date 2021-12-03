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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/async"
)

var _ TableReadCloser = (*AsyncReadAheadTableReader)(nil)

// AsyncReadAheadTableReader is a TableReadCloser implementation that spins up a go routine to keep reading data into
// a buffered channel so that it is ready when the caller wants it.
type AsyncReadAheadTableReader struct {
	backingReader TableReadCloser
	reader        *async.AsyncReader
}

// NewAsyncReadAheadTableReader creates a new AsyncReadAheadTableReader
func NewAsyncReadAheadTableReader(tr TableReadCloser, bufferSize int) *AsyncReadAheadTableReader {
	read := func(ctx context.Context) (interface{}, error) {
		return tr.ReadRow(ctx)
	}

	reader := async.NewAsyncReader(read, bufferSize)
	return &AsyncReadAheadTableReader{tr, reader}
}

// Start the worker routine reading rows to the channel
func (tr *AsyncReadAheadTableReader) Start(ctx context.Context) error {
	return tr.reader.Start(ctx)
}

// GetSchema gets the schema of the rows that this reader will return
func (tr *AsyncReadAheadTableReader) GetSchema() schema.Schema {
	return tr.backingReader.GetSchema()
}

func (tr *AsyncReadAheadTableReader) GetSqlSchema() sql.Schema {
	panic("dont call plxxx")
}

func (tr *AsyncReadAheadTableReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	panic("please dont call plxx")
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and calling
// IsBadRow(err) will be return true. This is a potentially non-fatal error and callers can decide if they want to
// continue on a bad row, or fail.
func (tr *AsyncReadAheadTableReader) ReadRow(ctx context.Context) (row.Row, error) {
	obj, err := tr.reader.Read()

	if err != nil {
		return nil, err
	}

	return obj.(row.Row), err
}

// Close releases resources being held
func (tr *AsyncReadAheadTableReader) Close(ctx context.Context) error {
	_ = tr.reader.Close()
	return tr.backingReader.Close(ctx)
}
