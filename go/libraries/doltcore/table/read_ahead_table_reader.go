package table

import (
	"context"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/utils/async"
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

// VerifySchema checks that the incoming schema matches the schema from the existing table
func (tr *AsyncReadAheadTableReader) VerifySchema(outSch schema.Schema) (bool, error) {
	return tr.backingReader.VerifySchema(outSch)
}

// Close releases resources being held
func (tr *AsyncReadAheadTableReader) Close(ctx context.Context) error {
	_ = tr.reader.Close()
	return tr.backingReader.Close(ctx)
}
