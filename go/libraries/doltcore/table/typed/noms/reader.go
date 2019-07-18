package noms

import (
	"context"
	"io"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// NomsMapReader is a TableReader that reads rows from a noms table which is stored in a types.Map where the key is
// a types.Value and the value is a types.Tuple of field values.
type NomsMapReader struct {
	sch schema.Schema
	itr types.MapIterator
}

// NewNomsMapReader creates a NomsMapReader for a given noms types.Map
func NewNomsMapReader(ctx context.Context, m types.Map, sch schema.Schema) *NomsMapReader {
	itr := m.Iterator(ctx)

	return &NomsMapReader{sch, itr}
}

// GetSchema gets the schema of the rows that this reader will return
func (nmr *NomsMapReader) GetSchema() schema.Schema {
	return nmr.sch
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (nmr *NomsMapReader) ReadRow(ctx context.Context) (row.Row, error) {
	var key types.Value
	var val types.Value
	err := pantoerr.PanicToError("Error reading next value", func() error {
		key, val = nmr.itr.Next(ctx)
		return nil
	})

	if err != nil {
		return nil, err
	} else if key == nil {
		return nil, io.EOF
	}

	return row.FromNoms(nmr.sch, key.(types.Tuple), val.(types.Tuple)), nil
}

// Close should release resources being held
func (nmr *NomsMapReader) Close(ctx context.Context) error {
	nmr.itr = nil
	return nil
}
