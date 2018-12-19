package noms

import (
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"io"
)

// NomsMapReader is a TableReader that reads rows from a noms table which is stored in a types.Map where the key is
// a types.Value and the value is a types.List.  This list is a tuple of field values.
type NomsMapReader struct {
	sch *schema.Schema
	itr types.MapIterator
}

// NewNomsMapReader creates a NomsMapReader for a given noms types.Map
func NewNomsMapReader(m types.Map, sch *schema.Schema) *NomsMapReader {
	itr := m.Iterator()

	return &NomsMapReader{sch, itr}
}

// GetSchema gets the schema of the rows that this reader will return
func (nmr *NomsMapReader) GetSchema() *schema.Schema {
	return nmr.sch
}

// ReadRow reads a row from a table.  If there is a bad row ErrBadRow will be returned. This is a potentially
// non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (nmr *NomsMapReader) ReadRow() (*table.Row, error) {
	var key types.Value
	var val types.Value
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = errors.New("Error reading next value")
			}
		}()
		key, val = nmr.itr.Next()
	}()

	if err != nil {
		return nil, err
	} else if key == nil {
		return nil, io.EOF
	}

	if valList, ok := val.(types.List); !ok {
		return nil, errors.New("Map value is not a list. This map is not a valid Dolt table.")
	} else {
		return table.NewRow(table.RowDataFromPKAndValueList(nmr.sch, key, valList)), nil
	}
}

// Close should release resources being held
func (nmr *NomsMapReader) Close() error {
	nmr.itr = nil
	return nil
}
