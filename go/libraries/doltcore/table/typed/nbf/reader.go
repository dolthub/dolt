package nbf

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"io"
)

var ReadBufSize = 256 * 1024

//NBFReader reads rows from nbf files (Noms Binary Format).  The
type NBFReader struct {
	closer io.Closer
	bRd    *bufio.Reader
	sch    *schema.Schema
}

func OpenNBFReader(path string, fs filesys.ReadableFS) (*NBFReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	return NewNBFReader(r)
}

func NewNBFReader(r io.ReadCloser) (*NBFReader, error) {
	br := bufio.NewReaderSize(r, ReadBufSize)
	sch, err := ReadBinarySchema(br)

	if err != nil {
		r.Close()
		return nil, err
	}

	return &NBFReader{r, br, sch}, nil
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (nbfr *NBFReader) ReadRow() (*table.Row, error) {
	sch := nbfr.sch
	numFields := sch.NumFields()
	fieldVals := make([]types.Value, 0, numFields)

	var err error
	for i := 0; i < numFields; i++ {
		f := sch.GetField(i)

		var val types.Value
		val, err = types.ReadValue(types.NomsKind(f.NomsKind()), nbfr.bRd)

		if val != nil {
			fieldVals = append(fieldVals, val)
		}

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
	}

	numFieldVals := len(fieldVals)
	if numFieldVals == 0 {
		return nil, err
	} else if numFieldVals != numFields {
		if err == nil {
			err = table.NewBadRow(nil, fmt.Sprintf("Schema specifies %d fields but row has %d values.", numFields, numFieldVals))
		}
		return nil, err
	}

	return table.NewRow(table.RowDataFromValues(nbfr.sch, fieldVals)), nil
}

// GetSchema gets the schema of the rows that this writer writes
func (nbfr *NBFReader) GetSchema() *schema.Schema {
	return nbfr.sch
}

// Close should release resources being held
func (nbfr *NBFReader) Close() error {
	if nbfr.closer != nil {
		err := nbfr.closer.Close()
		nbfr.closer = nil

		return err
	} else {
		return errors.New("Already closed.")
	}
}
