package nbf

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"io"
	"path/filepath"
)

var WriteBufSize = 256 * 1024

// NBFWriter writes data serialized as binary noms values.  NBFWriter will only write data if it is sorted by primary key.
type NBFWriter struct {
	closer io.Closer
	bWr    *bufio.Writer
	sch    *schema.Schema
	lastPK types.Value
}

// OpenNBFWriter opens a file within the provided filesystem for writing and writes the schema of the rows that will
// follow on subsequent calls to WriteRow.  The schema must include a primary key constraint.
func OpenNBFWriter(path string, fs filesys.WritableFS, outSch *schema.Schema) (*NBFWriter, error) {
	if outSch.GetPKIndex() == -1 {
		// Do not allow this function to be called if you aren't going to provide a valid output schema with a primary
		// key constraint.
		panic("Only schemas containing a primary key constraint can be used with nbf files.")
	}

	err := fs.MkDirs(filepath.Dir(path))

	if err != nil {
		return nil, err
	}

	wr, err := fs.OpenForWrite(path)

	if err != nil {
		return nil, err
	}

	return NewNBFWriter(wr, outSch)
}

// NewNBFWriter creates a new NBFWriter which will write to the provided write closer.  Closing the NBFWriter will cause
// the supplied io.WriteCloser to be closed also.
func NewNBFWriter(wr io.WriteCloser, outSch *schema.Schema) (*NBFWriter, error) {
	bwr := bufio.NewWriterSize(wr, WriteBufSize)
	err := WriteBinarySchema(outSch, wr)

	if err != nil {
		wr.Close()
		return nil, err
	}

	return &NBFWriter{wr, bwr, outSch, nil}, nil
}

// GetSchema gets the schema of the rows that this writer writes
func (nbfw *NBFWriter) GetSchema() *schema.Schema {
	return nbfw.sch
}

// WriteRow will write a row to a table
func (nbfw *NBFWriter) WriteRow(row *table.Row) error {
	sch := row.GetSchema()
	if sch.NumFields() != nbfw.sch.NumFields() {
		return errors.New("Invalid row does not have the correct number of fields.")
	} else if !table.RowIsValid(row) {
		return table.ErrBadRow
	}

	pk := table.GetPKFromRow(row)

	if nbfw.lastPK == nil || nbfw.lastPK.Less(pk) {
		rowData := row.CurrData()

		// Use a buffer instead of the buffered writer in case there is an issue with a column in the field. If an err
		// occurs serializing a single value for the row then we return without having written any data.  If all values are
		// successfully serialized to the buffer, then we write the full wrote to the output writer.
		buf := bytes.NewBuffer(make([]byte, 0, 2048))
		for i := 0; i < sch.NumFields(); i++ {
			val, _ := rowData.GetField(i)

			_, err := types.WriteValue(val, buf)

			if err != nil {
				return err
			}
		}

		nbfw.bWr.Write(buf.Bytes())
		nbfw.lastPK = pk
		return nil
	}

	return errors.New("Primary keys out of order.")
}

// Close should flush all writes, release resources being held
func (nbfw *NBFWriter) Close() error {
	if nbfw.closer != nil {
		errFl := nbfw.bWr.Flush()
		errCl := nbfw.closer.Close()
		nbfw.closer = nil

		if errCl != nil {
			return errCl
		}

		return errFl
	} else {
		return errors.New("Already closed.")
	}
}
