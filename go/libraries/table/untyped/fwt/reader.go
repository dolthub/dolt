package fwt

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/iohelp"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped"
	"io"
	"strings"
)

var ReadBufSize = 256 * 1024

type FWTReader struct {
	closer io.Closer
	bRd    *bufio.Reader
	fwtSch *FWTSchema
	isDone bool
	colSep string
}

func OpenFWTReader(path string, fs filesys.ReadableFS, fwtSch *FWTSchema, colSep string) (*FWTReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	return NewFWTReader(r, fwtSch, colSep)
}

func NewFWTReader(r io.ReadCloser, fwtSch *FWTSchema, colSep string) (*FWTReader, error) {
	br := bufio.NewReaderSize(r, ReadBufSize)

	return &FWTReader{r, br, fwtSch, false, colSep}, nil
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (fwtRd *FWTReader) ReadRow() (*table.Row, error) {
	if fwtRd.isDone {
		return nil, io.EOF
	}

	var line string
	var err error
	isDone := false
	for line == "" && !isDone && err == nil {
		line, isDone, err = iohelp.ReadLine(fwtRd.bRd)

		if err != nil && err != io.EOF {
			return nil, err
		}
	}

	fwtRd.isDone = isDone
	if line != "" {
		row, err := fwtRd.parseRow([]byte(line))
		return row, err
	} else if err == nil {
		return nil, io.EOF
	}

	return nil, err
}

// GetSchema gets the schema of the rows that this reader will return
func (fwtRd *FWTReader) GetSchema() *schema.Schema {
	return fwtRd.fwtSch.Sch
}

// Close should release resources being held
func (fwtRd *FWTReader) Close() error {
	if fwtRd.closer != nil {
		err := fwtRd.closer.Close()
		fwtRd.closer = nil

		return err
	} else {
		return errors.New("Already closed.")
	}
}

func (fwtRd *FWTReader) parseRow(lineBytes []byte) (*table.Row, error) {
	sepWidth := len(fwtRd.colSep)
	expectedBytes := fwtRd.fwtSch.GetTotalWidth(sepWidth)
	if len(lineBytes) != expectedBytes {
		return nil, table.NewBadRow(nil, fmt.Sprintf("expected a line containing %d bytes, but only received %d", len(lineBytes), expectedBytes))
	}

	numFields := fwtRd.fwtSch.Sch.NumFields()
	fields := make([]string, numFields)

	offset := 0
	for i := 0; i < numFields; i++ {
		colWidth := fwtRd.fwtSch.Widths[i]

		if colWidth > 0 {
			fields[i] = strings.TrimSpace(string(lineBytes[offset : offset+colWidth]))
			offset += colWidth + sepWidth
		}
	}

	return untyped.NewRowFromStrings(fwtRd.GetSchema(), fields), nil
}
