package fwt

import (
	"bufio"
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/iohelp"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"io"
	"path/filepath"
	"strings"
)

var WriteBufSize = 256 * 1024

type TextWriter struct {
	closer io.Closer
	bWr    *bufio.Writer
	sch    *schema.Schema
	colSep string
}

func OpenTextWriter(path string, fs filesys.WritableFS, sch *schema.Schema, colSep string) (table.TableWriteCloser, error) {
	err := fs.MkDirs(filepath.Dir(path))

	if err != nil {
		return nil, err
	}

	wr, err := fs.OpenForWrite(path)

	if err != nil {
		return nil, err
	}

	bwr := bufio.NewWriterSize(wr, WriteBufSize)
	return &TextWriter{wr, bwr, sch, colSep}, nil
}

func NewTextWriter(wr io.WriteCloser, sch *schema.Schema, colSep string) table.TableWriteCloser {
	bwr := bufio.NewWriterSize(wr, WriteBufSize)
	return &TextWriter{nil, bwr, sch, colSep}
}

// GetSchema gets the schema of the rows that this writer writes
func (tWr *TextWriter) GetSchema() *schema.Schema {
	return tWr.sch
}

// WriteRow will write a row to a table
func (tWr *TextWriter) WriteRow(row *table.Row) error {
	sch := row.GetSchema()
	rowData := row.CurrData()
	colStrs := make([]string, sch.NumFields())
	for i := 0; i < sch.NumFields(); i++ {
		val, _ := rowData.GetField(i)
		str := string(val.(types.String))
		colStrs[i] = str
	}

	lineStr := strings.Join(colStrs, tWr.colSep)
	err := iohelp.WriteAll(tWr.bWr, []byte(lineStr))

	if err != nil {
		return err
	}

	_, err = tWr.bWr.WriteRune('\n')

	return err
}

// Close should release resources being held
func (tWr *TextWriter) Close() error {
	if tWr.bWr != nil {
		errFl := tWr.bWr.Flush()
		tWr.bWr = nil

		if tWr.closer != nil {
			errCl := tWr.closer.Close()
			tWr.closer = nil

			if errCl != nil {
				return errCl
			}
		}

		return errFl
	} else {
		return errors.New("Already closed.")
	}
}
