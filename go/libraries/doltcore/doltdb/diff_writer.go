package doltdb

import (
	"bufio"
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"io"
	"path/filepath"
	"strings"
)

const (
	ColorRowProp = "color"
)

type ColorFunc func(string, ...interface{}) string

var WriteBufSize = 256 * 1024

type ColorDiffWriter struct {
	closer io.Closer
	bWr    *bufio.Writer
	sch    *schema.Schema
	colSep string
}

func OpenColorDiffWriter(path string, fs filesys.WritableFS, sch *schema.Schema, colSep string) (table.TableWriteCloser, error) {
	err := fs.MkDirs(filepath.Dir(path))

	if err != nil {
		return nil, err
	}

	wr, err := fs.OpenForWrite(path)

	if err != nil {
		return nil, err
	}

	return NewColorDiffWriter(wr, sch, colSep), nil
}

func NewColorDiffWriter(wr io.Writer, sch *schema.Schema, colSep string) table.TableWriteCloser {
	bwr := bufio.NewWriterSize(wr, WriteBufSize)
	return &ColorDiffWriter{nil, bwr, sch, colSep}
}

// GetSchema gets the schema of the rows that this writer writes
func (tWr *ColorDiffWriter) GetSchema() *schema.Schema {
	return tWr.sch
}

var colDiffColors = map[DiffChType]ColorFunc{
	DiffAdded:    color.GreenString,
	DiffModifiedOld: color.YellowString,
	DiffModifiedNew: color.YellowString,
	DiffRemoved:  color.RedString,
}

// WriteRow will write a row to a table
func (tWr *ColorDiffWriter) WriteRow(row *table.Row) error {
	sch := row.GetSchema()
	rowData := row.CurrData()
	colStrs := make([]string, sch.NumFields())
	colDiffs := make(map[string]DiffChType)
	if prop, ok := row.GetProperty(CollChangesProp); ok {
		if convertedVal, convertedOK := prop.(map[string]DiffChType); convertedOK {
			colDiffs = convertedVal
		}
	}

	for i := 0; i < sch.NumFields(); i++ {
		val, _ := rowData.GetField(i)
		str := string(val.(types.String))
		colStrs[i] = str
	}

	prefix := "   "
	colorColumns := false
	if prop, ok := row.GetProperty(DiffTypeProp); ok {
		if dt, convertedOK := prop.(DiffChType); convertedOK {
			switch dt {
			case DiffAdded:
				prefix = " + "
			case DiffRemoved:
				prefix = " - "
			case DiffModifiedOld:
				prefix = " < "
			case DiffModifiedNew:
				prefix = " > "
				colorColumns = true
			}
		}
	}

	if colorColumns {
		for i := 0; i < sch.NumFields(); i++ {
			fld := sch.GetField(i)
			fldName := fld.NameStr()
			if dt, ok := colDiffs[fldName]; ok {
				if colorFunc, ok := colDiffColors[dt]; ok {
					colStrs[i] = colorFunc(colStrs[i])
				}
			}
		}
	}

	lineStr := prefix + strings.Join(colStrs, tWr.colSep)

	if !colorColumns {
		if prop, ok := row.GetProperty(ColorRowProp); ok {
			colorer, convertedOK := prop.(func(string, ...interface{}) string)
			if convertedOK {
				lineStr = colorer(lineStr)
			}
		}
	}

	err := iohelp.WriteAll(tWr.bWr, []byte(lineStr))

	if err != nil {
		return err
	}

	_, err = tWr.bWr.WriteRune('\n')

	return err
}

// Close should release resources being held
func (tWr *ColorDiffWriter) Close() error {
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
