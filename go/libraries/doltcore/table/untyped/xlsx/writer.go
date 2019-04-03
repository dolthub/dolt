package xlsx

import (
	"bufio"
	"errors"
	"io"
	"path/filepath"

	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
)

var WriteBufSize = 256 * 1024

type XLSXWriter struct {
	closer io.Closer
	bWr    *bufio.Writer
	info   *XLSXFileInfo
	sch    schema.Schema
}

func OpenXLSXWriter(path string, fs filesys.WritableFS, outSch schema.Schema, info *XLSXFileInfo) (*XLSXWriter, error) {
	err := fs.MkDirs(filepath.Dir(path))

	if err != nil {
		return nil, err
	}

	wr, err := fs.OpenForWrite(path)

	if err != nil {
		return nil, err
	}

	return NewXLSXWriter(wr, outSch, info)
}

func NewXLSXWriter(wr io.WriteCloser, outSch schema.Schema, info *XLSXFileInfo) (*XLSXWriter, error) {

	bwr := bufio.NewWriterSize(wr, WriteBufSize)

	return &XLSXWriter{wr, bwr, info, outSch}, nil
}

func (xlsxw *XLSXWriter) GetSchema() schema.Schema {
	return xlsxw.sch
}

func (xlsxw *XLSXWriter) WriteRow(r row.Row) error {
	allCols := xlsxw.sch.GetAllCols()

	i := 0
	colValStrs := make([]string, allCols.Size())
	var xlStr [][]string
	var rowStr [][][]string

	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		val, ok := r.GetColVal(tag)
		if ok && !types.IsNull(val) {
			if val.Kind() == types.StringKind {
				colValStrs[0] = string(val.(types.String))
			} else {
				colValStrs[0] = types.EncodedValue(val)
			}
		}

		xlStr = append(xlStr, colValStrs)
		rowStr = append(rowStr, xlStr)
		i++
		return false
	})

	err := iohelp.WritePrimIfNoErr(xlsxw.bWr, rowStr, nil)

	return err
}

// Close should flush all writes, release resources being held
func (xlsxw *XLSXWriter) Close() error {
	if xlsxw.closer != nil {
		errFl := xlsxw.bWr.Flush()
		errCl := xlsxw.closer.Close()
		xlsxw.closer = nil

		if errCl != nil {
			return errCl
		}

		return errFl
	}
	return errors.New("already closed")

}
