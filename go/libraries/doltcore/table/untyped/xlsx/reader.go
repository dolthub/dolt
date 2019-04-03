package xlsx

import (
	"bufio"
	"errors"
	"io"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
)

var ReadBufSize = 256 * 1024

type XLSXReader struct {
	closer io.Closer
	bRd    *bufio.Reader
	info   *XLSXFileInfo
	sch    schema.Schema
	ind    int
}

func OpenXLSXReader(path string, fs filesys.ReadableFS, info *XLSXFileInfo, tblName string) (*XLSXReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	return NewXLSXReader(r, info, fs, path, tblName), nil
}

func NewXLSXReader(r io.ReadCloser, info *XLSXFileInfo, fs filesys.ReadableFS, path string, tblName string) *XLSXReader {
	br := bufio.NewReaderSize(r, ReadBufSize)
	colStrs, err := getColHeaders(path, tblName)
	data, _ := getXlsxRows(path, tblName)
	_, sch := untyped.NewUntypedSchema(colStrs...)

	decodedRows, _ := decodeXLSXRows(data, sch)
	info.SetRows(decodedRows)

	if err != nil {
		r.Close()
		return nil
	}

	return &XLSXReader{r, br, info, sch, 0}
}

func getColHeaders(path string, sheetName string) ([]string, error) {
	data, err := getXlsxRows(path, sheetName)
	if err != nil {
		return nil, err
	}

	colHeaders := data[0][0]
	return colHeaders, nil
}

func (xlsxr *XLSXReader) GetSchema() schema.Schema {
	return xlsxr.sch
}

// Close should release resources being held
func (xlsxr *XLSXReader) Close() error {
	if xlsxr.closer != nil {
		err := xlsxr.closer.Close()
		xlsxr.closer = nil

		return err
	} else {
		return errors.New("Already closed.")
	}
}

func (xlsxr *XLSXReader) ReadRow() (row.Row, error) {
	rows := xlsxr.info.Rows

	if xlsxr.ind == len(rows) {
		return nil, io.EOF
	}

	outRow := rows[xlsxr.ind]
	xlsxr.ind++

	return outRow, nil
}
