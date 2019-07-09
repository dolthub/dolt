package xlsx

import (
	"bufio"
	"context"
	"errors"
	"io"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

var ReadBufSize = 256 * 1024

type XLSXReader struct {
	closer io.Closer
	bRd    *bufio.Reader
	info   *XLSXFileInfo
	sch    schema.Schema
	ind    int
}

func OpenXLSXReader(path string, fs filesys.ReadableFS, info *XLSXFileInfo, format *types.Format, tblName string) (*XLSXReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	return NewXLSXReader(r, info, fs, format, path, tblName)
}

func NewXLSXReader(r io.ReadCloser, info *XLSXFileInfo, fs filesys.ReadableFS, format *types.Format, path string, tblName string) (*XLSXReader, error) {
	br := bufio.NewReaderSize(r, ReadBufSize)
	colStrs, err := getColHeaders(path, tblName)

	if err != nil {
		return nil, err
	}

	data, err := getXlsxRows(path, tblName)
	if err != nil {
		return nil, err
	}

	_, sch := untyped.NewUntypedSchema(colStrs...)

	decodedRows, err := decodeXLSXRows(format, data, sch)
	if err != nil {
		return nil, err
	}
	info.SetRows(decodedRows)

	if err != nil {
		r.Close()
		return nil, err
	}

	return &XLSXReader{r, br, info, sch, 0}, nil
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
func (xlsxr *XLSXReader) Close(ctx context.Context) error {
	if xlsxr.closer != nil {
		err := xlsxr.closer.Close()
		xlsxr.closer = nil

		return err
	} else {
		return errors.New("Already closed.")
	}
}

func (xlsxr *XLSXReader) ReadRow(ctx context.Context) (row.Row, error) {
	rows := xlsxr.info.Rows

	if xlsxr.ind == len(rows) {
		return nil, io.EOF
	}

	outRow := rows[xlsxr.ind]
	xlsxr.ind++

	return outRow, nil
}
