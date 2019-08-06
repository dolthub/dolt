// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package xlsx

import (
	"bufio"
	"context"
	"errors"
	"io"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var ReadBufSize = 256 * 1024

type XLSXReader struct {
	closer io.Closer
	bRd    *bufio.Reader
	info   *XLSXFileInfo
	sch    schema.Schema
	ind    int
}

func OpenXLSXReader(nbf *types.NomsBinFormat, path string, fs filesys.ReadableFS, info *XLSXFileInfo, tblName string) (*XLSXReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	return NewXLSXReader(nbf, r, info, fs, path, tblName)
}

func NewXLSXReader(nbf *types.NomsBinFormat, r io.ReadCloser, info *XLSXFileInfo, fs filesys.ReadableFS, path string, tblName string) (*XLSXReader, error) {
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

	decodedRows, err := decodeXLSXRows(nbf, data, sch)
	if err != nil {
		r.Close()
		return nil, err
	}

	info.SetRows(decodedRows)

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
