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
	rows   []row.Row
}

func OpenXLSXReader(nbf *types.NomsBinFormat, path string, fs filesys.ReadableFS, info *XLSXFileInfo) (*XLSXReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	br := bufio.NewReaderSize(r, ReadBufSize)

	colStrs, err := getColHeaders(path, info.SheetName)

	data, err := getXlsxRows(path, info.SheetName)
	if err != nil {
		return nil, err
	}

	_, sch := untyped.NewUntypedSchema(colStrs...)

	decodedRows, err := decodeXLSXRows(nbf, data, sch)
	if err != nil {
		r.Close()
		return nil, err
	}

	return &XLSXReader{r, br, info, sch, 0, decodedRows}, nil
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

func (xlsxr *XLSXReader) VerifySchema(outSch schema.Schema) (bool, error) {
	return schema.VerifyInSchema(xlsxr.sch, outSch)
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
	if xlsxr.ind == len(xlsxr.rows) {
		return nil, io.EOF
	}

	outRow := xlsxr.rows[xlsxr.ind]
	xlsxr.ind++

	return outRow, nil
}
