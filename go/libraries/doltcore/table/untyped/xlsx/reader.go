// Copyright 2019 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

var ReadBufSize = 256 * 1024

type XLSXReader struct {
	closer io.Closer
	bRd    *bufio.Reader
	info   *XLSXFileInfo
	sch    schema.Schema
	ind    int
	rows   []sql.Row
	vrw    types.ValueReadWriter
}

func OpenXLSXReaderFromBinary(ctx context.Context, vrw types.ValueReadWriter, r io.ReadCloser, info *XLSXFileInfo) (*XLSXReader, error) {
	br := bufio.NewReaderSize(r, ReadBufSize)

	contents, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	colStrs, err := getColHeadersFromBinary(contents, info.SheetName)
	if err != nil {
		return nil, err
	}

	data, err := getXlsxRowsFromBinary(contents, info.SheetName)
	if err != nil {
		return nil, err
	}

	_, sch := untyped.NewUntypedSchema(colStrs...)

	decodedRows, err := decodeXLSXRows(data, sch)
	if err != nil {
		r.Close()
		return nil, err
	}

	return &XLSXReader{r, br, info, sch, 0, decodedRows, vrw}, nil
}

func OpenXLSXReader(ctx context.Context, vrw types.ValueReadWriter, path string, fs filesys.ReadableFS, info *XLSXFileInfo) (*XLSXReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	br := bufio.NewReaderSize(r, ReadBufSize)

	colStrs, err := getColHeadersFromPath(path, info.SheetName)
	if err != nil {
		return nil, err
	}

	data, err := getXlsxRowsFromPath(path, info.SheetName)
	if err != nil {
		return nil, err
	}

	_, sch := untyped.NewUntypedSchema(colStrs...)

	decodedRows, err := decodeXLSXRows(data, sch)
	if err != nil {
		r.Close()
		return nil, err
	}

	return &XLSXReader{r, br, info, sch, 0, decodedRows, vrw}, nil
}

func getColHeadersFromPath(path string, sheetName string) ([]string, error) {
	data, err := getXlsxRowsFromPath(path, sheetName)
	if err != nil {
		return nil, err
	}

	colHeaders := data[0][0]
	return colHeaders, nil
}

func getColHeadersFromBinary(content []byte, sheetName string) ([]string, error) {
	data, err := getXlsxRowsFromBinary(content, sheetName)
	if err != nil {
		return nil, err
	}

	colHeaders := data[0][0]
	return colHeaders, nil
}

// GetSchema gets the schema of the rows that this reader will return
func (xlsxr *XLSXReader) GetSchema() schema.Schema {
	return xlsxr.sch
}

// VerifySchema checks that the incoming schema matches the schema from the existing table
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

	allCols := xlsxr.sch.GetAllCols()
	taggedVals := make(row.TaggedValues, allCols.Size())
	sqlRow := xlsxr.rows[xlsxr.ind]

	allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		taggedVals[tag], err = col.TypeInfo.ConvertValueToNomsValue(ctx, xlsxr.vrw, sqlRow[allCols.TagToIdx[tag]])
		return false, err
	})

	r, err := row.New(xlsxr.vrw.Format(), xlsxr.sch, taggedVals)
	if err != nil {
		return nil, err
	}

	xlsxr.ind++

	return r, nil
}

func (xlsxr *XLSXReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	if xlsxr.ind == len(xlsxr.rows) {
		return nil, io.EOF
	}

	outRow := xlsxr.rows[xlsxr.ind]
	xlsxr.ind++

	return outRow, nil
}
