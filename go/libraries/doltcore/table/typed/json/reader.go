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

package json

import (
	"bufio"
	"context"
	"errors"
	"io"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

var ReadBufSize = 256 * 1024

type JSONReader struct {
	closer io.Closer
	bRd    *bufio.Reader
	info   *JSONFileInfo
	sch    schema.Schema
	ind    int
}

func OpenJSONReader(nbf *types.NomsBinFormat, path string, fs filesys.ReadableFS, info *JSONFileInfo, schPath string) (*JSONReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	return NewJSONReader(nbf, r, info, fs, schPath, path)
}

func NewJSONReader(nbf *types.NomsBinFormat, r io.ReadCloser, info *JSONFileInfo, fs filesys.ReadableFS, schPath string, tblPath string) (*JSONReader, error) {
	br := bufio.NewReaderSize(r, ReadBufSize)
	if schPath == "" {
		panic("schema must be provided")
	}

	schData, err := fs.ReadFile(schPath)
	if err != nil {
		return nil, err
	}

	jsonSchStr := string(schData)
	sch, err := encoding.UnmarshalJson(jsonSchStr)
	if err != nil {
		return nil, err
	}

	tblData, err := fs.ReadFile(tblPath)
	if err != nil {
		return nil, err
	}

	jsonRows, err := UnmarshalFromJSON(tblData)
	if err != nil {
		return nil, err
	}

	decodedRows, err := jsonRows.decodeJSONRows(nbf, sch)
	info.SetRows(decodedRows)

	return &JSONReader{r, br, info, sch, 0}, nil

}

// Close should release resources being held
func (jsonr *JSONReader) Close(ctx context.Context) error {
	if jsonr.closer != nil {
		err := jsonr.closer.Close()
		jsonr.closer = nil

		return err
	}
	return errors.New("already closed")

}

func (jsonr *JSONReader) GetSchema() schema.Schema {
	return jsonr.sch

}

func (jsonr *JSONReader) ReadRow(ctx context.Context) (row.Row, error) {
	rows := jsonr.info.Rows

	if jsonr.ind == len(rows) {
		return nil, io.EOF
	}

	outRow := rows[jsonr.ind]
	jsonr.ind++

	return outRow, nil
}
