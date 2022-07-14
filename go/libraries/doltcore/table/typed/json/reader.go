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

package json

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/bcicen/jstream"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

var ReadBufSize = 256 * 1024

type JSONReader struct {
	vrw        types.ValueReadWriter
	closer     io.Closer
	sch        schema.Schema
	jsonStream *jstream.Decoder
	rowChan    chan *jstream.MetaValue
	sampleRow  sql.Row
}

var _ table.SqlTableReader = (*JSONReader)(nil)

func OpenJSONReader(vrw types.ValueReadWriter, path string, fs filesys.ReadableFS, sch schema.Schema) (*JSONReader, error) {
	r, err := fs.OpenForRead(path)
	if err != nil {
		return nil, err
	}

	return NewJSONReader(vrw, r, sch)
}

func NewJSONReader(vrw types.ValueReadWriter, r io.ReadCloser, sch schema.Schema) (*JSONReader, error) {
	if sch == nil {
		return nil, errors.New("schema must be provided to JsonReader")
	}

	decoder := jstream.NewDecoder(r, 2) // extract JSON values at a depth level of 1

	return &JSONReader{vrw: vrw, closer: r, sch: sch, jsonStream: decoder}, nil
}

// Close should release resources being held
func (r *JSONReader) Close(ctx context.Context) error {
	if r.closer != nil {
		err := r.closer.Close()
		r.closer = nil

		return err
	}
	return errors.New("already closed")
}

// GetSchema gets the schema of the rows that this reader will return
func (r *JSONReader) GetSchema() schema.Schema {
	return r.sch
}

// VerifySchema checks that the incoming schema matches the schema from the existing table
func (r *JSONReader) VerifySchema(sch schema.Schema) (bool, error) {
	if r.sampleRow == nil {
		var err error
		r.sampleRow, err = r.ReadSqlRow(context.Background())
		return err == nil, nil
	}
	return true, nil
}

func (r *JSONReader) ReadRow(ctx context.Context) (row.Row, error) {
	panic("deprecated")
}

func (r *JSONReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	if r.sampleRow != nil {
		ret := r.sampleRow
		r.sampleRow = nil
		return ret, nil
	}

	if r.rowChan == nil {
		r.rowChan = r.jsonStream.Stream()
	}

	metaRow, ok := <-r.rowChan
	if !ok {
		if r.jsonStream.Err() != nil {
			return nil, r.jsonStream.Err()
		}
		return nil, io.EOF
	}

	return r.convToSqlRow(metaRow.Value.(map[string]interface{}))
}

func (r *JSONReader) convToSqlRow(rowMap map[string]interface{}) (sql.Row, error) {
	allCols := r.sch.GetAllCols()

	ret := make(sql.Row, allCols.Size())
	for k, v := range rowMap {
		col, ok := allCols.GetByName(k)
		if !ok {
			return nil, fmt.Errorf("column %s not found in schema", k)
		}

		v, err := col.TypeInfo.ToSqlType().Convert(v)
		if err != nil {
			return nil, err
		}

		idx := allCols.TagToIdx[col.Tag]
		ret[idx] = v
	}

	return ret, nil
}
