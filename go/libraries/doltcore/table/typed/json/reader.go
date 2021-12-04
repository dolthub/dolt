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
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var ReadBufSize = 256 * 1024

type JSONReader struct {
	closer     io.Closer
	sch        sql.Schema
	jsonStream *jstream.Decoder
	rowChan    chan *jstream.MetaValue
}

func OpenJSONReader(path string, fs filesys.ReadableFS, sch sql.Schema) (*JSONReader, error) {
	r, err := fs.OpenForRead(path)
	if err != nil {
		return nil, err
	}

	return NewJSONReader(r, sch)
}

// TODO: Deprecate the use of schema.Schema from file data loc
func NewJSONReader(r io.ReadCloser, sch sql.Schema) (*JSONReader, error) {
	if sch == nil {
		return nil, errors.New("schema must be provided to JsonReader")
	}

	decoder := jstream.NewDecoder(r, 2) // extract JSON values at a depth level of 1 }

	return &JSONReader{closer: r, sch: sch, jsonStream: decoder}, nil
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
	panic("deprecated")
}

func (r *JSONReader) ReadRow(ctx context.Context) (row.Row, error) {
	panic("deprecated")
}

func (r *JSONReader) GetSqlSchema() sql.Schema {
	return r.sch
}

func (r *JSONReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
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
	sqlSchema := r.GetSqlSchema()
	ret := make(sql.Row, len(sqlSchema))

	for k, v := range rowMap {
		idx := sqlSchema.IndexOf(k, sqlSchema[0].Source)
		if idx < 0 {
			return nil, fmt.Errorf("column %s not found in schema", k)
		}

		v, err := sqlSchema[idx].Type.Convert(v)
		if err != nil {
			return nil, err
		}

		ret[idx] = v
	}

	return ret, nil
}
