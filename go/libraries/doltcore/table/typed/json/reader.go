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
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/bcicen/jstream"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var ReadBufSize = 256 * 1024

type JSONReader struct {
	nbf        *types.NomsBinFormat
	closer     io.Closer
	sch        schema.Schema
	jsonStream *jstream.Decoder
	rowChan    chan *jstream.MetaValue
	sampleRow  row.Row
}

func OpenJSONReader(nbf *types.NomsBinFormat, path string, fs filesys.ReadableFS, sch schema.Schema, schPath string) (*JSONReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	return newJsonReader(nbf, r, fs, sch, schPath, path)
}

func newJsonReader(nbf *types.NomsBinFormat, r io.ReadCloser, fs filesys.ReadableFS, sch schema.Schema, schPath string, tblPath string) (*JSONReader, error) {
	if sch == nil {
		if schPath == "" {
			return nil, errors.New("schema must be provided")
		}

		schData, err := fs.ReadFile(schPath)
		if err != nil {
			return nil, err
		}

		jsonSchStr := string(schData)
		sch, err = encoding.UnmarshalJson(jsonSchStr)
		if err != nil {
			return nil, err
		}
	}

	tblData, err := fs.OpenForRead(tblPath)
	if err != nil {
		return nil, err
	}

	decoder := jstream.NewDecoder(tblData, 2) // extract JSON values at a depth level of 1

	return &JSONReader{nbf: nbf, closer: r, sch: sch, jsonStream: decoder}, nil
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
		r.sampleRow, err = r.ReadRow(context.Background())
		return err == nil, nil
	}
	return true, nil
}

func (r *JSONReader) ReadRow(ctx context.Context) (row.Row, error) {
	if r.jsonStream.Err() != nil {
		return nil, r.jsonStream.Err()
	}

	if r.sampleRow != nil {
		ret := r.sampleRow
		r.sampleRow = nil
		return ret, nil
	}

	if r.rowChan == nil {
		r.rowChan = r.jsonStream.Stream()
	}

	row, ok := <-r.rowChan
	if !ok {
		if r.jsonStream.Err() != nil {
			return nil, r.jsonStream.Err()
		}
		return nil, io.EOF
	}

	m, ok := row.Value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Unexpected json value: %v", row.Value)
	}
	return r.convToRow(m)
}

func (r *JSONReader) convToRow(rowMap map[string]interface{}) (row.Row, error) {
	allCols := r.sch.GetAllCols()

	taggedVals := make(row.TaggedValues, 1)

	for k, v := range rowMap {
		col, ok := allCols.GetByName(k)
		if !ok {
			return nil, fmt.Errorf("column %s not found in schema", k)
		}

		switch v.(type) {
		case int, string, bool, float64:
			taggedVals[col.Tag], _ = col.TypeInfo.ConvertValueToNomsValue(v)
		}

	}

	err := r.sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if val, ok := taggedVals.Get(tag); !col.IsNullable() && (!ok || types.IsNull(val)) {
			return true, fmt.Errorf("column `%s` does not allow null values", col.Name)
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	return row.New(r.nbf, r.sch, taggedVals)
}
