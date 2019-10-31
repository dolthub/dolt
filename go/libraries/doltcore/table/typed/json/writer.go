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
	"encoding/json"
	"errors"
	"io"
	"path/filepath"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const jsonHeader = `{"rows": [`
const jsonFooter = `]}`

var WriteBufSize = 256 * 1024

type JSONWriter struct {
	closer      io.Closer
	bWr         *bufio.Writer
	sch         schema.Schema
	rowsWritten int
}

func OpenJSONWriter(path string, fs filesys.WritableFS, outSch schema.Schema) (*JSONWriter, error) {
	err := fs.MkDirs(filepath.Dir(path))

	if err != nil {
		return nil, err
	}

	wr, err := fs.OpenForWrite(path)

	if err != nil {
		return nil, err
	}

	return NewJSONWriter(wr, outSch)
}

func NewJSONWriter(wr io.WriteCloser, outSch schema.Schema) (*JSONWriter, error) {
	bwr := bufio.NewWriterSize(wr, WriteBufSize)
	err := iohelp.WriteAll(bwr, []byte(jsonHeader))
	if err != nil {
		return nil, err
	}
	return &JSONWriter{closer: wr, bWr: bwr, sch: outSch}, nil
}

func (jsonw *JSONWriter) GetSchema() schema.Schema {
	return jsonw.sch
}

// WriteRow will write a row to a table
func (jsonw *JSONWriter) WriteRow(ctx context.Context, r row.Row) error {
	allCols := jsonw.sch.GetAllCols()
	colValMap := make(map[string]interface{}, allCols.Size())
	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		val, ok := r.GetColVal(tag)
		if ok && !types.IsNull(val) {
			colValMap[col.Name] = val
		}

		return false, nil
	})

	data, err := marshalToJson(colValMap)
	if err != nil {
		return errors.New("marshaling did not work")
	}

	if jsonw.rowsWritten != 0 {
		_, err := jsonw.bWr.WriteRune(',')

		if err != nil {
			return err
		}
	}

	newErr := iohelp.WriteAll(jsonw.bWr, data)
	if newErr != nil {
		return newErr
	}
	jsonw.rowsWritten++

	return nil
}

// Close should flush all writes, release resources being held
func (jsonw *JSONWriter) Close(ctx context.Context) error {
	if jsonw.closer != nil {
		err := iohelp.WriteAll(jsonw.bWr, []byte(jsonFooter))

		if err != nil {
			return err
		}

		errFl := jsonw.bWr.Flush()
		errCl := jsonw.closer.Close()
		jsonw.closer = nil

		if errCl != nil {
			return errCl
		}

		return errFl
	}
	return errors.New("already closed")

}

func marshalToJson(valMap interface{}) ([]byte, error) {
	var jsonBytes []byte
	var err error

	jsonBytes, err = json.Marshal(valMap)
	if err != nil {
		return nil, err
	}
	return jsonBytes, nil
}
