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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
)

const jsonHeader = `{"rows": [`
const jsonFooter = `]}`

var WriteBufSize = 256 * 1024
var defaultString = sql.MustCreateStringWithDefaults(sqltypes.VarChar, 16383)

type JSONWriter struct {
	closer      io.Closer
	bWr         *bufio.Writer
	sch         schema.Schema
	rowsWritten int
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
	if err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		val, ok := r.GetColVal(tag)
		if !ok || types.IsNull(val) {
			return false, nil
		}

		switch col.TypeInfo.GetTypeIdentifier() {
		case typeinfo.DatetimeTypeIdentifier,
			typeinfo.DecimalTypeIdentifier,
			typeinfo.EnumTypeIdentifier,
			typeinfo.InlineBlobTypeIdentifier,
			typeinfo.SetTypeIdentifier,
			typeinfo.TimeTypeIdentifier,
			typeinfo.TupleTypeIdentifier,
			typeinfo.UuidTypeIdentifier,
			typeinfo.VarBinaryTypeIdentifier,
			typeinfo.YearTypeIdentifier:
			v, err := col.TypeInfo.FormatValue(val)
			if err != nil {
				return true, err
			}
			val = types.String(*v)

		case typeinfo.BitTypeIdentifier,
			typeinfo.BoolTypeIdentifier,
			typeinfo.VarStringTypeIdentifier,
			typeinfo.UintTypeIdentifier,
			typeinfo.IntTypeIdentifier,
			typeinfo.FloatTypeIdentifier:
			// use primitive type
		}

		colValMap[col.Name] = val

		return false, nil
	}); err != nil {
		return err
	}

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

func (jsonw *JSONWriter) WriteSqlRow(ctx context.Context, row sql.Row) error {
	allCols := jsonw.sch.GetAllCols()
	colValMap := make(map[string]interface{}, allCols.Size())
	if err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		val := row[allCols.TagToIdx[tag]]
		if val == nil {
			return false, nil
		}

		switch col.TypeInfo.GetTypeIdentifier() {
		case typeinfo.DecimalTypeIdentifier,
			typeinfo.EnumTypeIdentifier,
			typeinfo.InlineBlobTypeIdentifier,
			typeinfo.SetTypeIdentifier,
			typeinfo.TimeTypeIdentifier,
			typeinfo.TupleTypeIdentifier,
			typeinfo.UuidTypeIdentifier,
			typeinfo.VarBinaryTypeIdentifier,
			typeinfo.YearTypeIdentifier:
			val, err = defaultString.Convert(val)
			if err != nil {
				return true, err
			}
		case typeinfo.DatetimeTypeIdentifier:
			val, err = col.TypeInfo.FormatValue(types.Timestamp(val.(time.Time)))
			if err != nil {
				return true, err
			}

		case typeinfo.BitTypeIdentifier,
			typeinfo.BoolTypeIdentifier,
			typeinfo.VarStringTypeIdentifier,
			typeinfo.UintTypeIdentifier,
			typeinfo.IntTypeIdentifier,
			typeinfo.FloatTypeIdentifier:
			// use primitive type
		}

		colValMap[col.Name] = val

		return false, nil
	}); err != nil {
		return err
	}

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
