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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

const jsonHeader = `{"rows": [`
const jsonFooter = `]}`

var WriteBufSize = 256 * 1024
var defaultString = types.MustCreateStringWithDefaults(sqltypes.VarChar, 16383)

type RowWriter struct {
	closer      io.Closer
	header      string
	footer      string
	separator   string
	bWr         *bufio.Writer
	sch         schema.Schema
	rowsWritten int
}

var _ table.SqlRowWriter = (*RowWriter)(nil)

// NewJSONWriter returns a new writer that encodes rows as a single JSON object with a single key: "rows", which is a
// slice of all rows. To customize the output of the JSON object emitted, use |NewJSONWriterWithHeader|
func NewJSONWriter(wr io.WriteCloser, outSch schema.Schema) (*RowWriter, error) {
	return NewJSONWriterWithHeader(wr, outSch, jsonHeader, jsonFooter, ",")
}

func NewJSONWriterWithHeader(wr io.WriteCloser, outSch schema.Schema, header, footer, separator string) (*RowWriter, error) {
	bwr := bufio.NewWriterSize(wr, WriteBufSize)
	return &RowWriter{
		closer:    wr,
		bWr:       bwr,
		sch:       outSch,
		header:    header,
		footer:    footer,
		separator: separator,
	}, nil
}

func (j *RowWriter) GetSchema() schema.Schema {
	return j.sch
}

func (j *RowWriter) WriteSqlRow(ctx context.Context, row sql.Row) error {
	// The Type.SQL() call takes in a SQL context to determine the output character set for types that use a collation.
	// The context given is not a SQL context, so we force the `utf8mb4` character set to be used, as it is the most
	// likely to be supported by the destination. `utf8mb4` is the default character set for empty SQL contexts, so we
	// don't need to explicitly set it.
	sqlContext := sql.NewEmptyContext()

	if j.rowsWritten == 0 {
		err := iohelp.WriteAll(j.bWr, []byte(j.header))
		if err != nil {
			return err
		}
	}

	allCols := j.sch.GetAllCols()
	colValMap := make(map[string]interface{}, allCols.Size())
	if err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		val := row[allCols.TagToIdx[tag]]
		if val == nil {
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
			typeinfo.VarBinaryTypeIdentifier:
			sqlVal, err := col.TypeInfo.ToSqlType().SQL(sqlContext, nil, val)
			if err != nil {
				return true, err
			}
			val = sqlVal.ToString()
		case typeinfo.JSONTypeIdentifier:
			sqlVal, err := col.TypeInfo.ToSqlType().SQL(sqlContext, nil, val)
			if err != nil {
				return true, err
			}
			str := sqlVal.ToString()

			// This is kind of silly: we are unmarshalling JSON just to marshall it back again
			// But it makes marshalling much simpler
			err = json.Unmarshal([]byte(str), &val)
			if err != nil {
				return false, err
			}
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

	if j.rowsWritten != 0 {
		_, err := j.bWr.WriteString(j.separator)
		if err != nil {
			return err
		}
	}

	newErr := iohelp.WriteAll(j.bWr, data)
	if newErr != nil {
		return newErr
	}
	j.rowsWritten++

	return nil
}

func (j *RowWriter) Flush() error {
	return j.bWr.Flush()
}

// Close should flush all writes, release resources being held
func (j *RowWriter) Close(ctx context.Context) error {
	if j.closer != nil {
		if j.rowsWritten > 0 {
			err := iohelp.WriteAll(j.bWr, []byte(j.footer))
			if err != nil {
				return err
			}
		}

		errFl := j.bWr.Flush()
		errCl := j.closer.Close()
		j.closer = nil

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
