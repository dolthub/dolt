// Copyright 2021 Dolthub, Inc.
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

package parquet

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
)

type ParquetRowWriter struct {
	pwriter *writer.CSVWriter
	sch     sql.Schema
	closer  io.Closer
}

var _ table.SqlRowWriter = (*ParquetRowWriter)(nil)

// NewParquetRowWriter creates a new ParquetRowWriter instance for the specified schema and
// writing to the specified WriteCloser.
func NewParquetRowWriter(outSch sql.Schema, w io.WriteCloser) (*ParquetRowWriter, error) {
	var csvSchema []string
	var repetitionType string
	// creates csv schema for handling parquet format using NewCSVWriter
	for _, col := range outSch {
		repetitionType = ""
		colType := col.Type
		if col.Nullable {
			repetitionType = ", repetitiontype=OPTIONAL"
		}
		mappedType, err := mapTypeToParquetTypeDescription(colType, colType.Type())
		if err != nil {
			return nil, err
		}
		csvSchema = append(csvSchema, fmt.Sprintf("name=%s, %s%s", col.Name, mappedType, repetitionType))
	}

	// default np (degree of concurrency) is 4 recommended from the package
	pw, err := writer.NewCSVWriterFromWriter(csvSchema, w, 4)
	if err != nil {
		return nil, err
	}

	// pw.CompressionType defaults to parquet.CompressionCodec_SNAPPY
	return &ParquetRowWriter{pwriter: pw, sch: outSch, closer: w}, nil
}

// NewParquetRowWriterForFile creates a new ParquetRowWriter instance for the specified schema and
// writing to the specified file name.
func NewParquetRowWriterForFile(outSch schema.Schema, destName string) (*ParquetRowWriter, error) {
	primaryKeySchema, err := sqlutil.FromDoltSchema("", "", outSch)
	if err != nil {
		return nil, err
	}

	fw, err := local.NewLocalFileWriter(destName)
	if err != nil {
		return nil, err
	}

	return NewParquetRowWriter(primaryKeySchema.Schema, fw)
}

func (pwr *ParquetRowWriter) WriteSqlRow(_ context.Context, r sql.Row) error {
	colValStrs := make([]*string, len(pwr.sch))

	for i, val := range r {
		colT := pwr.sch[i]
		if val == nil {
			colValStrs[i] = nil
		} else {
			sqlType := colT.Type
			// convert datetime and time types to int64
			switch sqlType.Type() {
			case query.Type_DATETIME, query.Type_DATE, query.Type_TIMESTAMP:
				val = val.(time.Time).UnixMicro()
				sqlType = types.Int64
			case query.Type_TIME:
				val = int64(val.(types.Timespan).AsTimeDuration())
				sqlType = types.Int64
			case query.Type_BIT:
				sqlType = types.Uint64
			//case query.Type_DECIMAL:
			//	decVal := val.(decimal.Decimal)
			//	decVal.Shift(-decVal.Exponent())
			//	prec, scale := sqlType.(types.DecimalType_).Precision(), sqlType.(types.DecimalType_).Scale()
			//	val = parquettypes.DECIMAL_BYTE_ARRAY_ToString(decVal.BigInt().Bytes(), int(prec), int(scale))
			//	sqlType = types.Text
			}
			v, err := sqlutil.SqlColToStr(sqlType, val)
			if err != nil {
				return err
			}
			colValStrs[i] = &v
		}
	}

	err := pwr.pwriter.WriteString(colValStrs)
	if err != nil {
		return err
	}
	return nil
}

// Close should flush all writes, release resources being held
func (pwr *ParquetRowWriter) Close(_ context.Context) error {
	// WriteStop writes footer, stops writing and flushes
	err := pwr.pwriter.WriteStop()
	if err != nil {
		return err
	}

	if pwr.closer != nil {
		return pwr.closer.Close()
	}

	return nil
}

// mapTypeToParquetTypeDescription maps |qt| from a query.Type to a text description of the type for Parquet.
func mapTypeToParquetTypeDescription(t sql.Type, qt query.Type) (string, error) {
	switch qt {
	case query.Type_DATETIME, query.Type_DATE, query.Type_TIMESTAMP:
		return "type=INT64, convertedtype=TIMESTAMP_MICROS", nil
	case query.Type_YEAR:
		return "type=INT32, convertedtype=INT_32", nil
	case query.Type_TIME:
		return "type=INT64, convertedtype=TIMESPAN", nil
	case query.Type_DECIMAL:
		dt := t.(types.DecimalType_)
		return fmt.Sprintf("type=BYTE_ARRAY, convertedtype=DECIMAL, precision=%d, scale=%d", dt.Precision(), dt.Scale()), nil
	case query.Type_ENUM, query.Type_SET, query.Type_BLOB, query.Type_TUPLE, query.Type_VARBINARY,
		query.Type_JSON, query.Type_CHAR, query.Type_VARCHAR, query.Type_TEXT, query.Type_BINARY, query.Type_GEOMETRY:
		return "type=BYTE_ARRAY, convertedtype=UTF8", nil
	case query.Type_FLOAT32, query.Type_FLOAT64:
		return "type=DOUBLE", nil
	case query.Type_INT8, query.Type_INT16, query.Type_INT24, query.Type_INT32, query.Type_INT64:
		return "type=INT64, convertedtype=INT_64", nil
	case query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64:
		return "type=INT64, convertedtype=UINT_64", nil
	case query.Type_BIT:
		return "type=INT32, convertedtype=INT_16", nil
	default:
		return "", fmt.Errorf("unsupported type: %v", qt)
	}
}
