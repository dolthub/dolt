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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
)

type ParquetWriter struct {
	filewriter source.ParquetFile
	pwriter    *writer.CSVWriter
	sch        schema.Schema
}

var _ table.SqlRowWriter = (*ParquetWriter)(nil)

var typeMap = map[typeinfo.Identifier]string{
	typeinfo.DatetimeTypeIdentifier:   "type=INT64, convertedtype=TIMESTAMP_MICROS",
	typeinfo.DecimalTypeIdentifier:    "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.EnumTypeIdentifier:       "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.InlineBlobTypeIdentifier: "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.SetTypeIdentifier:        "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.TimeTypeIdentifier:       "type=INT64, convertedtype=TIMESPAN",
	typeinfo.TupleTypeIdentifier:      "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.UuidTypeIdentifier:       "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.VarBinaryTypeIdentifier:  "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.YearTypeIdentifier:       "type=INT32, convertedtype=INT_32",
	typeinfo.UnknownTypeIdentifier:    "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.JSONTypeIdentifier:       "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.BlobStringTypeIdentifier: "type=BYTE_ARRAY, convertedtype=UTF8",

	typeinfo.BitTypeIdentifier:       "type=INT32, convertedtype=INT_16",
	typeinfo.BoolTypeIdentifier:      "type=BOOLEAN",
	typeinfo.VarStringTypeIdentifier: "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.UintTypeIdentifier:      "type=INT64, convertedtype=UINT_64",
	typeinfo.IntTypeIdentifier:       "type=INT64, convertedtype=INT_64",
	typeinfo.FloatTypeIdentifier:     "type=DOUBLE",
}

func NewParquetWriter(outSch schema.Schema, destName string) (*ParquetWriter, error) {
	columns := outSch.GetAllCols().GetColumns()

	var csvSchema []string
	var repetitionType string
	// creates csv schema for handling parquet format using NewCSVWriter
	for _, col := range columns {
		repetitionType = ""
		colType := col.TypeInfo.GetTypeIdentifier()
		if col.IsNullable() {
			repetitionType = ", repetitiontype=OPTIONAL"
		}
		csvSchema = append(csvSchema, fmt.Sprintf("name=%s, %s%s", col.Name, typeMap[colType], repetitionType))
	}

	fw, err := local.NewLocalFileWriter(destName)
	if err != nil {
		return nil, err
	}

	// default np (degree of concurrency) is 4 recommended from the package
	pw, err := writer.NewCSVWriter(csvSchema, fw, 4)
	if err != nil {
		return nil, err
	}

	// pw.CompressionType defaults to parquet.CompressionCodec_SNAPPY
	return &ParquetWriter{filewriter: fw, pwriter: pw, sch: outSch}, nil
}

func (pwr *ParquetWriter) GetSchema() schema.Schema {
	return pwr.sch
}

func (pwr *ParquetWriter) WriteSqlRow(ctx context.Context, r sql.Row) error {
	colValStrs := make([]*string, pwr.sch.GetAllCols().Size())

	for i, val := range r {
		colT := pwr.sch.GetAllCols().GetByIndex(i)
		if val == nil {
			colValStrs[i] = nil
		} else {
			sqlType := colT.TypeInfo.ToSqlType()
			// convert datetime and time types to int64
			switch colT.TypeInfo.GetTypeIdentifier() {
			case typeinfo.DatetimeTypeIdentifier:
				val = val.(time.Time).UnixMicro()
				sqlType = sql.Int64
			case typeinfo.TimeTypeIdentifier:
				val = int64(val.(sql.Timespan).AsTimeDuration())
				sqlType = sql.Int64
			case typeinfo.BitTypeIdentifier:
				sqlType = sql.Uint64
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
func (pwr *ParquetWriter) Close(ctx context.Context) error {
	// WriteStop writes footer, stops writing and flushes
	err := pwr.pwriter.WriteStop()
	if err != nil {
		return err
	}
	pwr.filewriter.Close()
	return nil
}
