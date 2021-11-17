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

package parquet

import (
	"context"
	"fmt"
	"io"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

var WriteBufSize = 256 * 1024

type ParquetWriter struct {
	closer  source.ParquetFile
	pwriter *writer.CSVWriter
	sch     schema.Schema
}

func NewParquetWriter(wr io.WriteCloser, outSch schema.Schema, srcName string, destName string) (*ParquetWriter, error) {
	allCols := outSch.GetAllCols()
	columns := allCols.GetColumns()

	ColTypeMap := getTypeMap()
	var csvSchema []string
	for _, col := range columns {
		colName := col.Name
		colType := col.TypeInfo.GetTypeIdentifier()
		csvSchema = append(csvSchema, fmt.Sprintf("name=%s, %s", colName, ColTypeMap[colType]))
	}

	fw, err := local.NewLocalFileWriter(destName)
	if err != nil {
		return nil, err
	}

	// np configures the degree of concurrency for our Reader and Writers
	// TODO: not sure what default value to set 'np' to
	pw, err := writer.NewCSVWriter(csvSchema, fw, 4)
	if err != nil {
		return nil, err
	}

	return &ParquetWriter{closer: fw, pwriter: pw, sch: outSch}, nil
}

func (pwr *ParquetWriter) GetSchema() schema.Schema {
	return pwr.sch
}

// WriteRow will write a row to a table
func (pwr *ParquetWriter) WriteRow(ctx context.Context, r row.Row) error {
	allCols := pwr.sch.GetAllCols()
	colValStrs := make([]*string, allCols.Size())

	sqlRow, err := sqlutil.DoltRowToSqlRow(r, pwr.GetSchema())
	if err != nil {
		return err
	}

	for i, val := range sqlRow {
		if val == nil {
			colValStrs[i] = nil
		} else {
			v := sqlutil.SqlColToStr(ctx, val)
			colValStrs[i] = &v
		}
	}

	err = pwr.pwriter.WriteString(colValStrs)
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
	pwr.closer.Close()
	return nil
}

func getTypeMap() map[typeinfo.Identifier]string {

	typeMap := map[typeinfo.Identifier]string{
		typeinfo.DatetimeTypeIdentifier:   `type=BYTE_ARRAY`,
		typeinfo.DecimalTypeIdentifier:    `type=BYTE_ARRAY`,
		typeinfo.EnumTypeIdentifier:       `type=BYTE_ARRAY`,
		typeinfo.InlineBlobTypeIdentifier: `type=BYTE_ARRAY`,
		typeinfo.SetTypeIdentifier:        `type=BYTE_ARRAY`,
		typeinfo.TimeTypeIdentifier:       `type=BYTE_ARRAY`,
		typeinfo.TupleTypeIdentifier:      `type=BYTE_ARRAY`,
		typeinfo.UuidTypeIdentifier:       `type=BYTE_ARRAY`,
		typeinfo.VarBinaryTypeIdentifier:  `type=BYTE_ARRAY`,
		typeinfo.YearTypeIdentifier:       `type=BYTE_ARRAY`,

		typeinfo.BitTypeIdentifier:       `type=BYTE_ARRAY`,
		typeinfo.BoolTypeIdentifier:      `type=BOOLEAN`,
		typeinfo.VarStringTypeIdentifier: `type=BYTE_ARRAY`,
		typeinfo.UintTypeIdentifier:      `type=INT64, convertedtype=UINT_64`,
		typeinfo.IntTypeIdentifier:       `type=INT64, convertedtype=INT_64`,
		typeinfo.FloatTypeIdentifier:     `type=DOUBLE`,
	}

	return typeMap
}
