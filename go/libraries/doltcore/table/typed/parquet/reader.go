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
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// ParquetReader implements TableReader.  It reads parquet files and returns rows.
type ParquetReader struct {
	fileReader     source.ParquetFile
	pReader        *reader.ParquetReader
	sch            schema.Schema
	vrw            types.ValueReadWriter
	numRow         int
	rowReadCounter int
	fileData       map[string][]interface{}
	columnName     []string
}

// OpenParquetReader opens a reader at a given path within local filesystem.
func OpenParquetReader(vrw types.ValueReadWriter, path string, sch schema.Schema) (*ParquetReader, error) {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		return nil, err
	}

	return NewParquetReader(vrw, fr, sch)
}

// NewParquetReader creates a ParquetReader from a given fileReader.
// The ParquetFileInfo should describe the parquet file being read.
func NewParquetReader(vrw types.ValueReadWriter, fr source.ParquetFile, sche schema.Schema) (*ParquetReader, error) {
	pr, err := reader.NewParquetColumnReader(fr, 4)
	if err != nil {
		return nil, err
	}

	columns := sche.GetAllCols().GetColumns()
	num := pr.GetNumRows()

	// TODO : need to solve for getting single row data in readRow (storing all columns data in memory right now)
	data := make(map[string][]interface{})
	var colName []string
	for _, col := range columns {
		colData, _, _, cErr := pr.ReadColumnByPath(common.ReformPathStr(fmt.Sprintf("parquet_go_root.%s", col.Name)), num)
		if cErr != nil {
			return nil, err
		}
		data[col.Name] = colData
		colName = append(colName, col.Name)
	}

	return &ParquetReader{
		fileReader:     fr,
		pReader:        pr,
		sch:            sche,
		vrw:            vrw,
		numRow:         int(num),
		rowReadCounter: 0,
		fileData:       data,
		columnName:     colName,
	}, nil
}

func (pr *ParquetReader) ReadRow(ctx context.Context) (row.Row, error) {
	panic("deprecated")
}

func (pr *ParquetReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	if pr.rowReadCounter >= pr.numRow {
		return nil, io.EOF
	}

	allCols := pr.sch.GetAllCols()
	row := make(sql.Row, allCols.Size())
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		val := pr.fileData[col.Name][pr.rowReadCounter]
		if val != nil {
			sqlType := col.TypeInfo.ToSqlType()
			if _, ok := sqlType.(sql.DatetimeType); ok {
				val = time.Unix(val.(int64), 0)
			} else if _, ok := sqlType.(sql.TimeType); ok {
				val = sql.Time.Unmarshal(val.(int64))
			}
		}

		row[allCols.TagToIdx[tag]] = val

		return false, nil
	})

	pr.rowReadCounter++

	return row, nil
}

func (pr *ParquetReader) GetSchema() schema.Schema {
	return pr.sch
}

// Close should release resources being held
func (pr *ParquetReader) Close(ctx context.Context) error {
	pr.pReader.ReadStop()
	pr.fileReader.Close()
	return nil
}

func (r *ParquetReader) convToRow(ctx context.Context, rowMap map[string]interface{}) (row.Row, error) {
	allCols := r.sch.GetAllCols()

	taggedVals := make(row.TaggedValues, allCols.Size())
	for k, v := range rowMap {
		col, ok := allCols.GetByName(k)
		if !ok {
			return nil, fmt.Errorf("column %s not found in schema", k)
		}

		taggedVals[col.Tag], _ = col.TypeInfo.ConvertValueToNomsValue(ctx, r.vrw, v)

	}

	// todo: move null value checks to pipeline
	err := r.sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if val, ok := taggedVals.Get(tag); !col.IsNullable() && (!ok || types.IsNull(val)) {
			return true, fmt.Errorf("column `%s` does not allow null values", col.Name)
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	return row.New(r.vrw.Format(), r.sch, taggedVals)
}
