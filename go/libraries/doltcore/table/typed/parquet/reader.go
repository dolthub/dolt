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
	"math/big"
"strings"
"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
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

var _ table.SqlTableReader = (*ParquetReader)(nil)

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
			return nil, fmt.Errorf("cannot read column: %s", cErr.Error())
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

// DECIMAL_BYTE_ARRAY_ToString converts a decimal byte array to a string
// This is copied from parquet-go/types package, but handles panic correctly
func DECIMAL_BYTE_ARRAY_ToString(dec []byte, prec int, scale int) string {
	a := new(big.Int)
	a.SetBytes(dec)
	sa := a.Text(10)

	if scale > 0 {
		ln := len(sa)
		off := ln - scale
		if off < 0 {
			sa = "0." + strings.Repeat("0", -off) + sa
		} else {
			sa = sa[:off] + "." + sa[off:]
		}
	}
	return sa
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
			switch col.TypeInfo.GetTypeIdentifier() {
			case typeinfo.DatetimeTypeIdentifier:
				val = time.UnixMicro(val.(int64))
			case typeinfo.TimeTypeIdentifier:
				val = gmstypes.Timespan(time.Duration(val.(int64)).Microseconds())
			}
		}

		if col.Kind == types.DecimalKind {
			valBytes := []byte(val.(string))
			prec, scale := col.TypeInfo.ToSqlType().(gmstypes.DecimalType_).Precision(), col.TypeInfo.ToSqlType().(gmstypes.DecimalType_).Scale()
			val = DECIMAL_BYTE_ARRAY_ToString(valBytes, int(prec), int(scale))
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
