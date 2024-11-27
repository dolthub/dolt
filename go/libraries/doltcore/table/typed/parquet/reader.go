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

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
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

// DecimalByteArrayToString converts a decimal byte array to a string
// This is copied from https://github.com/xitongsys/parquet-go/blob/master/types/converter.go
// while we wait for official release
func DecimalByteArrayToString(dec []byte, prec int, scale int) string {
	sign := ""
	if dec[0] > 0x7f {
		sign = "-"
		for i := range dec {
			dec[i] = dec[i] ^ 0xff
		}
	}
	a := new(big.Int)
	a.SetBytes(dec)
	if sign == "-" {
		a = a.Add(a, big.NewInt(1))
	}
	sa := a.Text(10)

	if scale > 0 {
		ln := len(sa)
		if ln < scale+1 {
			sa = strings.Repeat("0", scale+1-ln) + sa
			ln = scale + 1
		}
		sa = sa[:ln-scale] + "." + sa[ln-scale:]
	}
	return sign + sa
}

func (pr *ParquetReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	if pr.rowReadCounter >= pr.numRow {
		return nil, io.EOF
	}

	allCols := pr.sch.GetAllCols()
	row := make(sql.UntypedSqlRow, allCols.Size())
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
			prec, scale := col.TypeInfo.ToSqlType().(gmstypes.DecimalType_).Precision(), col.TypeInfo.ToSqlType().(gmstypes.DecimalType_).Scale()
			val = DecimalByteArrayToString([]byte(val.(string)), int(prec), int(scale))
		}

		row.SetValue(allCols.TagToIdx[tag], val)

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
