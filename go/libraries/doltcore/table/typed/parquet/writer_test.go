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
	"path"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	nameColName  = "name"
	ageColName   = "age"
	titleColName = "title"
	nameColTag   = 0
	ageColTag    = 1
	titleColTag  = 2
)

var inCols = []schema.Column{
	{Name: nameColName, Tag: nameColTag, Kind: types.StringKind, IsPartOfPK: true, Constraints: nil, TypeInfo: typeinfo.StringDefaultType},
	{Name: ageColName, Tag: ageColTag, Kind: types.UintKind, IsPartOfPK: false, Constraints: nil, TypeInfo: typeinfo.Uint64Type},
	{Name: titleColName, Tag: titleColTag, Kind: types.StringKind, IsPartOfPK: false, Constraints: nil, TypeInfo: typeinfo.StringDefaultType},
}
var colColl = schema.NewColCollection(inCols...)
var rowSch = schema.MustSchemaFromCols(colColl)

type Person struct {
	Name  string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Age   int64  `parquet:"name=age, type=INT64, repetitiontype=OPTIONAL"`
	Title string `parquet:"name=title, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
}

func getSampleRows() []sql.Row {
	return []sql.Row{
		{"Bill Billerson", 32, "Senior Dufus"},
		{"Rob Robertson", 25, "Dufus"},
		{"John Johnson", 21, ""},
		{"Andy Anderson", 27, nil},
	}
}

func writeToParquet(pWr *ParquetRowWriter, rows []sql.Row, t *testing.T) {
	func() {
		defer func() {
			err := pWr.Close(context.Background())
			require.NoError(t, err)
		}()

		for _, row := range rows {
			err := pWr.WriteSqlRow(context.Background(), row)
			if err != nil {
				t.Fatal("Failed to write row")
			}
		}
	}()
}

func TestWriter(t *testing.T) {
	const expected = `Bill Billerson,32,Senior Dufus
Rob Robertson,25,Dufus
John Johnson,21,
Andy Anderson,27,
`

	path := path.Join(t.TempDir(), "parquet")

	rows := getSampleRows()

	pWr, err := NewParquetRowWriterForFile(rowSch, path)
	if err != nil {
		require.NoError(t, err)
	}

	writeToParquet(pWr, rows, t)

	pRd, err := local.NewLocalFileReader(path)
	if err != nil {
		require.NoError(t, err)
	}
	defer func() {
		err = pRd.Close()
		require.NoError(t, err)
	}()

	pr, err := reader.NewParquetReader(pRd, new(Person), 4)
	if err != nil {
		t.Fatal("Cannot create parquet reader", err)
	}
	defer pr.ReadStop()
	num := int(pr.GetNumRows())
	assert.Equal(t, num, 4)

	res, err := pr.ReadByNumber(num)
	if err != nil {
		t.Fatal("Cannot read", err)
	}

	var result string
	for _, person := range res {
		p, ok := person.(Person)
		if !ok {
			t.Fatal("cant convert")
		}
		result += fmt.Sprintf("%s,%d,%s\n", p.Name, p.Age, p.Title)
	}

	assert.Equal(t, expected, result)
}
