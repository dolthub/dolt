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
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
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

func mustRow(r row.Row, err error) row.Row {
	if err != nil {
		panic(err)
	}

	return r
}

func getSampleRows() (rows []row.Row) {
	return []row.Row{
		mustRow(row.New(types.Format_Default, rowSch, row.TaggedValues{
			nameColTag:  types.String("Bill Billerson"),
			ageColTag:   types.Uint(32),
			titleColTag: types.String("Senior Dufus")})),
		mustRow(row.New(types.Format_Default, rowSch, row.TaggedValues{
			nameColTag:  types.String("Rob Robertson"),
			ageColTag:   types.Uint(25),
			titleColTag: types.String("Dufus")})),
		mustRow(row.New(types.Format_Default, rowSch, row.TaggedValues{
			nameColTag:  types.String("John Johnson"),
			ageColTag:   types.Uint(21),
			titleColTag: types.String("")})),
		mustRow(row.New(types.Format_Default, rowSch, row.TaggedValues{
			nameColTag: types.String("Andy Anderson"),
			ageColTag:  types.Uint(27),
			/* title = NULL */})),
	}
}

func writeToParquet(pWr *ParquetWriter, rows []row.Row, t *testing.T) {
	func() {
		defer pWr.Close(context.Background())

		for _, row := range rows {
			err := pWr.WriteRow(context.Background(), row)
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

	file, err := ioutil.TempFile("", "parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())
	path := file.Name()

	rows := getSampleRows()

	pWr, err := NewParquetWriter(rowSch, path)
	if err != nil {
		t.Fatal("Could not open CSVWriter", err)
	}

	writeToParquet(pWr, rows, t)

	pRd, err := local.NewLocalFileReader(path)
	if err != nil {
		t.Fatal("Cannot open file", err)
	}

	pr, err := reader.NewParquetReader(pRd, new(Person), 4)
	if err != nil {
		t.Fatal("Cannot create parquet reader", err)
	}

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
