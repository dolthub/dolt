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

package csv

import (
	"context"
	"os"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
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

func getSampleRows() []sql.Row {
	return []sql.Row{
		{"Bill Billerson", 32, "Senior Dufus"},
		{"Rob Robertson", 25, "Dufus"},
		{"John Johnson", 21, ""},
		{"Andy Anderson", 27, nil},
	}
}

func writeToCSV(csvWr *CSVWriter, rows []sql.Row, t *testing.T) {
	func() {
		defer csvWr.Close(context.Background())

		for _, r := range rows {
			err := csvWr.WriteSqlRow(context.Background(), r)

			if err != nil {
				t.Fatal("Failed to write row")
			}
		}
	}()
}

func TestWriter(t *testing.T) {
	const root = "/"
	const path = "/file.csv"
	const expected = `name,age,title
Bill Billerson,32,Senior Dufus
Rob Robertson,25,Dufus
John Johnson,21,""
Andy Anderson,27,
`
	info := NewCSVInfo()

	rows := getSampleRows()

	fs := filesys.NewInMemFS(nil, nil, root)
	filePath, err := fs.Abs(path)
	if err != nil {
		t.Fatal("Could not open create filepath for CSVWriter", err)
	}
	writer, err := fs.OpenForWrite(filePath, os.ModePerm)
	if err != nil {
		t.Fatal("Could not open writer for CSVWriter", err)
	}

	csvWr, err := NewCSVWriter(writer, rowSch, info)

	if err != nil {
		t.Fatal("Could not open CSVWriter", err)
	}

	writeToCSV(csvWr, rows, t)

	results, err := fs.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(results) != expected {
		t.Errorf(`%s != %s`, results, expected)
	}
}

func TestWriterDelim(t *testing.T) {
	const root = "/"
	const path = "/file.csv"
	const expected = `name|age|title
Bill Billerson|32|Senior Dufus
Rob Robertson|25|Dufus
John Johnson|21|""
Andy Anderson|27|
`
	info := NewCSVInfo()
	info.SetDelim("|")

	rows := getSampleRows()

	fs := filesys.NewInMemFS(nil, nil, root)
	filePath, err := fs.Abs(path)
	if err != nil {
		t.Fatal("Could not open create filepath for CSVWriter", err)
	}
	writer, err := fs.OpenForWrite(filePath, os.ModePerm)
	if err != nil {
		t.Fatal("Could not open writer for CSVWriter", err)
	}
	csvWr, err := NewCSVWriter(writer, rowSch, info)

	if err != nil {
		t.Fatal("Could not open CSVWriter", err)
	}

	writeToCSV(csvWr, rows, t)

	results, err := fs.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(results) != expected {
		t.Errorf(`%s != %s`, results, expected)
	}
}
