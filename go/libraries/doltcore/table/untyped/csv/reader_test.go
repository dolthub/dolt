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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

var PersonDB1 = `name, Age, Title
Bill Billerson, 32, Senior Dufus
Rob Robertson, 25, "Assistant, Dufus"
Jack Jackson, 27, 
John Johnson, 21, "Intern
Dufus"`

var PersonDB2 = PersonDB1 + "\n"
var PersonDB3 = strings.Replace(PersonDB2, "\n", "\n\n", 4) // don't replace quoted newline

var PersonDBWithBadRow = `name, Age, Title
Bill Billerson, 32, Senior Dufus
Rob Robertson, 25, Dufus
Jack Jackson, 27, 
John Johnson, 21`

var PersonDBWithBadRow2 = PersonDBWithBadRow + "\n"
var PersonDBWithBadRow3 = strings.Replace(PersonDBWithBadRow2, "\n", "\n\n", -1)

var PersonDBWithoutHeaders = `Bill Billerson, 32, Senior Dufus
Rob Robertson, 25, "Assistant, Dufus"
Jack Jackson, 27, 
John Johnson, 21, "Intern
Dufus"`

var PersonDBDifferentHeaders = `n, a, t
Bill Billerson, 32, Senior Dufus
Rob Robertson, 25, "Assistant, Dufus"
Jack Jackson, 27, 
John Johnson, 21, "Intern
Dufus"`

func mustRow(r row.Row, err error) row.Row {
	if err != nil {
		panic(err)
	}

	return r
}

// TODO: Rewrite these tests
func TestReader(t *testing.T) {
	colNames := []string{"name", "age", "title"}
	var sch sql.Schema
	for _, colName := range colNames {
		sch = append(sch, &sql.Column{Name: colName, Type: sql.Null})
	}

	goodExpectedRows := []sql.Row{
		{"Bill Billerson", "32", "Senior Dufus"},
		{"Rob Robertson", "25", "Assistant, Dufus"},
		{"Jack Jackson", "27", nil},
		{"John Johnson", "21", "Intern\nDufus"},
	}
	badExpectedRows := []sql.Row{
		{"Bill Billerson", "32", "Senior Dufus"},
		{"Rob Robertson", "25", "Dufus"},
		{"Jack Jackson", "27", nil},
	}

	tests := []struct {
		inputStr     string
		expectedRows []sql.Row
		info         *CSVFileInfo
	}{
		{PersonDB1, goodExpectedRows, NewCSVInfo()},
		{PersonDB2, goodExpectedRows, NewCSVInfo()},
		{PersonDB3, goodExpectedRows, NewCSVInfo()},

		{PersonDBWithBadRow, badExpectedRows, NewCSVInfo()},
		{PersonDBWithBadRow2, badExpectedRows, NewCSVInfo()},
		{PersonDBWithBadRow3, badExpectedRows, NewCSVInfo()},

		{
			PersonDBWithoutHeaders,
			goodExpectedRows,
			NewCSVInfo().SetHasHeaderLine(false).SetColumns(colNames),
		},
		{
			PersonDBDifferentHeaders,
			goodExpectedRows,
			NewCSVInfo().SetHasHeaderLine(true).SetColumns(colNames),
		},
	}

	for _, test := range tests {
		rows, numBad, err := readTestRows(t, test.inputStr, test.info)

		if err != nil {
			t.Fatal("Unexpected Error:", err)
		}

		expectedBad := len(goodExpectedRows) - len(test.expectedRows)
		if numBad != expectedBad {
			t.Error("Unexpected bad rows count. expected:", expectedBad, "actual:", numBad)
		}

		if len(rows) != len(test.expectedRows) {
			t.Error("Did not receive the correct number of rows. expected: ", len(test.expectedRows), "actual:", len(rows))
		} else {
			for i, r := range rows {
				expectedRow := test.expectedRows[i]
				eq, err := r.Equals(expectedRow, sch)
				assert.NoError(t, err)
				if !eq {
					t.Error(sql.FormatRow(r), "!=", sql.FormatRow(expectedRow))
				}
			}
		}
	}
}

func readTestRows(t *testing.T, inputStr string, info *CSVFileInfo) ([]sql.Row, int, error) {
	const root = "/"
	const path = "/file.csv"

	fs := filesys.NewInMemFS(nil, map[string][]byte{path: []byte(inputStr)}, root)
	csvR, err := OpenCSVReader(types.Format_Default, path, fs, info)
	defer csvR.Close(context.Background())

	if err != nil {
		t.Fatal("Could not open reader", err)
	}

	badRows := 0
	var rows []sql.Row
	for {
		row, err := csvR.ReadSqlRow(context.Background())

		if err != io.EOF && err != nil && !table.IsBadRow(err) {
			return nil, -1, err
		} else if table.IsBadRow(err) {
			badRows++
			continue
		} else if err == io.EOF && row == nil {
			break
		}

		rows = append(rows, row)
	}

	return rows, badRows, err
}
