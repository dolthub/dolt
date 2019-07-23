// Copyright 2019 Liquidata, Inc.
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

package fwt

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"io"
	"strings"
	"testing"
)

var PersonDB1 = `Bill Billerson32Senior Dufus
Rob Robertson 25Dufus       
John Johnson  21Intern Dufus`

var PersonDB2 = PersonDB1 + "\n"
var PersonDB3 = strings.Replace(PersonDB2, "\n", "\n\n", -1)

var PersonDBWithBadRow = `Bill Billerson | 32 | Senior Dufus
Rob Robertson  | 25 | Dufus       
John Johnson   | 21 |`

var PersonDBWithBadRow2 = PersonDBWithBadRow + "\n"
var PersonDBWithBadRow3 = strings.Replace(PersonDBWithBadRow2, "\n", "\n\n", -1)

func TestReader(t *testing.T) {
	colNames := []string{"name", "age", "title"}
	_, sch := untyped.NewUntypedSchema(colNames...)
	goodExpectedRows := []row.Row{
		untyped.NewRowFromStrings(types.Format_7_18, sch, []string{"Bill Billerson", "32", "Senior Dufus"}),
		untyped.NewRowFromStrings(types.Format_7_18, sch, []string{"Rob Robertson", "25", "Dufus"}),
		untyped.NewRowFromStrings(types.Format_7_18, sch, []string{"John Johnson", "21", "Intern Dufus"}),
	}
	badExpectedRows := []row.Row{
		untyped.NewRowFromStrings(types.Format_7_18, sch, []string{"Bill Billerson", "32", "Senior Dufus"}),
		untyped.NewRowFromStrings(types.Format_7_18, sch, []string{"Rob Robertson", "25", "Dufus"}),
	}

	widths := map[string]int{
		colNames[0]: 14,
		colNames[1]: 2,
		colNames[2]: 12,
	}

	fwtSch, _ := NewFWTSchema(sch, widths)

	i := []struct {
		inputStr     string
		expectedRows []row.Row
		sep          string
	}{
		{PersonDB1, goodExpectedRows, ""},
		{PersonDB2, goodExpectedRows, ""},
		{PersonDB3, goodExpectedRows, ""},

		{PersonDBWithBadRow, badExpectedRows, " | "},
		{PersonDBWithBadRow2, badExpectedRows, " | "},
		{PersonDBWithBadRow3, badExpectedRows, " | "},
	}
	tests := i

	for _, test := range tests {
		rows, numBad, err := readTestRows(t, test.inputStr, fwtSch, test.sep)

		if err != nil {
			t.Error("Unexpected Error:", err)
		}

		expectedBad := len(goodExpectedRows) - len(test.expectedRows)
		if numBad != expectedBad {
			t.Error("Unexpected bad rows count. expected:", expectedBad, "actual:", numBad)
		}

		if !row.IsValid(rows[0], sch) {
			t.Fatal("Invalid Row for expected schema")
		} else if len(rows) != len(test.expectedRows) {
			t.Error("Did not receive the correct number of rows. expected: ", len(test.expectedRows), "actual:", len(rows))
		} else {
			for i, r := range rows {
				expectedRow := test.expectedRows[i]
				if !row.AreEqual(r, expectedRow, sch) {
					t.Error(row.Fmt(context.Background(), r, sch), "!=", row.Fmt(context.Background(), expectedRow, sch))
				}
			}
		}
	}
}

func readTestRows(t *testing.T, inputStr string, fwtSch *FWTSchema, sep string) ([]row.Row, int, error) {
	const root = "/"
	const path = "/file.csv"

	fs := filesys.NewInMemFS(nil, map[string][]byte{path: []byte(inputStr)}, root)
	fwtRd, err := OpenFWTReader(types.Format_7_18, path, fs, fwtSch, sep)
	defer fwtRd.Close(context.Background())

	if err != nil {
		t.Fatal("Could not open reader", err)
	}

	badRows := 0
	var rows []row.Row
	for {
		row, err := fwtRd.ReadRow(context.Background())

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
