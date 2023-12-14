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
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
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

func mustEncodeBytes(t *testing.T, bs []byte, enc encoding.Encoding) []byte {
	ret, n, err := transform.Bytes(enc.NewEncoder(), bs)
	require.NoError(t, err)
	require.Equal(t, n, len(bs))
	return ret
}

func TestReader(t *testing.T) {
	colNames := []string{"name", "age", "title"}
	_, sch := untyped.NewUntypedSchema(colNames...)
	goodExpectedRows := []row.Row{
		mustRow(untyped.NewRowFromStrings(types.Format_Default, sch, []string{"Bill Billerson", "32", "Senior Dufus"})),
		mustRow(untyped.NewRowFromStrings(types.Format_Default, sch, []string{"Rob Robertson", "25", "Assistant, Dufus"})),
		mustRow(untyped.NewRowFromStrings(types.Format_Default, sch, []string{"Jack Jackson", "27"})),
		mustRow(untyped.NewRowFromStrings(types.Format_Default, sch, []string{"John Johnson", "21", "Intern\nDufus"})),
	}
	badExpectedRows := []row.Row{
		mustRow(untyped.NewRowFromStrings(types.Format_Default, sch, []string{"Bill Billerson", "32", "Senior Dufus"})),
		mustRow(untyped.NewRowFromStrings(types.Format_Default, sch, []string{"Rob Robertson", "25", "Dufus"})),
		mustRow(untyped.NewRowFromStrings(types.Format_Default, sch, []string{"Jack Jackson", "27"})),
	}

	utf8bomBytes := mustEncodeBytes(t, []byte(PersonDB1), unicode.UTF8BOM)
	require.Equal(t, utf8bomBytes[0:3], []byte{0xEF, 0xBB, 0xBF})
	utf16leBytes := mustEncodeBytes(t, []byte(PersonDB1), unicode.UTF16(unicode.LittleEndian, unicode.UseBOM))
	utf16beBytes := mustEncodeBytes(t, []byte(PersonDB1), unicode.UTF16(unicode.BigEndian, unicode.UseBOM))

	tests := []struct {
		input        []byte
		expectedRows []row.Row
		info         *CSVFileInfo
	}{
		{[]byte(PersonDB1), goodExpectedRows, NewCSVInfo()},
		{[]byte(PersonDB2), goodExpectedRows, NewCSVInfo()},
		{[]byte(PersonDB3), goodExpectedRows, NewCSVInfo()},

		{utf8bomBytes, goodExpectedRows, NewCSVInfo()},
		{utf16leBytes, goodExpectedRows, NewCSVInfo()},
		{utf16beBytes, goodExpectedRows, NewCSVInfo()},

		{[]byte(PersonDBWithBadRow), badExpectedRows, NewCSVInfo()},
		{[]byte(PersonDBWithBadRow2), badExpectedRows, NewCSVInfo()},
		{[]byte(PersonDBWithBadRow3), badExpectedRows, NewCSVInfo()},

		{
			[]byte(PersonDBWithoutHeaders),
			goodExpectedRows,
			NewCSVInfo().SetHasHeaderLine(false).SetColumns(colNames),
		},
		{
			[]byte(PersonDBDifferentHeaders),
			goodExpectedRows,
			NewCSVInfo().SetHasHeaderLine(true).SetColumns(colNames),
		},
	}

	for _, test := range tests {
		rows, numBad, err := readTestRows(t, test.input, test.info)

		if err != nil {
			t.Fatal("Unexpected Error:", err)
		}

		expectedBad := len(goodExpectedRows) - len(test.expectedRows)
		if numBad != expectedBad {
			t.Error("Unexpected bad rows count. expected:", expectedBad, "actual:", numBad)
		}

		if isv, err := row.IsValid(rows[0], sch); err != nil {
			t.Fatal(err)
		} else if !isv {
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

func readTestRows(t *testing.T, input []byte, info *CSVFileInfo) ([]row.Row, int, error) {
	const root = "/"
	const path = "/file.csv"

	fs := filesys.NewInMemFS(nil, map[string][]byte{path: input}, root)
	csvR, err := OpenCSVReader(types.Format_Default, path, fs, info)
	defer csvR.Close(context.Background())

	if err != nil {
		t.Fatal("Could not open reader", err)
	}

	badRows := 0
	var rows []row.Row
	for {
		row, err := csvR.ReadRow(context.Background())

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
