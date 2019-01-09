package csv

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"io"
	"strings"
	"testing"
)

var PersonDB1 = `name, Age, Title
Bill Billerson, 32, Senior Dufus
Rob Robertson, 25, Dufus
John Johnson, 21, Intern Dufus`

var PersonDB2 = PersonDB1 + "\n"
var PersonDB3 = strings.Replace(PersonDB2, "\n", "\n\n", -1)

var PersonDBWithBadRow = `name, Age, Title
Bill Billerson, 32, Senior Dufus
Rob Robertson, 25, Dufus
John Johnson, 21`

var PersonDBWithBadRow2 = PersonDBWithBadRow + "\n"
var PersonDBWithBadRow3 = strings.Replace(PersonDBWithBadRow2, "\n", "\n\n", -1)

var PersonDBWithoutHeaders = `Bill Billerson, 32, Senior Dufus
Rob Robertson, 25, Dufus
John Johnson, 21, Intern Dufus`

var PersonDBDifferentHeaders = `n, a, t
Bill Billerson, 32, Senior Dufus
Rob Robertson, 25, Dufus
John Johnson, 21, Intern Dufus`

func TestReader(t *testing.T) {
	colNames := []string{"name", "age", "title"}
	sch := untyped.NewUntypedSchema(colNames)
	goodExpectedRows := []*table.Row{
		untyped.NewRowFromStrings(sch, []string{"Bill Billerson", "32", "Senior Dufus"}),
		untyped.NewRowFromStrings(sch, []string{"Rob Robertson", "25", "Dufus"}),
		untyped.NewRowFromStrings(sch, []string{"John Johnson", "21", "Intern Dufus"}),
	}
	badExpectedRows := []*table.Row{
		untyped.NewRowFromStrings(sch, []string{"Bill Billerson", "32", "Senior Dufus"}),
		untyped.NewRowFromStrings(sch, []string{"Rob Robertson", "25", "Dufus"}),
	}

	tests := []struct {
		inputStr     string
		expectedRows []*table.Row
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

		if !rows[0].GetSchema().Equals(test.expectedRows[0].GetSchema()) {
			t.Fatal("Unexpected schema")
		} else if len(rows) != len(test.expectedRows) {
			t.Error("Did not receive the correct number of rows. expected: ", len(test.expectedRows), "actual:", len(rows))
		} else {
			for i, row := range rows {
				expectedRow := test.expectedRows[i]
				if !table.RowsEqualIgnoringSchema(row, expectedRow) {
					t.Error(table.RowFmt(row), "!=", table.RowFmt(expectedRow))
				}
			}
		}
	}
}

func readTestRows(t *testing.T, inputStr string, info *CSVFileInfo) ([]*table.Row, int, error) {
	const root = "/"
	const path = "/file.csv"

	fs := filesys.NewInMemFS(nil, map[string][]byte{path: []byte(inputStr)}, root)
	csvR, err := OpenCSVReader(path, fs, info)
	defer csvR.Close()

	if err != nil {
		t.Fatal("Could not open reader", err)
	}

	badRows := 0
	var rows []*table.Row
	for {
		row, err := csvR.ReadRow()

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
