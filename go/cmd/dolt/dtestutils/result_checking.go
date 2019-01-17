package dtestutils

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"testing"
)

func CheckResultTable(t *testing.T, tableName string, dEnv *env.DoltEnv, expectedTable *table.InMemTable, pkInExpectedTable string) {
	root, err := dEnv.WorkingRoot()

	if err != nil {
		t.Error("Could not get dolt working root value", err)
	}

	tbl, ok := root.GetTable(tableName)

	if !ok {
		t.Error("Could not find table")
		return
	}

	tblRdr := noms.NewNomsMapReader(tbl.GetRowData(), tbl.GetSchema())
	defer tblRdr.Close()

	CheckResultsAgainstReader(t, tblRdr, tblRdr.GetSchema().GetPKIndex(), expectedTable, pkInExpectedTable)
}

func CheckResultsAgainstReader(t *testing.T, tblRdr table.TableReadCloser, tblPKIdx int, expectedTable *table.InMemTable, pkInExpectedTable string) {
	expectedRdr := table.NewInMemTableReader(expectedTable)
	defer expectedRdr.Close()

	expectedPKIdx := expectedRdr.GetSchema().GetFieldIndex(pkInExpectedTable)
	resultRowMap, _, err := table.ReadAllRowsToMap(tblRdr, tblPKIdx, false)

	if err != nil {
		t.Error("Could not read all rows from table to map.", err)
		return
	}

	expectedRowMap, _, err := table.ReadAllRowsToMap(expectedRdr, expectedPKIdx, false)

	if err != nil {
		t.Error("Could not read all expected rows to a map.", err)
		return
	}

	if len(resultRowMap) != len(expectedRowMap) {
		t.Error("unexpected number of rows in map.")
		return
	}

	for pk, expectedRows := range expectedRowMap {
		actualRows, pkOk := resultRowMap[pk]

		if !pkOk {
			t.Error("Could not find row with key", pk, "in results.")
			break
		}

		if len(actualRows) != 1 || len(expectedRows) != 1 {
			t.Error("num rows with key", pk, "does not match expectation.")
			break
		}

		expectedRow := expectedRows[0]
		actualRow := actualRows[0]

		if !table.RowsEqualIgnoringSchema(expectedRow, actualRow) {
			t.Error(table.RowFmt(expectedRow), "!=", table.RowFmt(actualRow))
			break
		}
	}
}
