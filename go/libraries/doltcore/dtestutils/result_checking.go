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

package dtestutils

/*
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

func CheckResultsAgainstReader(t *testing.T, tblRdr table.TableReadCloser, expectedTable *table.InMemTable, pkInExpectedTable string) {
	sch := tblRdr.GetSchema()
	expectedRdr := table.NewInMemTableReader(expectedTable)
	defer expectedRdr.Close()

	expectedPKIdx := expectedRdr.GetSchema().GetFieldIndex(pkInExpectedTable)
	resultRowMap, _, err := table.ReadAllRowsToMap(tblRdr, tblPKIdx, false)

	if err != nil {
		t.Error("Could not read all rows from table to map.", err)
		return
	}

	expectedRowMap, _, err := table.ReadAllRowsToMap(expectedRdr, false)

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

		if !row.AreEqual(expectedRow, actualRow, sch) {
			t.Error(row.Fmt(expectedRow, sch), "!=", row.Fmt(actualRow, sch))
			break
		}
	}
}
*/
