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

package tblcmds

/*
var allIDs = []types.UUID{
	types.UUID(dtestutils.UUIDS[0]),
	types.UUID(dtestutils.UUIDS[1]),
	types.UUID(dtestutils.UUIDS[2]),
}

var noZeroID = []types.UUID{
	types.UUID(dtestutils.UUIDS[1]),
	types.UUID(dtestutils.UUIDS[2]),
}

func TestRmRow(t *testing.T) {
	tests := []struct {
		args         []string
		expectedRet  int
		expectedKeys []types.UUID
	}{
		{[]string{}, 1, allIDs},
		{[]string{"-table", tableName}, 1, allIDs},
		{[]string{"-table", tableName, "id:00000000-0000-0000-0000-000000000000"}, 0, noZeroID},
		{[]string{"-table", tableName, "id:"}, 1, allIDs},
		{[]string{"-table", tableName, "id"}, 1, allIDs},
		{[]string{"-table", tableName, "00000000-0000-0000-0000-000000000000"}, 1, allIDs},
		{[]string{"-table", tableName, "id:not_a_uuid"}, 1, allIDs},
		{[]string{"-table", tableName, "id:99999999-9999-9999-9999-999999999999"}, 1, allIDs},
	}

	for _, test := range tests {
		dEnv := createEnvWithSeedData(t)

		commandStr := "dolt edit putrow"
		result := RmRow(commandStr, test.args, dEnv)

		if result != test.expectedRet {
			commandLine := commandStr + " " + strings.Join(test.args, " ")
			t.Fatal("Unexpected failure. command", commandLine, "returned", result)
		}

		checkExpectedRows(t, commandStr+strings.Join(test.args, " "), dEnv, test.expectedKeys)
	}
}

func checkExpectedRows(t *testing.T, commandStr string, dEnv *env.DoltEnv, uuids []types.UUID) {
	root, _ := dEnv.WorkingRoot()
	tbl, _ := root.GetTable(tableName)
	m := tbl.GetRowData()

	if int(m.Len()) != len(uuids) {
		t.Error("For", commandStr, "- Expected Row Count:", len(uuids), "Actual:", m.Len())
	}

	for _, uuid := range uuids {
		_, ok := m.MaybeGet(uuid)

		if !ok {
			t.Error("For", commandStr, "- Expected row with id:", uuid, "not found.")
		}
	}
}
*/
