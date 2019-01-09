package tblcmds

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"strings"
	"testing"
)

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
