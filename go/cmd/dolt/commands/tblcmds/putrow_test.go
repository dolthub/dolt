package tblcmds

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"strings"
	"testing"
)

var expectedId = types.UUID(uuid.Must(uuid.Parse("11111111-1111-1111-1111-111111111111")))
var expectedFieldVals = map[string]types.Value{
	"id":         expectedId,
	"name":       types.String("Eric Ericson"),
	"age":        types.Uint(45),
	"is_married": types.Bool(true),
}

func TestPutRow(t *testing.T) {
	tests := []struct {
		args          []string
		expectedRes   int
		expectedTitle string
	}{
		{[]string{""}, 1, ""},
		{[]string{"-table", tableName}, 1, ""},
		{[]string{"-table", tableName, "id:", "name:Eric Ericson", "age:45", "is_married:true"}, 1, ""},
		{[]string{"-table", tableName, "id:11111111-1111-1111-1111-111111111111", "name:Eric Ericson", "age:45", "is_married:true", "title:Dolt"}, 0, "Dolt"},
		{[]string{"-table", tableName, "id:11111111-1111-1111-1111-111111111111", "name:Eric Ericson", "age:45", "is_married:true", "title:"}, 0, ""},
		{[]string{"-table", tableName, "id:11111111-1111-1111-1111-111111111111", "name:Eric Ericson", "age:45", "is_married:true", "title"}, 1, ""},
		{[]string{"-table", tableName, "id:11111111-1111-1111-1111-111111111111", "name:Eric Ericson", "age:45", "is_married:true", ":Dolt"}, 1, ""},
		{[]string{"-table", tableName, "id:1", "name:Eric Ericson", "age:45", "is_married:true"}, 1, ""},
		{[]string{"-table", tableName, "id:1", "name:Eric Ericson", "age:45", "is_married:true"}, 1, ""},
	}
	for _, test := range tests {
		dEnv := createEnvWithSeedData(t)

		commandStr := "dolt edit putrow"
		result := PutRow(commandStr, test.args, dEnv)

		if result != test.expectedRes {
			commandLine := commandStr + " " + strings.Join(test.args, " ")
			t.Fatal("Unexpected failure. command", commandLine, "returned", result)
		}

		if result == 0 {
			root, _ := dEnv.WorkingRoot()
			tbl, _ := root.GetTable(tableName)
			sch := tbl.GetSchema(dEnv.DoltDB.ValueReadWriter())
			row, exists := tbl.GetRow(expectedId, sch)

			if !exists {
				t.Fatal("Could not find row")
			}

			rowData := row.CurrData()
			for k, v := range expectedFieldVals {
				val, fld := rowData.GetFieldByName(k)

				if !val.Equals(v) {
					t.Error("Unexpected value for", fld.NameStr(), "expected:", v, "actual:", val)
				}
			}

			titleVal, _ := rowData.GetFieldByName("title")

			if !titleVal.Equals(types.String(test.expectedTitle)) {
				t.Error("Value of title was not the expected value. expected", test.expectedTitle, "actual", titleVal)
			}
		}
	}
}
