package tblcmds

import (
	"reflect"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/jsonenc"
)

func TestExportTblSchema(t *testing.T) {
	tests := []struct {
		table       string
		outFilePath string
	}{
		{
			tableName,
			"schema.json",
		},
	}

	for _, test := range tests {
		dEnv := createEnvWithSeedData(t)
		root, _ := dEnv.WorkingRoot()
		tbl, _ := root.GetTable(tableName)

		originalSchema := tbl.GetSchema()
		result := exportTblSchema(tableName, tbl, test.outFilePath, dEnv)

		if result != nil {
			t.Fatal("Unexpected failure.")
		}

		data, err := dEnv.FS.ReadFile(test.outFilePath)

		if err != nil {
			t.Fatal(err.Error())
		}

		newSchema, _ := jsonenc.SchemaFromJSON(data)

		if !reflect.DeepEqual(originalSchema, newSchema) {
			t.Error(originalSchema, "!=", newSchema)
		}
	}
}
