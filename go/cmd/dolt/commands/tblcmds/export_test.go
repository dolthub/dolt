package tblcmds

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/mvdata"
	"testing"
)

func TestExport(t *testing.T) {
	tests := []struct {
		args          []string
		outFilePath   string
		schemaJson    []byte
		mappingJson   string
		outputIsTyped bool
	}{
		{
			[]string{"-table", tableName, "-pk", "id", csvPath},
			csvPath,
			nil,
			"",
			false,
		},
		{
			[]string{"-table", tableName, psvPath},
			psvPath,
			nil,
			"",
			false,
		},
		{
			[]string{"-table", tableName, nbfPath},
			nbfPath,
			nil,
			"",
			true,
		},
	}

	for _, test := range tests {
		dEnv := createEnvWithSeedData(t)

		result := Export("dolt edit export", test.args, dEnv)

		if result != 0 {
			t.Fatal("Unexpected failure.")
		}

		outLoc := mvdata.NewDataLocation(test.outFilePath, "")
		rd, _, err := outLoc.CreateReader(nil, dEnv.FS)

		if err != nil {
			t.Fatal(err.Error())
		}

		idIdx := rd.GetSchema().GetFieldIndex("id")
		imt, _ := dtestutils.CreateTestDataTable(test.outputIsTyped)
		dtestutils.CheckResultsAgainstReader(t, rd, idIdx, imt, "id")
	}
}
