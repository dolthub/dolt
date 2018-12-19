package edit

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands/edit/mvdata"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
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
		cliEnv := createEnvWithSeedData(t)

		result := Export("dolt edit export", test.args, cliEnv)

		if result != 0 {
			t.Fatal("Unexpected failure.")
		}

		outLoc := mvdata.NewDataLocation(test.outFilePath, "")
		rd, _, verr := outLoc.CreateReader(nil, cliEnv.FS)

		if verr != nil {
			t.Fatal(verr.Verbose())
		}

		idIdx := rd.GetSchema().GetFieldIndex("id")
		imt, _ := dtestutils.CreateTestDataTable(test.outputIsTyped)
		dtestutils.CheckResultsAgainstReader(t, rd, idIdx, imt, "id")
	}
}
