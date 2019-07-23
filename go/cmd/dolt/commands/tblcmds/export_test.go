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
func TestExport(t *testing.T) {
	tests := []struct {
		args          []string
		outFilePath   string
		schemaJson    []byte
		mappingJson   string
		outputIsTyped bool
	}{
		{
			[]string{"-pk", "id", tableName, csvPath},
			csvPath,
			nil,
			"",
			false,
		},
		{
			[]string{tableName, psvPath},
			psvPath,
			nil,
			"",
			false,
		},
		{
			[]string{tableName, nbfPath},
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
}*/
