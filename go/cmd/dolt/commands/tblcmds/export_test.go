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
