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

package commands

const (
	dataNbfFile = "data.nbf"
	table1      = "tbl1"
)

/*
func TestAddResetCommitRmCommands(t *testing.T) {
	dEnv := dtestutils.CreateTestEnv()
	imt, sch := dtestutils.CreateTestDataTable(true)
	imtRd := table.NewInMemTableReader(imt)

	fOut, _ := dEnv.FS.OpenForWrite(dataNbfFile)
	nbfWr, _ := nbf.NewNBFWriter(fOut, sch)

	table.PipeRows(imtRd, nbfWr, false)
	nbfWr.Close()
	imtRd.Close()

	Version("test")("dolt version", []string{}, dEnv)

	Diff("dolt diff", []string{table1}, dEnv)

	Status("dolt status", []string{}, dEnv)

	Ls("dolt ls", []string{}, dEnv)

	Add("dolt add", []string{table1}, dEnv)

	Commit("dolt commit", []string{"-m", "Added table"}, dEnv)

	Log("dolt log", []string{}, dEnv)

	Add("dolt add", []string{table1}, dEnv)

	Reset("dolt reset", []string{table1}, dEnv)
}
*/
