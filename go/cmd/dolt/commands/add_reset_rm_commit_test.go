package commands

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands/edit"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/typed/nbf"
	"testing"
)

const (
	dataNbfFile = "data.nbf"
	table1      = "tbl1"
	table2      = "tbl2"
)

func TestAddResetCommitRmCommands(t *testing.T) {
	cliEnv := dtestutils.CreateTestEnv()
	imt, sch := dtestutils.CreateTestDataTable(true)
	imtRd := table.NewInMemTableReader(imt)

	fOut, _ := cliEnv.FS.OpenForWrite(dataNbfFile)
	nbfWr, _ := nbf.NewNBFWriter(fOut, sch)

	table.PipeRows(imtRd, nbfWr, false)
	nbfWr.Close()
	imtRd.Close()

	Version("test")("dolt version", []string{}, cliEnv)

	edit.Create("dolt edit create", []string{"-table", table1, dataNbfFile}, cliEnv)

	Diff("dolt diff", []string{"-table", table1}, cliEnv)

	Status("dolt status", []string{}, cliEnv)

	Ls("dolt ls", []string{}, cliEnv)

	Add("dolt add", []string{table1}, cliEnv)

	Commit("dolt commit", []string{"-m", "Added table"}, cliEnv)

	Log("dolt log", []string{}, cliEnv)

	edit.RmRow("dolt rm-row", []string{"-table", table1, "id:00000000-0000-0000-0000-000000000001"}, cliEnv)

	Add("dolt add", []string{table1}, cliEnv)

	Reset("dolt reset", []string{table1}, cliEnv)

	Rm("dolt rm", []string{table1}, cliEnv)
}
