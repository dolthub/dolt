package commands

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/nbf"
	"testing"
)

const (
	dataNbfFile = "data.nbf"
	table1      = "tbl1"
)

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
