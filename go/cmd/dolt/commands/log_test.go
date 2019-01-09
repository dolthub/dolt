package commands

import (
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"testing"
)

func TestLog(t *testing.T) {
	dEnv := createUninitializedEnv()
	err := dEnv.InitRepo("Bill Billerson", "bigbillieb@fake.horse")

	if err != nil {
		t.Error("Failed to init repo")
	}

	cs, _ := doltdb.NewCommitSpec("HEAD", "master")
	commit, _ := dEnv.DoltDB.Resolve(cs)

	cli.Println(commit)
}
