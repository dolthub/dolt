package commands

import (
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"testing"
)

func TestLog(t *testing.T) {
	cliEnv := createUninitializedEnv()
	err := cliEnv.InitRepo("Bill Billerson", "bigbillieb@fake.horse")

	if err != nil {
		t.Error("Failed to init repo")
	}

	cs, _ := doltdb.NewCommitSpec("HEAD", "master")
	commit, _ := cliEnv.DoltDB.Resolve(cs)

	fmt.Println(commit)
}
