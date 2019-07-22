package commands

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"testing"
)

func TestLog(t *testing.T) {
	dEnv := createUninitializedEnv()
	err := dEnv.InitRepo(context.Background(), types.Format_7_18, "Bill Billerson", "bigbillieb@fake.horse")

	if err != nil {
		t.Error("Failed to init repo")
	}

	cs, _ := doltdb.NewCommitSpec("HEAD", "master")
	commit, _ := dEnv.DoltDB.Resolve(context.Background(), cs)

	cli.Println(commit)
}
