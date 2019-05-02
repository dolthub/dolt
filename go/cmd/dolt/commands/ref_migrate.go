package commands

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/ref"
	"strings"
)

func RefMigrate(commandStr string, args []string, dEnv *env.DoltEnv) int {
	cli.Println("local branches:")
	migrateDoltDB(dEnv.DoltDB)

	remotes, _ := dEnv.GetRemotes()
	for _, rem := range remotes {
		cli.Println(rem.Name + ":")
		remDB := rem.GetRemoteDB(context.Background())
		MigrateDoltDB(remDB)
	}

	return 0
}

func MigrateDoltDB(ddb *doltdb.DoltDB) {
	ctx := context.Background()
	refs := ddb.GetRefsOfType(context.Background(), map[ref.RefType]struct{}{ref.InvalidRefType: {}})

	for _, dref := range refs {
		dest := ref.NewBranchRef(dref.Path)
		if strings.HasPrefix(dref.Path, "remotes/") {
			dest = ref.NewRemoteRefFromPathStr(dref.Path)
		} else if strings.HasPrefix(dref.Path, "__") {
			dest = ref.NewInternalRef("create")
		}

		cli.Println("copying", dref.Path, "to", dest.String())
		err := ddb.CopyBranchByName(ctx, dref.Path, dest.String())

		if err != nil {
			cli.Println("failed to copy", dref.Path, "to", dest.String(), err)
			continue
		}

		cli.Println("deleting", dref.Path)
		err = ddb.DeleteBranchByName(ctx, dref.Path)

		if err != nil {
			cli.Println("failed to delete", dref.Path)
			continue
		}
	}

	cli.Println("\tGood refs:")
	refs = ddb.GetRefs(context.Background())
	for _, dref := range refs {
		cli.Println("\t", dref.String())
	}
}
