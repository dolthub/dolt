package actions

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/merge"
)

type AutoResolveStats struct {
}

func AutoResolveAll(dEnv *env.DoltEnv, autoResolver merge.AutoResolver) error {
	root, err := dEnv.WorkingRoot()

	if err != nil {
		return err
	}

	tbls := root.TablesInConflict()
	return autoResolve(dEnv, root, autoResolver, tbls)
}

func AutoResolveTables(dEnv *env.DoltEnv, autoResolver merge.AutoResolver, tbls []string) error {
	root, err := dEnv.WorkingRoot()

	if err != nil {
		return err
	}

	return autoResolve(dEnv, root, autoResolver, tbls)
}

func autoResolve(dEnv *env.DoltEnv, root *doltdb.RootValue, autoResolver merge.AutoResolver, tbls []string) error {
	for _, tblName := range tbls {
		tbl, ok := root.GetTable(tblName)

		if !ok {
			return doltdb.ErrTableNotFound
		}

		updatedTbl, err := merge.ResolveTable(root.VRW(), tbl, autoResolver)

		if err != nil {
			return err
		}

		root = root.PutTable(dEnv.DoltDB, tblName, updatedTbl)
	}

	return dEnv.UpdateWorkingRoot(root)
}
