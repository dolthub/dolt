package actions

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
)

func CheckoutAllTables(dEnv *env.DoltEnv) error {
	roots, err := getRoots(dEnv, WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return err
	}

	tbls := AllTables(roots[WorkingRoot], roots[StagedRoot], roots[HeadRoot])
	return checkoutTables(dEnv, roots, tbls)

}

func CheckoutTables(dEnv *env.DoltEnv, tbls []string) error {
	roots, err := getRoots(dEnv, WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return err
	}

	return checkoutTables(dEnv, roots, tbls)
}

func checkoutTables(dEnv *env.DoltEnv, roots map[RootType]*doltdb.RootValue, tbls []string) error {
	var unknown []string

	currRoot := roots[WorkingRoot]
	staged := roots[StagedRoot]
	head := roots[HeadRoot]
	for _, tblName := range tbls {
		tbl, ok := staged.GetTable(tblName)

		if !ok {
			tbl, ok = head.GetTable(tblName)

			if !ok {
				unknown = append(unknown, tblName)
				continue
			}
		}

		currRoot = currRoot.PutTable(dEnv.DoltDB, tblName, tbl)
	}

	if len(unknown) > 0 {
		var err error
		currRoot, err = currRoot.RemoveTables(unknown)

		if err != nil {
			return err
		}
	}

	return dEnv.UpdateWorkingRoot(currRoot)
}
