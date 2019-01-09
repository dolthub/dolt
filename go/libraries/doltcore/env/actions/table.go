package actions

import "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"

func CheckoutTables(dEnv *env.DoltEnv, tbls []string) error {
	roots, err := getRoots(dEnv, WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return err
	}

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
			}
		}

		currRoot = currRoot.PutTable(dEnv.DoltDB, tblName, tbl)
	}

	if len(unknown) > 0 {
		return TblNotExist{unknown}
	}

	return dEnv.UpdateWorkingRoot(currRoot)
}
