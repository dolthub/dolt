package actions

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/set"
)

func StageTables(dEnv *env.DoltEnv, tbls []string) error {
	staged, working, err := getStagedAndWorking(dEnv)

	if err != nil {
		return err
	}

	return stageTables(dEnv, tbls, staged, working)

}

func StageAllTables(dEnv *env.DoltEnv) error {
	staged, working, err := getStagedAndWorking(dEnv)

	if err != nil {
		return err
	}

	tbls := AllTables(staged, working)
	return stageTables(dEnv, tbls, staged, working)
}

func stageTables(dEnv *env.DoltEnv, tbls []string, staged *doltdb.RootValue, working *doltdb.RootValue) error {
	err := ValidateTables(tbls, staged, working)

	if err != nil {
		return err
	}

	updatedRoot := staged.UpdateTablesFromOther(tbls, working)

	return dEnv.UpdateStagedRoot(updatedRoot)
}

func AllTables(roots ...*doltdb.RootValue) []string {
	allTblNames := make([]string, 0, 16)
	for _, root := range roots {
		allTblNames = append(allTblNames, root.GetTableNames()...)
		allTblNames = append(allTblNames, root.GetTableNames()...)
	}

	return set.Unique(allTblNames)
}

func ValidateTables(tbls []string, roots ...*doltdb.RootValue) error {
	var missing []string
	for _, tbl := range tbls {
		found := false
		for _, root := range roots {
			if root.HasTable(tbl) {
				found = true
				break
			}
		}

		if !found {
			missing = append(missing, tbl)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	return TblNotExist{missing}
}

func getStagedAndWorking(dEnv *env.DoltEnv) (*doltdb.RootValue, *doltdb.RootValue, error) {
	roots, err := getRoots(dEnv, StagedRoot, WorkingRoot)

	if err != nil {
		return nil, nil, err
	}

	return roots[StagedRoot], roots[WorkingRoot], nil
}

func getWorkingAndHead(dEnv *env.DoltEnv) (*doltdb.RootValue, *doltdb.RootValue, error) {
	roots, err := getRoots(dEnv, WorkingRoot, HeadRoot)

	if err != nil {
		return nil, nil, err
	}

	return roots[WorkingRoot], roots[HeadRoot], nil
}

func getRoots(dEnv *env.DoltEnv, rootTypes ...RootType) (map[RootType]*doltdb.RootValue, error) {
	roots := make(map[RootType]*doltdb.RootValue)
	for _, rt := range rootTypes {
		var err error
		var root *doltdb.RootValue
		switch rt {
		case StagedRoot:
			root, err = dEnv.StagedRoot()
		case WorkingRoot:
			root, err = dEnv.WorkingRoot()
		case HeadRoot:
			root, err = dEnv.HeadRoot()
		default:
			panic("Method does not support this root type.")
		}

		if err != nil {
			return nil, RootValueUnreadable{rt, err}
		}

		roots[rt] = root
	}

	return roots, nil
}
