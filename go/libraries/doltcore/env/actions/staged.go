package actions

import (
	"context"
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/set"
)

var ErrTablesInConflict = errors.New("table is in conflict")

func StageTables(ctx context.Context, dEnv *env.DoltEnv, tbls []string, allowConflicts bool) error {
	staged, working, err := getStagedAndWorking(ctx, dEnv)

	if err != nil {
		return err
	}

	return stageTables(ctx, dEnv, tbls, staged, working, allowConflicts)

}

func StageAllTables(ctx context.Context, dEnv *env.DoltEnv, allowConflicts bool) error {
	staged, working, err := getStagedAndWorking(ctx, dEnv)

	if err != nil {
		return err
	}

	tbls := AllTables(ctx, staged, working)
	return stageTables(ctx, dEnv, tbls, staged, working, allowConflicts)
}

func stageTables(ctx context.Context, dEnv *env.DoltEnv, tbls []string, staged *doltdb.RootValue, working *doltdb.RootValue, allowConflicts bool) error {
	err := ValidateTables(ctx, tbls, staged, working)

	if err != nil {
		return err
	}

	if !allowConflicts {
		var inConflict []string
		for _, tblName := range tbls {
			tbl, _ := working.GetTable(ctx, tblName)

			if tbl.NumRowsInConflict() > 0 {
				if !allowConflicts {
					inConflict = append(inConflict, tblName)
				}
			}
		}

		if len(inConflict) > 0 {
			return NewTblInConflictError(inConflict)
		}
	}

	for _, tblName := range tbls {
		tbl, _ := working.GetTable(ctx, tblName)

		if tbl.HasConflicts() && tbl.NumRowsInConflict() == 0 {
			working = working.PutTable(ctx, dEnv.DoltDB, tblName, tbl.ClearConflicts())
		}
	}

	staged = staged.UpdateTablesFromOther(ctx, tbls, working)

	if wh, err := dEnv.DoltDB.WriteRootValue(ctx, working); err == nil {
		if sh, err := dEnv.DoltDB.WriteRootValue(ctx, staged); err == nil {
			dEnv.RepoState.Staged = sh.String()
			dEnv.RepoState.Working = wh.String()

			if err = dEnv.RepoState.Save(); err != nil {
				return env.ErrStateUpdate
			}

			return nil
		}
	}

	return doltdb.ErrNomsIO
}

func AllTables(ctx context.Context, roots ...*doltdb.RootValue) []string {
	allTblNames := make([]string, 0, 16)
	for _, root := range roots {
		allTblNames = append(allTblNames, root.GetTableNames(ctx)...)
		allTblNames = append(allTblNames, root.GetTableNames(ctx)...)
	}

	return set.Unique(allTblNames)
}

func ValidateTables(ctx context.Context, tbls []string, roots ...*doltdb.RootValue) error {
	var missing []string
	for _, tbl := range tbls {
		found := false
		for _, root := range roots {
			if root.HasTable(ctx, tbl) {
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

	return NewTblNotExistError(missing)
}

func getStagedAndWorking(ctx context.Context, dEnv *env.DoltEnv) (*doltdb.RootValue, *doltdb.RootValue, error) {
	roots, err := getRoots(ctx, dEnv, StagedRoot, WorkingRoot)

	if err != nil {
		return nil, nil, err
	}

	return roots[StagedRoot], roots[WorkingRoot], nil
}

func getWorkingAndHead(ctx context.Context, dEnv *env.DoltEnv) (*doltdb.RootValue, *doltdb.RootValue, error) {
	roots, err := getRoots(ctx, dEnv, WorkingRoot, HeadRoot)

	if err != nil {
		return nil, nil, err
	}

	return roots[WorkingRoot], roots[HeadRoot], nil
}

func getRoots(ctx context.Context, dEnv *env.DoltEnv, rootTypes ...RootType) (map[RootType]*doltdb.RootValue, error) {
	roots := make(map[RootType]*doltdb.RootValue)
	for _, rt := range rootTypes {
		var err error
		var root *doltdb.RootValue
		switch rt {
		case StagedRoot:
			root, err = dEnv.StagedRoot(ctx)
		case WorkingRoot:
			root, err = dEnv.WorkingRoot(ctx)
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
