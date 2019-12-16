// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actions

import (
	"context"
	"errors"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
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

	tbls, err := AllTables(ctx, staged, working)

	if err != nil {
		return err
	}

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
			tbl, _, err := working.GetTable(ctx, tblName)

			if err != nil {
				return err
			}

			if num, err := tbl.NumRowsInConflict(ctx); err != nil {
				return err
			} else if num > 0 {
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
		tbl, _, err := working.GetTable(ctx, tblName)

		if err != nil {
			return err
		}

		has, err := tbl.HasConflicts()

		if has {
			if num, err := tbl.NumRowsInConflict(ctx); err != nil {
				return err
			} else if num == 0 {
				clrTbl, err := tbl.ClearConflicts()

				if err != nil {
					return err
				}

				working, err = working.PutTable(ctx, tblName, clrTbl)

				if err != nil {
					return err
				}
			}
		}
	}

	staged, err = staged.UpdateTablesFromOther(ctx, tbls, working)

	if err != nil {
		return err
	}

	if wh, err := dEnv.DoltDB.WriteRootValue(ctx, working); err == nil {
		if sh, err := dEnv.DoltDB.WriteRootValue(ctx, staged); err == nil {
			dEnv.RepoState.Staged = sh.String()
			dEnv.RepoState.Working = wh.String()

			if err = dEnv.RepoState.Save(dEnv.FS); err != nil {
				return env.ErrStateUpdate
			}

			return nil
		}
	}

	return doltdb.ErrNomsIO
}

func AllTables(ctx context.Context, roots ...*doltdb.RootValue) ([]string, error) {
	allTblNames := make([]string, 0, 16)
	for _, root := range roots {
		tblNames, err := root.GetTableNames(ctx)

		if err != nil {
			return nil, err
		}

		allTblNames = append(allTblNames, tblNames...)
	}

	return set.Unique(allTblNames), nil
}

func ValidateTables(ctx context.Context, tbls []string, roots ...*doltdb.RootValue) error {
	var missing []string
	for _, tbl := range tbls {
		found := false
		for _, root := range roots {
			if has, err := root.HasTable(ctx, tbl); err != nil {
				return err
			} else if has {
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
			root, err = dEnv.HeadRoot(ctx)
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
