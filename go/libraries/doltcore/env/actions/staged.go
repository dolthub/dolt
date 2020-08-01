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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
)

var ErrTablesInConflict = errors.New("table is in conflict")

func StageTables(ctx context.Context, dEnv *env.DoltEnv, tbls []string) error {
	tables, docDetails, err := GetTblsAndDocDetails(dEnv, tbls)
	if err != nil {
		return err
	}

	if len(docDetails) > 0 {
		err = dEnv.PutDocsToWorking(ctx, docDetails)
		if err != nil {
			return err
		}
	}

	staged, working, err := getStagedAndWorking(ctx, dEnv)

	if err != nil {
		return err
	}

	err = stageTables(ctx, dEnv, tables, staged, working)
	if err != nil {
		dEnv.ResetWorkingDocsToStagedDocs(ctx)
		return err
	}
	return nil
}

// GetTblsAndDocDetails takes a slice of strings where valid doc names are replaced with doc table name. Doc names are
// appended to a docDetails slice. We return a tuple of tables, docDetails and error.
func GetTblsAndDocDetails(dEnv *env.DoltEnv, tbls []string) (tables []string, docDetails []doltdb.DocDetails, err error) {
	for i, tbl := range tbls {
		docDetail, err := dEnv.GetOneDocDetail(tbl)
		if err != nil {
			return nil, nil, err
		}
		if docDetail.DocPk != "" {
			docDetails = append(docDetails, docDetail)
			tbls[i] = doltdb.DocTableName
		}
	}
	return tbls, docDetails, nil
}

func StageAllTables(ctx context.Context, dEnv *env.DoltEnv) error {
	err := dEnv.PutDocsToWorking(ctx, nil)
	if err != nil {
		return err
	}

	staged, working, err := getStagedAndWorking(ctx, dEnv)

	if err != nil {
		return err
	}

	tbls, err := doltdb.UnionTableNames(ctx, staged, working)

	if err != nil {
		return err
	}

	err = stageTables(ctx, dEnv, tbls, staged, working)
	if err != nil {
		dEnv.ResetWorkingDocsToStagedDocs(ctx)
		return err
	}
	return nil
}

func stageTables(ctx context.Context, dEnv *env.DoltEnv, tbls []string, staged *doltdb.RootValue, working *doltdb.RootValue) error {
	err := ValidateTables(ctx, tbls, staged, working)
	if err != nil {
		return err
	}

	working, err = checkTablesForConflicts(ctx, tbls, working)
	if err != nil {
		return err
	}

	staged, err = moveTablesToStaged(ctx, tbls, staged, working)
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

func checkTablesForConflicts(ctx context.Context, tbls []string, working *doltdb.RootValue) (*doltdb.RootValue, error) {
	var inConflict []string
	for _, tblName := range tbls {
		tbl, _, err := working.GetTable(ctx, tblName)
		if err != nil {
			return nil, err
		}

		has, err := tbl.HasConflicts()
		if err != nil {
			return nil, err
		}
		if has {
			num, err := tbl.NumRowsInConflict(ctx)
			if err != nil {
				return nil, err
			}

			if num == 0 {
				clrTbl, err := tbl.ClearConflicts()
				if err != nil {
					return nil, err
				}

				working, err = working.PutTable(ctx, tblName, clrTbl)
				if err != nil {
					return nil, err
				}
			}

			if num > 0 {
				inConflict = append(inConflict, tblName)
			}
		}
	}

	if len(inConflict) > 0 {
		return nil, NewTblInConflictError(inConflict)
	}

	return working, nil
}

func moveTablesToStaged(ctx context.Context, tbls []string, staged, working *doltdb.RootValue) (newStaged *doltdb.RootValue, err error) {
	tblSet := set.NewStrSet(tbls)

	stagedFKs, err := staged.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	tblDeltas, err := diff.GetTableDeltas(ctx, staged, working)
	if err != nil {
		return nil, err
	}

	tblsToDrop := set.NewStrSet(nil)

	for _, td := range tblDeltas {
		if td.IsDrop() {
			if !tblSet.Contains(td.FromName) {
				continue
			}

			tblsToDrop.Add(td.FromName)
		} else {
			if !tblSet.Contains(td.ToName) {
				continue
			}

			if td.IsRename() {
				// rename table before adding the new version so we don't have
				// two copies of the same table
				staged, err = staged.RenameTable(ctx, td.FromName, td.ToName)
				if err != nil {
					return nil, err
				}
			}

			staged, err = staged.PutTable(ctx, td.ToName, td.ToTable)
			if err != nil {
				return nil, err
			}

			stagedFKs.RemoveKeys(td.FromFks...)
			err = stagedFKs.AddKeys(td.ToFks...)
			if err != nil {
				return nil, err
			}

			ss, _, err := working.GetSuperSchema(ctx, td.ToName)
			if err != nil {
				return nil, err
			}

			staged, err = staged.PutSuperSchema(ctx, td.ToName, ss)
			if err != nil {
				return nil, err
			}
		}
	}

	staged, err = staged.PutForeignKeyCollection(ctx, stagedFKs)
	if err != nil {
		return nil, err
	}

	// RemoveTables also removes that table's ForeignKeys
	staged, err = staged.RemoveTables(ctx, tblsToDrop.AsSlice()...)
	if err != nil {
		return nil, err
	}

	return staged, nil
}

// ValidateTables checks that all tables passed exist in at least one of the roots passed.
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
