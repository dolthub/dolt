// Copyright 2019 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

var ErrTablesInConflict = errors.New("table is in conflict")

func StageTables(ctx context.Context, dbData env.DbData, tbls []string) error {
	ddb := dbData.Ddb
	rsr := dbData.Rsr
	rsw := dbData.Rsw
	drw := dbData.Drw

	tables, docs, err := GetTablesOrDocs(drw, tbls)
	if err != nil {
		return err
	}

	staged, working, err := getStagedAndWorking(ctx, ddb, rsr)
	if err != nil {
		return err
	}

	if len(docs) > 0 {
		working, err = doltdocs.UpdateRootWithDocs(ctx, working, docs)
		if err != nil {
			return err
		}
	}

	err = stageTables(ctx, ddb, rsw, tables, staged, working)
	if err != nil {
		env.ResetWorkingDocsToStagedDocs(ctx, ddb, rsr, rsw)
		return err
	}
	return nil
}

func StageAllTables(ctx context.Context, dbData env.DbData) error {
	ddb := dbData.Ddb
	rsr := dbData.Rsr
	rsw := dbData.Rsw
	drw := dbData.Drw

	staged, err := env.StagedRoot(ctx, ddb, rsr)
	if err != nil {
		return err
	}

	working, err := env.WorkingRoot(ctx, ddb, rsr)
	if err != nil {
		return err
	}

	docs, err := drw.GetDocsOnDisk()
	if err != nil {
		return err
	}

	working, err = doltdocs.UpdateRootWithDocs(ctx, working, docs)
	if err != nil {
		return err
	}

	tbls, err := doltdb.UnionTableNames(ctx, staged, working)
	if err != nil {
		return err
	}

	err = stageTables(ctx, ddb, rsw, tbls, staged, working)
	if err != nil {
		env.ResetWorkingDocsToStagedDocs(ctx, ddb, rsr, rsw)
		return err
	}

	return nil
}

func stageTables(ctx context.Context, db *doltdb.DoltDB, rsw env.RepoStateWriter, tbls []string, staged *doltdb.RootValue, working *doltdb.RootValue) error {
	err := ValidateTables(ctx, tbls, staged, working)
	if err != nil {
		return err
	}

	working, err = checkTablesForConflicts(ctx, tbls, working)
	if err != nil {
		return err
	}

	staged, err = MoveTablesBetweenRoots(ctx, tbls, working, staged)
	if err != nil {
		return err
	}

	if _, err := env.UpdateWorkingRoot(ctx, db, rsw, working); err == nil {
		if sh, err := env.UpdateStagedRoot(ctx, db, rsw, staged); err == nil {
			err = rsw.SetStagedHash(ctx, sh)

			if err != nil {
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

func getStagedAndWorking(ctx context.Context, ddb *doltdb.DoltDB, rsr env.RepoStateReader) (*doltdb.RootValue, *doltdb.RootValue, error) {
	roots, err := getRoots(ctx, ddb, rsr, doltdb.StagedRoot, doltdb.WorkingRoot)

	if err != nil {
		return nil, nil, err
	}

	return roots[doltdb.StagedRoot], roots[doltdb.WorkingRoot], nil
}

// getRoots returns a RootValue object for each root type passed in rootTypes.
func getRoots(ctx context.Context, ddb *doltdb.DoltDB, rsr env.RepoStateReader, rootTypes ...doltdb.RootType) (map[doltdb.RootType]*doltdb.RootValue, error) {
	roots := make(map[doltdb.RootType]*doltdb.RootValue)
	for _, rt := range rootTypes {
		var err error
		var root *doltdb.RootValue
		switch rt {
		case doltdb.StagedRoot:
			root, err = env.StagedRoot(ctx, ddb, rsr)
		case doltdb.WorkingRoot:
			root, err = env.WorkingRoot(ctx, ddb, rsr)
		case doltdb.HeadRoot:
			root, err = env.HeadRoot(ctx, ddb, rsr)
		default:
			panic("Method does not support this root type.")
		}

		if err != nil {
			return nil, doltdb.RootValueUnreadable{RootType: rt, Cause: err}
		}

		roots[rt] = root
	}

	return roots, nil
}
