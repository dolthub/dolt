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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

var ErrTablesInConflict = errors.New("table is in conflict")

func StageTables(ctx context.Context, roots doltdb.Roots, dbData env.DbData, tbls []string) error {
	rsw := dbData.Rsw
	drw := dbData.Drw

	tables, docs, err := GetTablesOrDocs(drw, tbls)
	if err != nil {
		return err
	}

	if len(docs) > 0 {
		roots.Working, err = doltdocs.UpdateRootWithDocs(ctx, roots.Working, docs)
		if err != nil {
			return err
		}
	}

	err = stageTables(ctx, roots, rsw, tables)
	if err != nil {
		env.ResetWorkingDocsToStagedDocs(ctx, roots, rsw)
		return err
	}
	return nil
}

func StageTablesNoDocs(ctx context.Context, roots doltdb.Roots, tbls []string) (doltdb.Roots, error) {
	return stageTablesNoEnvUpdate(ctx, roots, tbls)
}

func StageAllTables(ctx context.Context, roots doltdb.Roots, dbData env.DbData) error {
	rsw := dbData.Rsw
	drw := dbData.Drw

	docs, err := drw.GetDocsOnDisk()
	if err != nil {
		return err
	}

	roots.Working, err = doltdocs.UpdateRootWithDocs(ctx, roots.Working, docs)
	if err != nil {
		return err
	}

	tbls, err := doltdb.UnionTableNames(ctx, roots.Staged, roots.Working)
	if err != nil {
		return err
	}

	err = stageTables(ctx, roots, rsw, tbls)
	if err != nil {
		env.ResetWorkingDocsToStagedDocs(ctx, roots, rsw)
		return err
	}

	return nil
}

func StageAllTablesNoDocs(ctx context.Context, roots doltdb.Roots) (doltdb.Roots, error) {
	tbls, err := doltdb.UnionTableNames(ctx, roots.Staged, roots.Working)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return stageTablesNoEnvUpdate(ctx, roots, tbls)
}

func stageTablesNoEnvUpdate(
	ctx context.Context,
	roots doltdb.Roots,
	tbls []string,
) (doltdb.Roots, error) {
	var err error
	err = ValidateTables(ctx, tbls, roots.Staged, roots.Working)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Working, err = checkTablesForConflicts(ctx, tbls, roots.Working)
	if err != nil {
		return doltdb.Roots{}, err
	}

	err = checkTablesForConstraintViolations(ctx, tbls, roots.Working)
	if err != nil {
		return doltdb.Roots{}, err
	}
	roots.Staged, err = MoveTablesBetweenRoots(ctx, tbls, roots.Working, roots.Staged)

	return roots, nil
}

func stageTables(
	ctx context.Context,
	roots doltdb.Roots,
	rsw env.RepoStateWriter,
	tbls []string,
) error {
	var err error
	roots, err = stageTablesNoEnvUpdate(ctx, roots, tbls)
	if err != nil {
		return err
	}

	// TODO: combine to single operation
	err = rsw.UpdateWorkingRoot(ctx, roots.Working)
	if err != nil {
		return err
	}

	return rsw.UpdateStagedRoot(ctx, roots.Staged)
}

// checkTablesForConflicts clears any 0-row conflicts from the tables named, and returns a new root with those
// conflicts cleared. If any tables named have real conflicts, returns an error.
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

func checkTablesForConstraintViolations(ctx context.Context, tbls []string, working *doltdb.RootValue) error {
	var violates []string
	for _, tblName := range tbls {
		tbl, ok, err := working.GetTable(ctx, tblName)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if cvMap, err := tbl.GetConstraintViolations(ctx); err != nil {
			return err
		} else if !cvMap.Empty() {
			violates = append(violates, tblName)
		}
	}

	if len(violates) > 0 {
		return NewTblHasConstraintViolations(violates)
	}
	return nil
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
