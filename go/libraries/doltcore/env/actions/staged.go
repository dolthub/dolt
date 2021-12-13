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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
)

func StageTables(ctx context.Context, roots doltdb.Roots, docs doltdocs.Docs, tbls []string) (doltdb.Roots, error) {
	if len(docs) > 0 {
		var err error
		roots.Working, err = doltdocs.UpdateRootWithDocs(ctx, roots.Working, docs)
		if err != nil {
			return doltdb.Roots{}, err
		}
	}

	return stageTables(ctx, roots, tbls)
}

func StageTablesNoDocs(ctx context.Context, roots doltdb.Roots, tbls []string) (doltdb.Roots, error) {
	return stageTables(ctx, roots, tbls)
}

func StageAllTables(ctx context.Context, roots doltdb.Roots, docs doltdocs.Docs) (doltdb.Roots, error) {
	var err error

	// To stage all docs for removal, use an empty slice instead of nil
	if docs != nil {
		roots.Working, err = doltdocs.UpdateRootWithDocs(ctx, roots.Working, docs)
		if err != nil {
			return doltdb.Roots{}, err
		}
	}

	tbls, err := doltdb.UnionTableNames(ctx, roots.Staged, roots.Working)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return stageTables(ctx, roots, tbls)
}

func StageAllTablesNoDocs(ctx context.Context, roots doltdb.Roots) (doltdb.Roots, error) {
	tbls, err := doltdb.UnionTableNames(ctx, roots.Staged, roots.Working)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return stageTables(ctx, roots, tbls)
}

func stageTables(
	ctx context.Context,
	roots doltdb.Roots,
	tbls []string,
) (doltdb.Roots, error) {
	var err error
	err = ValidateTables(ctx, tbls, roots.Staged, roots.Working)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Working, err = clearEmptyConflicts(ctx, tbls, roots.Working)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Staged, err = MoveTablesBetweenRoots(ctx, tbls, roots.Working, roots.Staged)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return roots, nil
}

// clearEmptyConflicts clears any 0-row conflicts from the tables named, and returns a new root.
func clearEmptyConflicts(ctx context.Context, tbls []string, working *doltdb.RootValue) (*doltdb.RootValue, error) {
	for _, tblName := range tbls {
		tbl, ok, err := working.GetTable(ctx, tblName)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		has, err := tbl.HasConflicts(ctx)
		if err != nil {
			return nil, err
		}
		if has {
			num, err := tbl.NumRowsInConflict(ctx)
			if err != nil {
				return nil, err
			}

			if num == 0 {
				clrTbl, err := tbl.ClearConflicts(ctx)
				if err != nil {
					return nil, err
				}

				working, err = working.PutTable(ctx, tblName, clrTbl)
				if err != nil {
					return nil, err
				}
			}
		}
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
