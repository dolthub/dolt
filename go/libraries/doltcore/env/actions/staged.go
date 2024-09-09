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
	"fmt"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

func StageTables(ctx context.Context, roots doltdb.Roots, tbls []doltdb.TableName, filterIgnoredTables bool) (doltdb.Roots, error) {
	if filterIgnoredTables {
		var err error
		filteredTables, err := doltdb.FilterIgnoredTables(ctx, tbls, roots)
		if len(filteredTables.Conflicts) > 0 {
			return doltdb.Roots{}, filteredTables.Conflicts[0]
		}
		if err != nil {
			return doltdb.Roots{}, err
		}
		tbls = filteredTables.DontIgnore
	}

	return stageTables(ctx, roots, tbls)
}

func StageAllTables(ctx context.Context, roots doltdb.Roots, filterIgnoredTables bool) (doltdb.Roots, error) {
	tbls, err := doltdb.UnionTableNames(ctx, roots.Staged, roots.Working)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots, err = StageTables(ctx, roots, tbls, filterIgnoredTables)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots, err = StageAllSchemas(ctx, roots)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return roots, nil
}

func StageAllSchemas(ctx context.Context, roots doltdb.Roots) (doltdb.Roots, error) {
	newStaged, err := MoveAllSchemasBetweenRoots(ctx, roots.Working, roots.Staged)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Staged = newStaged
	return roots, nil
}

// MoveAllSchemasBetweenRoots copies all schemas from the src RootValue to the dest RootValue.
func MoveAllSchemasBetweenRoots(ctx context.Context, src, dest doltdb.RootValue) (doltdb.RootValue, error) {
	srcSchemaNames, err := getDatabaseSchemaNames(ctx, src)
	if err != nil {
		return nil, err
	}

	if srcSchemaNames.Size() == 0 {
		return dest, nil
	}

	destSchemaNames, err := getDatabaseSchemaNames(ctx, dest)
	if err != nil {
		return nil, err
	}

	srcSchemaNames.Iterate(func(schemaName string) (cont bool) {
		if !destSchemaNames.Contains(schemaName) {
			dest, err = dest.CreateDatabaseSchema(ctx, schema.DatabaseSchema{
				Name: schemaName,
			})
			if err != nil {
				return false
			}
		}
		return true
	})

	return dest, nil
}

func StageDatabase(ctx context.Context, roots doltdb.Roots) (doltdb.Roots, error) {
	wColl, err := roots.Working.GetCollation(ctx)
	if err != nil {
		return doltdb.Roots{}, err
	}
	sColl, err := roots.Staged.GetCollation(ctx)
	if err != nil {
		return doltdb.Roots{}, err
	}
	if wColl == sColl {
		return roots, nil
	}
	roots.Staged, err = roots.Staged.SetCollation(ctx, wColl)
	if err != nil {
		return doltdb.Roots{}, err
	}
	return roots, nil
}

func StageModifiedAndDeletedTables(ctx context.Context, roots doltdb.Roots) (doltdb.Roots, error) {
	_, unstaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return doltdb.Roots{}, err
	}

	var tbls []doltdb.TableName
	for _, tableDelta := range unstaged {
		if strings.HasPrefix(tableDelta.FromName.Name, diff.DBPrefix) {
			continue
		}
		if !tableDelta.IsAdd() {
			tbls = append(tbls, tableDelta.FromName)
		}
	}

	return stageTables(ctx, roots, tbls)
}

func stageTables(ctx context.Context, roots doltdb.Roots, tbls []doltdb.TableName) (doltdb.Roots, error) {
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
func clearEmptyConflicts(ctx context.Context, tbls []doltdb.TableName, working doltdb.RootValue) (doltdb.RootValue, error) {
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
func ValidateTables(ctx context.Context, tbls []doltdb.TableName, roots ...doltdb.RootValue) error {
	var missing []doltdb.TableName
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

	return NewTblNotExistError(summarizeTableNames(missing))
}

func summarizeTableNames(names []doltdb.TableName) []string {
	namesStrs := make([]string, len(names))
	for i, name := range names {
		if name.Schema != "" {
			namesStrs[i] = fmt.Sprintf("%s.%s", name.Schema, name.Name)
		} else {
			namesStrs[i] = fmt.Sprintf("%s", name.Name)
		}
	}
	return namesStrs
}
