// Copyright 2020 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

// resetHardTables resolves a new HEAD commit from a refSpec and updates working set roots by
// resetting the table contexts for tracked tables. New tables are ignored. Returns new HEAD
// Commit and Roots.
func resetHardTables(ctx context.Context, dbData env.DbData, cSpecStr string, roots doltdb.Roots) (*doltdb.Commit, doltdb.Roots, error) {
	ddb := dbData.Ddb
	rsr := dbData.Rsr

	var newHead *doltdb.Commit
	if cSpecStr != "" {
		cs, err := doltdb.NewCommitSpec(cSpecStr)
		if err != nil {
			return nil, doltdb.Roots{}, err
		}

		newHead, err = ddb.Resolve(ctx, cs, rsr.CWBHeadRef())
		if err != nil {
			return nil, doltdb.Roots{}, err
		}

		roots.Head, err = newHead.GetRootValue(ctx)
		if err != nil {
			return nil, doltdb.Roots{}, err
		}
	}

	// mirroring Git behavior, untracked tables are ignored on 'reset --hard',
	// save the state of these tables and apply them to |newHead|'s root.
	//
	// as a special case, if an untracked table has a tag collision with any
	// tables in |newHead| we silently drop it from the new working set.
	// these tag collision is typically cause by table renames (bug #751).

	untracked, err := roots.Working.GetAllSchemas(ctx)
	if err != nil {
		return nil, doltdb.Roots{}, err
	}
	// untracked tables exist in |working| but not in |staged|
	staged, err := roots.Staged.GetTableNames(ctx)
	if err != nil {
		return nil, doltdb.Roots{}, err
	}
	for _, name := range staged {
		delete(untracked, name)
	}

	newWkRoot := roots.Head

	ws, err := newWkRoot.GetAllSchemas(ctx)
	if err != nil {
		return nil, doltdb.Roots{}, err
	}
	tags := mapColumnTags(ws)

	for name, sch := range untracked {
		for _, pk := range sch.GetAllCols().GetColumns() {
			if _, ok := tags[pk.Tag]; ok {
				// |pk.Tag| collides with a schema in |newWkRoot|
				delete(untracked, name)
			}
		}
	}

	for name := range untracked {
		tbl, _, err := roots.Working.GetTable(ctx, name)
		if err != nil {
			return nil, doltdb.Roots{}, err
		}
		newWkRoot, err = newWkRoot.PutTable(ctx, name, tbl)
		if err != nil {
			return nil, doltdb.Roots{}, fmt.Errorf("failed to write table back to database: %s", err)
		}
	}

	// need to save the state of files that aren't tracked
	untrackedTables := make(map[string]*doltdb.Table)
	wTblNames, err := roots.Working.GetTableNames(ctx)

	if err != nil {
		return nil, doltdb.Roots{}, err
	}

	for _, tblName := range wTblNames {
		untrackedTables[tblName], _, err = roots.Working.GetTable(ctx, tblName)

		if err != nil {
			return nil, doltdb.Roots{}, err
		}
	}

	headTblNames, err := roots.Staged.GetTableNames(ctx)

	if err != nil {
		return nil, doltdb.Roots{}, err
	}

	for _, tblName := range headTblNames {
		delete(untrackedTables, tblName)
	}

	roots.Working = newWkRoot
	roots.Staged = roots.Head

	return newHead, roots, nil
}

// ResetHardTables resets the tables in working, staged, and head based on the given parameters. Returns the new
// head commit and resulting roots
func ResetHardTables(ctx context.Context, dbData env.DbData, cSpecStr string, roots doltdb.Roots) (*doltdb.Commit, doltdb.Roots, error) {
	return resetHardTables(ctx, dbData, cSpecStr, roots)
}

// ResetHard resets the working, staged, and head to the ones in the provided roots and head ref.
// The reset can be performed on a non-current branch and working set.
// Returns an error if the reset fails.
func ResetHard(
	ctx context.Context,
	dEnv *env.DoltEnv,
	cSpecStr string,
	roots doltdb.Roots,
	headRef ref.DoltRef,
	ws *doltdb.WorkingSet,
) error {
	dbData := dEnv.DbData()

	newHead, roots, err := resetHardTables(ctx, dbData, cSpecStr, roots)
	if err != nil {
		return err
	}

	err = dEnv.UpdateWorkingSet(ctx, ws.WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged).ClearMerge())
	if err != nil {
		return err
	}

	if newHead != nil {
		err = dEnv.DoltDB.SetHeadToCommit(ctx, headRef, newHead)
		if err != nil {
			return err
		}
	}

	return nil
}

func ResetSoftTables(ctx context.Context, dbData env.DbData, apr *argparser.ArgParseResults, roots doltdb.Roots) (doltdb.Roots, error) {
	tables, err := getUnionedTables(ctx, apr.Args, roots.Staged, roots.Head)
	if err != nil {
		return doltdb.Roots{}, err
	}

	err = ValidateTables(context.TODO(), tables, roots.Staged, roots.Head)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Staged, err = MoveTablesBetweenRoots(ctx, tables, roots.Head, roots.Staged)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return roots, nil
}

// ResetSoft resets the staged value from HEAD for the tables given and returns the updated roots.
func ResetSoft(ctx context.Context, dbData env.DbData, tables []string, roots doltdb.Roots) (doltdb.Roots, error) {
	tables, err := getUnionedTables(ctx, tables, roots.Staged, roots.Head)
	if err != nil {
		return doltdb.Roots{}, err
	}

	err = ValidateTables(context.TODO(), tables, roots.Staged, roots.Head)
	if err != nil {
		return doltdb.Roots{}, err
	}
	return resetStaged(ctx, roots, tables)
}

// ResetSoftToRef matches the `git reset --soft <REF>` pattern. It resets both staged and head to the previous ref
// and leaves the working unset. The user can then choose to create a commit that contains all changes since the ref.
func ResetSoftToRef(ctx context.Context, dbData env.DbData, cSpecStr string) (*doltdb.RootValue, error) {
	cs, err := doltdb.NewCommitSpec(cSpecStr)
	if err != nil {
		return nil, err
	}

	newHead, err := dbData.Ddb.Resolve(ctx, cs, dbData.Rsr.CWBHeadRef())
	if err != nil {
		return nil, err
	}

	foundRoot, err := newHead.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	// Changed the staged to the old root. Leave the working as is.
	err = dbData.Rsw.UpdateStagedRoot(ctx, foundRoot)
	if err != nil {
		return nil, err
	}

	// Update the head to this commit
	if err = dbData.Ddb.SetHeadToCommit(ctx, dbData.Rsr.CWBHeadRef(), newHead); err != nil {
		return nil, err
	}

	return foundRoot, err
}

func getUnionedTables(ctx context.Context, tables []string, stagedRoot, headRoot *doltdb.RootValue) ([]string, error) {
	if len(tables) == 0 || (len(tables) == 1 && tables[0] == ".") {
		var err error
		tables, err = doltdb.UnionTableNames(ctx, stagedRoot, headRoot)

		if err != nil {
			return nil, err
		}
	}

	return tables, nil
}

func resetStaged(ctx context.Context, roots doltdb.Roots, tbls []string) (doltdb.Roots, error) {
	newStaged, err := MoveTablesBetweenRoots(ctx, tbls, roots.Head, roots.Staged)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Staged = newStaged
	return roots, nil
}

// ValidateIsRef validates whether the input parameter is a valid cString
func ValidateIsRef(ctx context.Context, cSpecStr string, ddb *doltdb.DoltDB, rsr env.RepoStateReader) bool {
	cs, err := doltdb.NewCommitSpec(cSpecStr)
	if err != nil {
		return false
	}

	_, err = ddb.Resolve(ctx, cs, rsr.CWBHeadRef())
	if err != nil {
		return false
	}

	return true
}

// CleanUntracked deletes untracked tables from the working root.
// Evaluates untracked tables as: all working tables - all staged tables.
func CleanUntracked(ctx context.Context, roots doltdb.Roots, tables []string, dryrun bool) (doltdb.Roots, error) {
	untrackedTables := make(map[string]struct{})

	var err error
	if len(tables) == 0 {
		tables, err = roots.Working.GetTableNames(ctx)
		if err != nil {
			return doltdb.Roots{}, nil
		}
	}

	for i := range tables {
		name := tables[i]
		_, _, err = roots.Working.GetTable(ctx, name)
		if err != nil {
			return doltdb.Roots{}, err
		}
		untrackedTables[name] = struct{}{}
	}

	// untracked tables = working tables - staged tables
	headTblNames, err := roots.Staged.GetTableNames(ctx)
	if err != nil {
		return doltdb.Roots{}, err
	}

	for _, name := range headTblNames {
		delete(untrackedTables, name)
	}

	newRoot := roots.Working
	var toDelete []string
	for t := range untrackedTables {
		toDelete = append(toDelete, t)
	}

	newRoot, err = newRoot.RemoveTables(ctx, false, false, toDelete...)
	if err != nil {
		return doltdb.Roots{}, fmt.Errorf("failed to remove tables; %w", err)
	}

	if dryrun {
		return roots, nil
	}
	roots.Working = newRoot

	return roots, nil
}

// mapColumnTags takes a map from table name to schema.Schema and generates
// a map from column tags to table names (see RootValue.GetAllSchemas).
func mapColumnTags(tables map[string]schema.Schema) (m map[uint64]string) {
	m = make(map[uint64]string, len(tables))
	for tbl, sch := range tables {
		for _, tag := range sch.GetAllCols().Tags {
			m[tag] = tbl
		}
	}
	return
}
