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
	"errors"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

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

	newWkRoot := roots.Head
	for tblName, tbl := range untrackedTables {
		if tblName != doltdb.DocTableName {
			newWkRoot, err = newWkRoot.PutTable(ctx, tblName, tbl)
		}
		if err != nil {
			return nil, doltdb.Roots{}, errors.New("error: failed to write table back to database")
		}
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

func ResetHard(ctx context.Context, dEnv *env.DoltEnv, cSpecStr string, roots doltdb.Roots) error {
	dbData := dEnv.DbData()

	newHead, roots, err := resetHardTables(ctx, dbData, cSpecStr, roots)
	if err != nil {
		return err
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return err
	}

	err = dEnv.UpdateWorkingSet(ctx, ws.WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged).ClearMerge())
	if err != nil {
		return err
	}

	err = SaveTrackedDocsFromWorking(ctx, dEnv)
	if err != nil {
		return err
	}

	if newHead != nil {
		err = dEnv.DoltDB.SetHeadToCommit(ctx, dEnv.RepoStateReader().CWBHeadRef(), newHead)
		if err != nil {
			return err
		}
	}

	return nil
}

func ResetSoftTables(ctx context.Context, dbData env.DbData, apr *argparser.ArgParseResults, roots doltdb.Roots) (doltdb.Roots, error) {
	tables, err := getUnionedTables(ctx, apr.Args, roots.Staged, roots.Head)
	tables = RemoveDocsTable(tables)
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

	tables, docs, err := GetTablesOrDocs(dbData.Drw, tables)
	if err != nil {
		return doltdb.Roots{}, err
	}

	if len(docs) > 0 {
		tables = RemoveDocsTable(tables)
	}

	err = ValidateTables(context.TODO(), tables, roots.Staged, roots.Head)

	if err != nil {
		return doltdb.Roots{}, err
	}

	roots, err = resetDocs(ctx, roots, docs)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots, err = resetStaged(ctx, roots, tables)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return roots, nil
}

// ResetSoftToRef matches the `git reset --soft <REF>` pattern. It resets both staged and head to the previous ref
// and leaves the working unset. The user can then choose to create a commit that contains all changes since the ref.
func ResetSoftToRef(ctx context.Context, dbData env.DbData, cSpecStr string) error {
	cs, err := doltdb.NewCommitSpec(cSpecStr)
	if err != nil {
		return err
	}

	newHead, err := dbData.Ddb.Resolve(ctx, cs, dbData.Rsr.CWBHeadRef())
	if err != nil {
		return err
	}

	foundRoot, err := newHead.GetRootValue(ctx)
	if err != nil {
		return err
	}

	// Changed the staged to the old root. Leave the working as is.
	err = dbData.Rsw.UpdateStagedRoot(ctx, foundRoot)
	if err != nil {
		return err
	}

	// Update the head to this commit
	if err = dbData.Ddb.SetHeadToCommit(ctx, dbData.Rsr.CWBHeadRef(), newHead); err != nil {
		return err
	}

	return err
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

// resetDocs resets the working and staged docs with docs from head.
func resetDocs(ctx context.Context, roots doltdb.Roots, docs doltdocs.Docs) (doltdb.Roots, error) {
	docs, err := doltdocs.GetDocsFromRoot(ctx, roots.Head, doltdocs.GetDocNamesFromDocs(docs)...)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Working, err = doltdocs.UpdateRootWithDocs(ctx, roots.Working, docs)
	if err != nil {
		return doltdb.Roots{}, err
	}

	roots.Staged, err = doltdocs.UpdateRootWithDocs(ctx, roots.Staged, docs)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return roots, nil
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
