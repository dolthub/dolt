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

func resetHardTables(ctx context.Context, dbData env.DbData, cSpecStr string, workingRoot, stagedRoot, headRoot *doltdb.RootValue) (*doltdb.Commit, error) {
	ddb := dbData.Ddb
	rsr := dbData.Rsr
	rsw := dbData.Rsw

	var newHead *doltdb.Commit
	if cSpecStr != "" {
		cs, err := doltdb.NewCommitSpec(cSpecStr)
		if err != nil {
			return nil, err
		}

		newHead, err = ddb.Resolve(ctx, cs, rsr.CWBHeadRef())
		if err != nil {
			return nil, err
		}

		headRoot, err = newHead.GetRootValue()
		if err != nil {
			return nil, err
		}
	}

	// need to save the state of files that aren't tracked
	untrackedTables := make(map[string]*doltdb.Table)
	wTblNames, err := workingRoot.GetTableNames(ctx)

	if err != nil {
		return nil, err
	}

	for _, tblName := range wTblNames {
		untrackedTables[tblName], _, err = workingRoot.GetTable(ctx, tblName)

		if err != nil {
			return nil, err
		}
	}

	headTblNames, err := stagedRoot.GetTableNames(ctx)

	if err != nil {
		return nil, err
	}

	for _, tblName := range headTblNames {
		delete(untrackedTables, tblName)
	}

	newWkRoot := headRoot
	for tblName, tbl := range untrackedTables {
		if tblName != doltdb.DocTableName {
			newWkRoot, err = newWkRoot.PutTable(ctx, tblName, tbl)
		}
		if err != nil {
			return nil, errors.New("error: failed to write table back to database")
		}
	}

	// TODO: combine these to a single operation
	err = env.UpdateWorkingRoot(ctx, rsw, newWkRoot)
	if err != nil {
		return nil, err
	}

	err = env.UpdateStagedRoot(ctx, rsw, headRoot)
	if err != nil {
		return nil, err
	}

	return newHead, nil
}

// ResetHardTables resets the tables in working, staged, and head based on the given parameters. Returns the commit hash
// if head is updated.
func ResetHardTables(ctx context.Context, dbData env.DbData, cSpecStr string, workingRoot, stagedRoot, headRoot *doltdb.RootValue) (string, error) {
	newHead, err := resetHardTables(ctx, dbData, cSpecStr, workingRoot, stagedRoot, headRoot)

	if err != nil {
		return "", err
	}

	ddb := dbData.Ddb
	rsr := dbData.Rsr

	if newHead != nil {
		if err := ddb.SetHeadToCommit(ctx, rsr.CWBHeadRef(), newHead); err != nil {
			return "", err
		}

		h, err := newHead.HashOf()
		if err != nil {
			return "", err
		}

		return h.String(), nil
	}

	return "", nil
}

func ResetHard(ctx context.Context, dEnv *env.DoltEnv, cSpecStr string, workingRoot, stagedRoot, headRoot *doltdb.RootValue) error {
	dbData := dEnv.DbData()

	newHead, err := resetHardTables(ctx, dbData, cSpecStr, workingRoot, stagedRoot, headRoot)

	if err != nil {
		return err
	}

	err = SaveTrackedDocsFromWorking(ctx, dEnv)
	if err != nil {
		return err
	}

	ddb := dbData.Ddb
	rsr := dbData.Rsr

	if newHead != nil {
		if err = ddb.SetHeadToCommit(ctx, rsr.CWBHeadRef(), newHead); err != nil {
			return err
		}
	}

	return nil
}

func ResetSoftTables(ctx context.Context, dbData env.DbData, apr *argparser.ArgParseResults, stagedRoot, headRoot *doltdb.RootValue) (*doltdb.RootValue, error) {
	tables, err := getUnionedTables(ctx, apr.Args(), stagedRoot, headRoot)
	tables = RemoveDocsTable(tables)

	if err != nil {
		return nil, err
	}

	err = ValidateTables(context.TODO(), tables, stagedRoot, headRoot)

	if err != nil {
		return nil, err
	}

	stagedRoot, err = resetStaged(ctx, dbData.Ddb, dbData.Rsw, tables, stagedRoot, headRoot)

	if err != nil {
		return nil, err
	}

	return stagedRoot, nil
}

func ResetSoft(ctx context.Context, dbData env.DbData, tables []string, workingRoot, stagedRoot, headRoot *doltdb.RootValue) (*doltdb.RootValue, error) {
	tables, err := getUnionedTables(ctx, tables, stagedRoot, headRoot)

	if err != nil {
		return nil, err
	}

	tables, docs, err := GetTablesOrDocs(dbData.Drw, tables)
	if err != nil {
		return nil, err
	}

	if len(docs) > 0 {
		tables = RemoveDocsTable(tables)
	}

	err = ValidateTables(context.TODO(), tables, stagedRoot, headRoot)

	if err != nil {
		return nil, err
	}

	stagedRoot, err = resetDocs(ctx, dbData, workingRoot, headRoot, stagedRoot, docs)
	if err != nil {
		return nil, err
	}

	stagedRoot, err = resetStaged(ctx, dbData.Ddb, dbData.Rsw, tables, stagedRoot, headRoot)

	if err != nil {
		return nil, err
	}

	return stagedRoot, nil
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

	foundRoot, err := newHead.GetRootValue()
	if err != nil {
		return err
	}

	// Changed the stage to old the root. Leave the working as is.
	err = env.UpdateStagedRoot(ctx, dbData.Rsw, foundRoot)
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
func resetDocs(ctx context.Context, dbData env.DbData, workingRoot, headRoot, staged *doltdb.RootValue, docs doltdocs.Docs) (newStgRoot *doltdb.RootValue, err error) {
	docs, err = doltdocs.GetDocsFromRoot(ctx, headRoot, doltdocs.GetDocNamesFromDocs(docs)...)
	if err != nil {
		return nil, err
	}

	workingRoot, err = doltdocs.UpdateRootWithDocs(ctx, workingRoot, docs)
	if err != nil {
		return nil, err
	}

	err = env.UpdateWorkingRoot(ctx, dbData.Rsw, workingRoot)
	if err != nil {
		return nil, err
	}

	return doltdocs.UpdateRootWithDocs(ctx, staged, docs)
}

func resetStaged(ctx context.Context, ddb *doltdb.DoltDB, rsw env.RepoStateWriter, tbls []string, staged, head *doltdb.RootValue) (*doltdb.RootValue, error) {
	updatedRoot, err := MoveTablesBetweenRoots(ctx, tbls, head, staged)

	if err != nil {
		return nil, err
	}

	return updatedRoot, env.UpdateStagedRootWithVErr(ddb, rsw, updatedRoot)
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
