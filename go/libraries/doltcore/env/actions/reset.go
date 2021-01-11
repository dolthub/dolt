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
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

func resetHardTables(ctx context.Context, dbData env.DbData, apr *argparser.ArgParseResults, workingRoot, stagedRoot, headRoot *doltdb.RootValue) (*doltdb.Commit, error) {
	if apr.NArg() > 1 {
		return nil, errors.New("--hard supports at most one additional param")
	}

	ddb := dbData.Ddb
	rsr := dbData.Rsr
	rsw := dbData.Rsw

	var newHead *doltdb.Commit
	if apr.NArg() == 1 {
		cs, err := doltdb.NewCommitSpec(apr.Arg(0))
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

	_, err = env.UpdateWorkingRoot(ctx, ddb, rsw, newWkRoot)

	if err != nil {
		return nil, err
	}

	_, err = env.UpdateStagedRoot(ctx, ddb, rsw, headRoot)

	if err != nil {
		return nil, err
	}

	return newHead, nil
}

func ResetHardTables(ctx context.Context, dbData env.DbData, apr *argparser.ArgParseResults, workingRoot, stagedRoot, headRoot *doltdb.RootValue) error {
	newHead, err := resetHardTables(ctx, dbData, apr, workingRoot, stagedRoot, headRoot)

	if err != nil {
		return err
	}

	ddb := dbData.Ddb
	rsr := dbData.Rsr

	if newHead != nil {
		if err := ddb.SetHeadToCommit(ctx, rsr.CWBHeadRef(), newHead); err != nil {
			return err
		}
	}

	return nil
}

func ResetHard(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, workingRoot, stagedRoot, headRoot *doltdb.RootValue) error {
	dbData := dEnv.DbData()

	newHead, err := resetHardTables(ctx, dbData, apr, workingRoot, stagedRoot, headRoot)

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

func ResetSoft(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, stagedRoot, headRoot *doltdb.RootValue) (*doltdb.RootValue, error) {
	tables, err := getUnionedTables(ctx, apr.Args(), stagedRoot, headRoot)

	if err != nil {
		return nil, err
	}

	dbData := dEnv.DbData()
	tables, docs, err := GetTblsAndDocDetails(dbData.Drw, tables)
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

	stagedRoot, err = resetDocs(ctx, dEnv, headRoot, docs)
	if err != nil {
		return nil, err
	}

	stagedRoot, err = resetStaged(ctx, dbData.Ddb, dbData.Rsw, tables, stagedRoot, headRoot)

	if err != nil {
		return nil, err
	}

	return stagedRoot, nil
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

func resetDocs(ctx context.Context, dEnv *env.DoltEnv, headRoot *doltdb.RootValue, docDetails env.Docs) (newStgRoot *doltdb.RootValue, err error) {
	docs, err := dEnv.GetDocsWithNewerTextFromRoot(ctx, headRoot, docDetails)
	if err != nil {
		return nil, err
	}

	err = dEnv.PutDocsToWorking(ctx, docs)
	if err != nil {
		return nil, err
	}

	return dEnv.PutDocsToStaged(ctx, docs)
}

func resetStaged(ctx context.Context, ddb *doltdb.DoltDB, rsw env.RepoStateWriter, tbls []string, staged, head *doltdb.RootValue) (*doltdb.RootValue, error) {
	updatedRoot, err := MoveTablesBetweenRoots(ctx, tbls, head, staged)

	if err != nil {
		return nil, err
	}

	return updatedRoot, env.UpdateStagedRootWithVErr(ddb, rsw, updatedRoot)
}
