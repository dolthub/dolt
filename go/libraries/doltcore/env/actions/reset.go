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
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

func resetHardTables(ctx context.Context, dbData env.DbData, apr *argparser.ArgParseResults, workingRoot, stagedRoot, headRoot *doltdb.RootValue) (*doltdb.Commit, errhand.VerboseError) {
	if apr.NArg() > 1 {
		return nil, errhand.BuildDError("--%s supports at most one additional param", "hard").SetPrintUsage().Build()
	}

	ddb := dbData.Ddb
	rsr := dbData.Rsr
	rsw := dbData.Rsw

	var newHead *doltdb.Commit
	if apr.NArg() == 1 {
		cs, err := doltdb.NewCommitSpec(apr.Arg(0))
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}

		newHead, err = ddb.Resolve(ctx, cs, rsr.CWBHeadRef())
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}

		headRoot, err = newHead.GetRootValue()
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
	}

	// need to save the state of files that aren't tracked
	untrackedTables := make(map[string]*doltdb.Table)
	wTblNames, err := workingRoot.GetTableNames(ctx)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to read tables from the working set").AddCause(err).Build()
	}

	for _, tblName := range wTblNames {
		untrackedTables[tblName], _, err = workingRoot.GetTable(ctx, tblName)

		if err != nil {
			return nil, errhand.BuildDError("error: failed to read '%s' from the working set", tblName).AddCause(err).Build()
		}
	}

	headTblNames, err := stagedRoot.GetTableNames(ctx)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to read tables from head").AddCause(err).Build()
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
			return nil, errhand.BuildDError("error: failed to write table back to database").Build()
		}
	}

	_, err = env.UpdateWorkingRoot(ctx, ddb, rsw, newWkRoot)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to update the working tables.").AddCause(err).Build()
	}

	_, err = env.UpdateStagedRoot(ctx, ddb, rsw, headRoot)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to update the staged tables.").AddCause(err).Build()
	}

	return newHead, nil
}

func ResetHardTables(ctx context.Context, dbData env.DbData, apr *argparser.ArgParseResults, workingRoot, stagedRoot, headRoot *doltdb.RootValue) errhand.VerboseError {
	newHead, verr := resetHardTables(ctx, dbData, apr, workingRoot, stagedRoot, headRoot)

	if verr != nil {
		return verr
	}

	ddb := dbData.Ddb
	rsr := dbData.Rsr

	if newHead != nil {
		if err := ddb.SetHeadToCommit(ctx, rsr.CWBHeadRef(), newHead); err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	return nil
}

func ResetHard(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, workingRoot, stagedRoot, headRoot *doltdb.RootValue) errhand.VerboseError {
	dbData := dEnv.DbData()

	newHead, verr := resetHardTables(ctx, dbData, apr,workingRoot, stagedRoot, headRoot)

	if verr != nil {
		return verr
	}

	err := SaveTrackedDocsFromWorking(ctx, dEnv)
	if err != nil {
		return errhand.BuildDError("error: failed to update docs on the filesystem.").AddCause(err).Build()
	}

	ddb := dbData.Ddb
	rsr := dbData.Rsr

	if newHead != nil {
		if err = ddb.SetHeadToCommit(ctx, rsr.CWBHeadRef(), newHead); err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	return nil
}

func ResetSoftTables(ctx context.Context, dbData env.DbData, apr *argparser.ArgParseResults, stagedRoot, headRoot *doltdb.RootValue) (*doltdb.RootValue, errhand.VerboseError) {
	tables, verr := getUnionedTables(ctx, apr.Args(), stagedRoot, headRoot)

	if verr != nil {
		return nil, verr
	}

	verr = validateTablesWithVErr(tables, stagedRoot, headRoot)

	if verr != nil {
		return nil, verr
	}

	stagedRoot, verr = resetStaged(ctx, dbData.Ddb, dbData.Rsw, tables, stagedRoot, headRoot)

	if verr != nil {
		return nil, verr
	}

	return stagedRoot, nil
}

func ResetSoft(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, stagedRoot, headRoot *doltdb.RootValue) (*doltdb.RootValue, errhand.VerboseError) {
	tables, verr := getUnionedTables(ctx, apr.Args(), stagedRoot, headRoot)

	if verr != nil {
		return nil, verr
	}

	dbData := dEnv.DbData()
	tables, docs, err := GetTblsAndDocDetails(dbData.Drw, tables)
	if err != nil {
		return nil, errhand.BuildDError("error: failed to get all tables").AddCause(err).Build()
	}

	if len(docs) > 0 {
		tables = RemoveDocsTbl(tables)
	}

	verr = validateTablesWithVErr(tables, stagedRoot, headRoot)

	if verr != nil {
		return nil, verr
	}

	stagedRoot, err = resetDocs(ctx, dEnv, headRoot, docs)
	if err != nil {
		return nil, errhand.BuildDError("error: failed to reset docs").AddCause(err).Build()
	}


	stagedRoot, verr = resetStaged(ctx, dbData.Ddb, dbData.Rsw, tables, stagedRoot, headRoot)

	if verr != nil {
		return nil, verr
	}

	return stagedRoot, nil
}

func getUnionedTables(ctx context.Context, tables []string, stagedRoot, headRoot *doltdb.RootValue) ([]string, errhand.VerboseError){
	if len(tables) == 0 || (len(tables) == 1 && tables[0] == ".") {
		var err error
		tables, err = doltdb.UnionTableNames(ctx, stagedRoot, headRoot)

		if err != nil {
			return nil, errhand.BuildDError("error: failed to get all tables").AddCause(err).Build()
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


func resetStaged(ctx context.Context, ddb *doltdb.DoltDB, rsw env.RepoStateWriter, tbls []string, staged, head *doltdb.RootValue) (*doltdb.RootValue, errhand.VerboseError) {
	updatedRoot, err := MoveTablesBetweenRoots(ctx, tbls, head, staged)

	if err != nil {
		tt := strings.Join(tbls, ", ")
		return nil, errhand.BuildDError("error: failed to unstage tables: %s", tt).AddCause(err).Build()
	}

	return updatedRoot, env.UpdateStagedRootWithVErr(ddb, rsw, updatedRoot)
}

func validateTablesWithVErr(tbls []string, roots ...*doltdb.RootValue) errhand.VerboseError {
	err := ValidateTables(context.TODO(), tbls, roots...)

	if err != nil {
		if IsTblNotExist(err) {
			tbls := GetTablesForError(err)
			bdr := errhand.BuildDError("Invalid Table(s):")

			for _, tbl := range tbls {
				bdr.AddDetails("\t" + tbl)
			}

			return bdr.Build()
		} else {
			return errhand.BuildDError("fatal: " + err.Error()).Build()
		}
	}

	return nil
}
