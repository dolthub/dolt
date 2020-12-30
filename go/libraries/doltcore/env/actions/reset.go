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
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"strings"
)

func ResetHard(ctx context.Context, dbData env.DbData, apr *argparser.ArgParseResults, workingRoot, stagedRoot, headRoot *doltdb.RootValue) errhand.VerboseError {
	// TODO: Reset that hard additional param
	if apr.NArg() > 1 {
		return errhand.BuildDError("--%s supports at most one additional param", "hard").SetPrintUsage().Build()
	}

	ddb := dbData.Ddb
	rsr := dbData.Rsr
	rsw := dbData.Rsw

	var newHead *doltdb.Commit
	if apr.NArg() == 1 {
		cs, err := doltdb.NewCommitSpec(apr.Arg(0))
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}

		newHead, err = ddb.Resolve(ctx, cs, rsr.CWBHeadRef())
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}

		headRoot, err = newHead.GetRootValue()
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	// need to save the state of files that aren't tracked
	untrackedTables := make(map[string]*doltdb.Table)
	wTblNames, err := workingRoot.GetTableNames(ctx)

	if err != nil {
		return errhand.BuildDError("error: failed to read tables from the working set").AddCause(err).Build()
	}

	for _, tblName := range wTblNames {
		untrackedTables[tblName], _, err = workingRoot.GetTable(ctx, tblName)

		if err != nil {
			return errhand.BuildDError("error: failed to read '%s' from the working set", tblName).AddCause(err).Build()
		}
	}

	headTblNames, err := stagedRoot.GetTableNames(ctx)

	if err != nil {
		return errhand.BuildDError("error: failed to read tables from head").AddCause(err).Build()
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
			return errhand.BuildDError("error: failed to write table back to database").Build()
		}
	}

	// TODO: update working and staged in one repo_state write.
	_, err = env.UpdateWorkingRoot(ctx, ddb, rsw, newWkRoot)

	if err != nil {
		return errhand.BuildDError("error: failed to update the working tables.").AddCause(err).Build()
	}

	_, err = env.UpdateStagedRoot(ctx, ddb, rsw, headRoot)

	if err != nil {
		return errhand.BuildDError("error: failed to update the staged tables.").AddCause(err).Build()
	}

	err = SaveTrackedDocsFromWorking(ctx, dbData)
	if err != nil {
		return errhand.BuildDError("error: failed to update docs on the filesystem.").AddCause(err).Build()
	}

	if newHead != nil {
		if err = ddb.SetHeadToCommit(ctx, rsr.CWBHeadRef(), newHead); err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	return nil
}


func ResetSoft(ctx context.Context, dbData env.DbData, apr *argparser.ArgParseResults, stagedRoot, headRoot *doltdb.RootValue) (*doltdb.RootValue, errhand.VerboseError) {
	ddb := dbData.Ddb
	rsw := dbData.Rsw
	drw := dbData.Drw

	tbls := apr.Args()

	if len(tbls) == 0 || (len(tbls) == 1 && tbls[0] == ".") {
		var err error
		tbls, err = doltdb.UnionTableNames(ctx, stagedRoot, headRoot)

		if err != nil {
			return nil, errhand.BuildDError("error: failed to get all tables").AddCause(err).Build()
		}
	}

	tables, docs, err := GetTblsAndDocDetails(drw, tbls)
	if err != nil {
		return nil, errhand.BuildDError("error: failed to get all tables").AddCause(err).Build()
	}

	if len(docs) > 0 {
		tables = RemoveDocsTbl(tables)
	}

	verr := validateTablesWithVErr(tables, stagedRoot, headRoot)

	if verr != nil {
		return nil, verr
	}

	stagedRoot, err = resetDocs(ctx, drw, headRoot, docs)
	if err != nil {
		return nil, errhand.BuildDError("error: failed to reset docs").AddCause(err).Build()
	}

	stagedRoot, verr = resetStaged(ctx, ddb, rsw, tables, stagedRoot, headRoot)

	if verr != nil {
		return nil, verr
	}

	return stagedRoot, nil
}

func resetDocs(ctx context.Context, drw env.DocsReadWriter, headRoot *doltdb.RootValue, docDetails env.Docs) (newStgRoot *doltdb.RootValue, err error) {
	docs, err := drw.GetDocsWithNewerTextFromRoot(ctx, headRoot, docDetails)
	if err != nil {
		return nil, err
	}

	err = drw.PutDocsToWorking(ctx, docs)

	if err != nil {
		return nil, err
	}

	return drw.PutDocsToStaged(ctx, docs)
}

func resetStaged(ctx context.Context, ddb *doltdb.DoltDB, rsw env.RepoStateWriter, tbls []string, staged, head *doltdb.RootValue) (*doltdb.RootValue, errhand.VerboseError) {
	updatedRoot, err := MoveTablesBetweenRoots(ctx, tbls, head, staged)

	if err != nil {
		tt := strings.Join(tbls, ", ")
		return nil, errhand.BuildDError("error: failed to unstage tables: %s", tt).AddCause(err).Build()
	}

	// return updatedRoot, commands.UpdateStagedWithVErr(ddb, rsw, updatedRoot)
	return updatedRoot, nil
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
