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
