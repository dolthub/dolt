// Copyright 2023 Dolthub, Inc.
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

package dprocedures

import (
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var ErrEmptyCherryPick = errors.New("cannot cherry-pick empty string")
var ErrCherryPickUncommittedChanges = errors.New("cannot cherry-pick with uncommitted changes")
var ErrCherryPickConflictsOrViolations = errors.New("error: Unable to apply commit cleanly due to conflicts " +
	"or constraint violations. Please resolve the conflicts and/or constraint violations, then call `dolt_add()` " +
	"to add the tables to the staged set, then call `dolt_commit()` to commit the changes and finish cherry-picking. \n" +
	"To undo all changes from this cherry-pick operation, call `dolt_cherry_pick('--abort')`.\n" +
	"For more information on handling conflicts, see: https://docs.dolthub.com/concepts/dolt/git/conflicts")

// doltCherryPick is the stored procedure version for the CLI command `dolt cherry-pick`.
func doltCherryPick(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	newCommitHash, err := doDoltCherryPick(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(newCommitHash), nil
}

func doDoltCherryPick(ctx *sql.Context, args []string) (string, error) {
	// Get the information for the sql context.
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return "", fmt.Errorf("error: empty database name")
	}

	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return "", err
	}

	apr, err := cli.CreateCherryPickArgParser().Parse(args)
	if err != nil {
		return "", err
	}

	dSess := dsess.DSessFromSess(ctx.Session)

	if apr.Contains(cli.AbortParam) {
		ws, err := dSess.WorkingSet(ctx, dbName)
		if err != nil {
			return "", fmt.Errorf("fatal: unable to load working set: %v", err)
		}

		if !ws.MergeActive() {
			return "", fmt.Errorf("error: There is no cherry-pick merge to abort")
		}

		roots, ok := dSess.GetRoots(ctx, dbName)
		if !ok {
			return "", fmt.Errorf("fatal: unable to load roots for %s", dbName)
		}

		newWs, err := abortMerge(ctx, ws, roots)
		if err != nil {
			return "", fmt.Errorf("fatal: unable to abort merge: %v", err)
		}

		return "", dSess.SetWorkingSet(ctx, dbName, newWs)
	}

	// we only support cherry-picking a single commit for now.
	if apr.NArg() == 0 {
		return "", ErrEmptyCherryPick
	} else if apr.NArg() > 1 {
		return "", fmt.Errorf("cherry-picking multiple commits is not supported yet")
	}

	cherryStr := apr.Arg(0)
	if len(cherryStr) == 0 {
		return "", ErrEmptyCherryPick
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return "", sql.ErrDatabaseNotFound.New(dbName)
	}

	mergeResult, commitMsg, err := cherryPick(ctx, dSess, roots, dbName, cherryStr)
	if err != nil {
		return "", err
	}

	newWorkingRoot := mergeResult.Root
	err = dSess.SetRoot(ctx, dbName, newWorkingRoot)
	if err != nil {
		return "", err
	}

	for tableName, mergeStats := range mergeResult.Stats {
		if !mergeStats.HasArtifacts() {
			res, err := doDoltAdd(ctx, []string{tableName})
			if err != nil {
				return "", err
			}
			if res != 0 {
				return "", fmt.Errorf("dolt add failed")
			}
		}
	}

	if mergeResult.HasMergeArtifacts() {
		return "", ErrCherryPickConflictsOrViolations
	} else {
		commitHash, _, err := doDoltCommit(ctx, []string{"-m", commitMsg})
		return commitHash, err
	}
}

// cherryPick checks that the current working set is clean, verifies the cherry-pick commit is not a merge commit
// or a commit without parent commit, performs merge and returns the new working set root value and
// the commit message of cherry-picked commit as the commit message of the new commit created during this command.
func cherryPick(ctx *sql.Context, dSess *dsess.DoltSession, roots doltdb.Roots, dbName, cherryStr string) (*merge.Result, string, error) {
	// check for clean working set
	headRootHash, err := roots.Head.HashOf()
	if err != nil {
		return nil, "", err
	}

	workingRootHash, err := roots.Working.HashOf()
	if err != nil {
		return nil, "", err
	}
	if workingRootHash != headRootHash {
		return nil, "", ErrCherryPickUncommittedChanges
	}

	stagedRootHash, err := roots.Staged.HashOf()
	if err != nil {
		return nil, "", err
	}
	if stagedRootHash != headRootHash {
		return nil, "", ErrCherryPickUncommittedChanges
	}

	doltDB, ok := dSess.GetDoltDB(ctx, dbName)
	if !ok {
		return nil, "", fmt.Errorf("failed to get DoltDB")
	}

	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return nil, "", fmt.Errorf("failed to get dbData")
	}

	cherryCommitSpec, err := doltdb.NewCommitSpec(cherryStr)
	if err != nil {
		return nil, "", err
	}
	headRef, err := dbData.Rsr.CWBHeadRef()
	if err != nil {
		return nil, "", err
	}
	cherryCommit, err := doltDB.Resolve(ctx, cherryCommitSpec, headRef)
	if err != nil {
		return nil, "", err
	}

	if len(cherryCommit.DatasParents()) > 1 {
		return nil, "", fmt.Errorf("cherry-picking a merge commit is not supported")
	}
	if len(cherryCommit.DatasParents()) == 0 {
		return nil, "", fmt.Errorf("cherry-picking a commit without parents is not supported")
	}

	cherryRoot, err := cherryCommit.GetRootValue(ctx)
	if err != nil {
		return nil, "", err
	}

	parentCommit, err := doltDB.ResolveParent(ctx, cherryCommit, 0)
	if err != nil {
		return nil, "", err
	}
	parentRoot, err := parentCommit.GetRootValue(ctx)
	if err != nil {
		return nil, "", err
	}

	dbState, ok, err := dSess.LookupDbState(ctx, dbName)
	if err != nil {
		return nil, "", err
	} else if !ok {
		return nil, "", sql.ErrDatabaseNotFound.New(dbName)
	}

	// use parent of cherry-pick as ancestor root to merge

	mo := merge.MergeOpts{
		IsCherryPick:        true,
		KeepSchemaConflicts: false,
	}
	result, err := merge.MergeRoots(ctx, roots.Working, cherryRoot, parentRoot, cherryCommit, parentCommit, dbState.EditOpts(), mo)
	if err != nil {
		return nil, "", err
	}

	workingRootHash, err = result.Root.HashOf()
	if err != nil {
		return nil, "", err
	}

	if headRootHash.Equal(workingRootHash) {
		return nil, "", fmt.Errorf("no changes were made, nothing to commit")
	}

	cherryCommitMeta, err := cherryCommit.GetCommitMeta(ctx)
	if err != nil {
		return nil, "", err
	}

	// If any of the merge stats show a data or schema conflict or a constraint
	// violation, record that a merge is in progress.
	for _, stats := range result.Stats {
		if stats.HasArtifacts() {
			ws, err := dSess.WorkingSet(ctx, dbName)
			if err != nil {
				return nil, "", err
			}
			newWorkingSet := ws.StartMerge(cherryCommit, cherryStr)
			err = dSess.SetWorkingSet(ctx, dbName, newWorkingSet)
			if err != nil {
				return nil, "", err
			}

			break
		}
	}

	return result, cherryCommitMeta.Description, nil
}
