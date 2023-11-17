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
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var ErrEmptyCherryPick = errors.New("cannot cherry-pick empty string")
var ErrCherryPickUncommittedChanges = errors.New("cannot cherry-pick with uncommitted changes")

var cherryPickSchema = []*sql.Column{
	{
		Name:     "hash",
		Type:     gmstypes.LongText,
		Nullable: true,
	},
	{
		Name:     "data_conflicts",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
	{
		Name:     "schema_conflicts",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
	{
		Name:     "constraint_violations",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
}

// doltCherryPick is the stored procedure version for the CLI command `dolt cherry-pick`.
func doltCherryPick(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	newCommitHash, dataConflicts, schemaConflicts, constraintViolations, err := doDoltCherryPick(ctx, false, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(newCommitHash, dataConflicts, schemaConflicts, constraintViolations), nil
}

// TODO: This is a hack to support the squash rebase action. We should clean up the interface to cherry-pick logic
//
//	so that we can call this more cleanly. Having a separate action for cherry-pick, that this code would just
//	call, and that rebase could call, is one way to clean this up.
func doltCherryPickWithAmend(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	newCommitHash, dataConflicts, schemaConflicts, constraintViolations, err := doDoltCherryPick(ctx, true, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(newCommitHash, dataConflicts, schemaConflicts, constraintViolations), nil
}

// doDoltCherryPick attempts to perform a cherry-pick merge based on the arguments specified in |args| and returns
// the new, created commit hash (if it was successful created), a count of the number of tables with data conflicts,
// a count of the number of tables with schema conflicts, and a count of the number of tables with constraint violations.
func doDoltCherryPick(ctx *sql.Context, amend bool, args []string) (string, int, int, int, error) {
	// Get the information for the sql context.
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return "", 0, 0, 0, fmt.Errorf("error: empty database name")
	}

	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return "", 0, 0, 0, err
	}

	apr, err := cli.CreateCherryPickArgParser().Parse(args)
	if err != nil {
		return "", 0, 0, 0, err
	}

	dSess := dsess.DSessFromSess(ctx.Session)

	if apr.Contains(cli.AbortParam) {
		ws, err := dSess.WorkingSet(ctx, dbName)
		if err != nil {
			return "", 0, 0, 0, fmt.Errorf("fatal: unable to load working set: %v", err)
		}

		if !ws.MergeActive() {
			return "", 0, 0, 0, fmt.Errorf("error: There is no cherry-pick merge to abort")
		}

		roots, ok := dSess.GetRoots(ctx, dbName)
		if !ok {
			return "", 0, 0, 0, fmt.Errorf("fatal: unable to load roots for %s", dbName)
		}

		newWs, err := abortMerge(ctx, ws, roots)
		if err != nil {
			return "", 0, 0, 0, fmt.Errorf("fatal: unable to abort merge: %v", err)
		}

		return "", 0, 0, 0, dSess.SetWorkingSet(ctx, dbName, newWs)
	}

	// we only support cherry-picking a single commit for now.
	if apr.NArg() == 0 {
		return "", 0, 0, 0, ErrEmptyCherryPick
	} else if apr.NArg() > 1 {
		return "", 0, 0, 0, fmt.Errorf("cherry-picking multiple commits is not supported yet")
	}

	cherryStr := apr.Arg(0)
	if len(cherryStr) == 0 {
		return "", 0, 0, 0, ErrEmptyCherryPick
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return "", 0, 0, 0, sql.ErrDatabaseNotFound.New(dbName)
	}

	mergeResult, commitMsg, err := cherryPick(ctx, dSess, roots, dbName, cherryStr)
	if err != nil {
		return "", 0, 0, 0, err
	}

	newWorkingRoot := mergeResult.Root
	err = dSess.SetRoot(ctx, dbName, newWorkingRoot)
	if err != nil {
		return "", 0, 0, 0, err
	}

	err = stageCherryPickedTables(ctx, mergeResult.Stats)
	if err != nil {
		return "", 0, 0, 0, err
	}

	if mergeResult.HasMergeArtifacts() {
		return "", mergeResult.CountOfTablesWithDataConflicts(),
			mergeResult.CountOfTablesWithSchemaConflicts(), mergeResult.CountOfTablesWithConstraintViolations(), nil
	} else {
		// TODO: We need a way to control this commit message for the squash rebase action
		args := []string{"-m", commitMsg}
		if amend {
			args = append(args, "--amend")
		}

		commitHash, _, err := doDoltCommit(ctx, args)
		return commitHash, 0, 0, 0, err
	}
}

// stageCherryPickedTables stages the tables from |mergeStats| that don't have any merge artifacts – i.e.
// tables that don't have any data or schema conflicts and don't have any constraint violations.
func stageCherryPickedTables(ctx *sql.Context, mergeStats map[string]*merge.MergeStats) error {
	tablesToAdd := make([]string, 0, len(mergeStats))
	for tableName, mergeStats := range mergeStats {
		if mergeStats.HasArtifacts() {
			continue
		}

		// Find any tables being deleted and make sure we stage those tables first
		if mergeStats.Operation == merge.TableRemoved {
			tablesToAdd = append([]string{tableName}, tablesToAdd...)
		} else {
			tablesToAdd = append(tablesToAdd, tableName)
		}
	}

	for _, tableName := range tablesToAdd {
		res, err := doDoltAdd(ctx, []string{tableName})
		if err != nil {
			return err
		}
		if res != 0 {
			return fmt.Errorf("dolt add failed")
		}
	}

	return nil
}

// cherryPick checks that the current working set is clean, verifies the cherry-pick commit is not a merge commit
// or a commit without parent commit, performs merge and returns the new working set root value and
// the commit message of cherry-picked commit as the commit message of the new commit created during this command.
func cherryPick(ctx *sql.Context, dSess *dsess.DoltSession, roots doltdb.Roots, dbName, cherryStr string) (*merge.Result, string, error) {
	// check for clean working set
	wsOnlyHasIgnoredTables, err := diff.WorkingSetContainsOnlyIgnoredTables(ctx, roots)
	if err != nil {
		return nil, "", err
	}
	if !wsOnlyHasIgnoredTables {
		// TODO: disabling this temporarily for hacking squash rebase support together
		//       If we extracted the cherry-pick logic to an action, we could have additional arguments that let us
		//        control this behavior (or this could even be in just the stored procedure code)
		//	return nil, "", ErrCherryPickUncommittedChanges
	}

	headRootHash, err := roots.Head.HashOf()
	if err != nil {
		return nil, "", err
	}

	workingRootHash, err := roots.Working.HashOf()
	if err != nil {
		return nil, "", err
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

	// When cherry-picking, we need to use the parent of the cherry-picked commit as the ancestor. This
	// ensures that only the delta from the cherry-pick commit is applied.
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
			newWorkingSet := ws.StartCherryPick(cherryCommit, cherryStr)
			err = dSess.SetWorkingSet(ctx, dbName, newWorkingSet)
			if err != nil {
				return nil, "", err
			}

			break
		}
	}

	return result, cherryCommitMeta.Description, nil
}
