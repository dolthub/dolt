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

package cherry_pick

import (
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// ErrCherryPickUncommittedChanges is returned when a cherry-pick is attempted without a clean working set.
var ErrCherryPickUncommittedChanges = errors.New("cannot cherry-pick with uncommitted changes")

// CherryPickOptions specifies optional parameters specifying how a cherry-pick is performed.
type CherryPickOptions struct {
	// Amend controls whether the commit at HEAD is amended and combined with the commit to be cherry-picked.
	Amend bool

	// CommitMessage is optional, and controls the message for the new commit.
	CommitMessage string
}

// CherryPick replays a commit, specified by |options.Commit|, and applies it as a new commit to the current HEAD. If
// successful, the hash of the new commit is returned. If the cherry-pick results in merge conflicts, the merge result
// is returned. If any unexpected error occur, it is returned.
func CherryPick(ctx *sql.Context, commit string, options CherryPickOptions) (string, *merge.Result, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	dbName := ctx.GetCurrentDatabase()

	roots, ok := doltSession.GetRoots(ctx, dbName)
	if !ok {
		return "", nil, fmt.Errorf("failed to get roots for current session")
	}

	mergeResult, commitMsg, err := cherryPick(ctx, doltSession, roots, dbName, commit)
	if err != nil {
		return "", mergeResult, err
	}

	newWorkingRoot := mergeResult.Root
	err = doltSession.SetRoot(ctx, dbName, newWorkingRoot)
	if err != nil {
		return "", nil, err
	}

	err = stageCherryPickedTables(ctx, mergeResult.Stats)
	if err != nil {
		return "", nil, err
	}

	// If there were merge conflicts, just return the merge result.
	if mergeResult.HasMergeArtifacts() {
		return "", mergeResult, nil
	}

	commitProps := actions.CommitStagedProps{
		Date:    ctx.QueryTime(),
		Name:    ctx.Client().User,
		Email:   fmt.Sprintf("%s@%s", ctx.Client().User, ctx.Client().Address),
		Message: commitMsg,
	}

	if options.CommitMessage != "" {
		commitProps.Message = options.CommitMessage
	}
	if options.Amend {
		commitProps.Amend = true
	}

	// NOTE: roots are old here (after staging the tables) and need to be refreshed
	roots, ok = doltSession.GetRoots(ctx, dbName)
	if !ok {
		return "", nil, fmt.Errorf("failed to get roots for current session")
	}

	pendingCommit, err := doltSession.NewPendingCommit(ctx, dbName, roots, commitProps)
	if err != nil {
		return "", nil, err
	}
	if pendingCommit == nil {
		return "", nil, errors.New("nothing to commit")
	}

	newCommit, err := doltSession.DoltCommit(ctx, dbName, doltSession.GetTransaction(), pendingCommit)
	if err != nil {
		return "", nil, err
	}

	h, err := newCommit.HashOf()
	if err != nil {
		return "", nil, err
	}

	return h.String(), nil, nil
}

// AbortCherryPick aborts a cherry-pick merge, if one is in progress. If unable to abort for any reason
// (e.g. if there is not cherry-pick merge in progress), an error is returned.
func AbortCherryPick(ctx *sql.Context, dbName string) error {
	doltSession := dsess.DSessFromSess(ctx.Session)

	ws, err := doltSession.WorkingSet(ctx, dbName)
	if err != nil {
		return fmt.Errorf("fatal: unable to load working set: %v", err)
	}

	if !ws.MergeActive() {
		return fmt.Errorf("error: There is no cherry-pick merge to abort")
	}

	roots, ok := doltSession.GetRoots(ctx, dbName)
	if !ok {
		return fmt.Errorf("fatal: unable to load roots for %s", dbName)
	}

	newWs, err := merge.AbortMerge(ctx, ws, roots)
	if err != nil {
		return fmt.Errorf("fatal: unable to abort merge: %v", err)
	}

	return doltSession.SetWorkingSet(ctx, dbName, newWs)
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
		return nil, "", ErrCherryPickUncommittedChanges
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
		return result, "", err
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

// stageCherryPickedTables stages the tables from |mergeStats| that don't have any merge artifacts – i.e.
// tables that don't have any data or schema conflicts and don't have any constraint violations.
func stageCherryPickedTables(ctx *sql.Context, mergeStats map[string]*merge.MergeStats) (err error) {
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

	doltSession := dsess.DSessFromSess(ctx.Session)
	dbName := ctx.GetCurrentDatabase()
	roots, ok := doltSession.GetRoots(ctx, dbName)
	if !ok {
		return fmt.Errorf("unable to get roots for database '%s' from session", dbName)
	}

	roots, err = actions.StageTables(ctx, roots, tablesToAdd, true)
	if err != nil {
		return err
	}

	return doltSession.SetRoots(ctx, dbName, roots)
}
