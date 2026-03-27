// Copyright 2026 Dolthub, Inc.
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

package revert

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

var ErrRevertUncommittedChanges = fmt.Errorf("error: Your local changes would be overwritten by revert.\nhint: Please commit your changes before you revert.")

// Revert applies the inverse of the changes introduced by |commitSpecStr| onto the current working set, creating a
// new commit if successful. If conflicts are encountered the working set is left with the conflicts and the merge
// state is saved so the user can resolve them and call ContinueRevert or AbortRevert.
//
// Returns the new commit hash on success (empty string if conflicts were encountered), the merge result (non-nil
// when conflicts exist), and any error.
func Revert(ctx *sql.Context, commitSpecStr string, authorName, authorEmail string) (string, *merge.Result, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	dbName := ctx.GetCurrentDatabase()

	roots, ok := doltSession.GetRoots(ctx, dbName)
	if !ok {
		return "", nil, fmt.Errorf("failed to get roots for current session")
	}

	wsOnlyHasIgnoredTables, err := diff.WorkingSetContainsOnlyIgnoredTables(ctx, roots)
	if err != nil {
		return "", nil, err
	}
	if !wsOnlyHasIgnoredTables {
		return "", nil, ErrRevertUncommittedChanges
	}

	tableResolver, err := dsess.GetTableResolver(ctx, dbName)
	if err != nil {
		return "", nil, err
	}

	ddb, ok := doltSession.GetDoltDB(ctx, dbName)
	if !ok {
		return "", nil, fmt.Errorf("failed to get dolt database")
	}

	headRef, err := doltSession.CWBHeadRef(ctx, dbName)
	if err != nil {
		return "", nil, err
	}

	commitSpec, err := doltdb.NewCommitSpec(commitSpecStr)
	if err != nil {
		return "", nil, err
	}

	optCmt, err := ddb.Resolve(ctx, commitSpec, headRef)
	if err != nil {
		return "", nil, err
	}
	commit, ok := optCmt.ToCommit()
	if !ok {
		return "", nil, doltdb.ErrGhostCommitEncountered
	}

	dbState, ok, err := doltSession.LookupDbState(ctx, dbName)
	if err != nil {
		return "", nil, err
	} else if !ok {
		return "", nil, fmt.Errorf("could not load database state for %s", dbName)
	}

	// Capture the pre-revert working set BEFORE making any changes, so that
	// AbortRevert can restore the correct clean state if the user calls --abort.
	preRevertWs, err := doltSession.WorkingSet(ctx, dbName)
	if err != nil {
		return "", nil, err
	}

	mergeResult, revertMessage, err := revertCommit(ctx, tableResolver, ddb, roots.Working, commit, dbState.EditOpts())
	if err != nil {
		return "", nil, err
	}

	err = doltSession.SetWorkingRoot(ctx, dbName, mergeResult.Root)
	if err != nil {
		return "", nil, err
	}

	err = stageRevertedTables(ctx, mergeResult.Stats)
	if err != nil {
		return "", nil, err
	}

	if mergeResult.HasMergeArtifacts() {
		// Get the working set after staging, then rebuild it using the pre-revert
		// base so that preMergeWorking correctly captures the clean pre-revert root.
		ws, err := doltSession.WorkingSet(ctx, dbName)
		if err != nil {
			return "", nil, err
		}
		newWs := preRevertWs.StartRevert(commit, commitSpecStr).
			WithWorkingRoot(ws.WorkingRoot()).
			WithStagedRoot(ws.StagedRoot())

		// StartTransaction must be called BEFORE SetWorkingSet. StartTransaction
		// calls d.clear() which wipes in-memory session state, so any working set
		// changes applied before it would be lost.
		if doltSession.GetTransaction() == nil {
			if _, err = doltSession.StartTransaction(ctx, sql.ReadWrite); err != nil {
				return "", nil, err
			}
		}

		if err = doltSession.SetWorkingSet(ctx, dbName, newWs); err != nil {
			return "", nil, err
		}

		// Persist the conflict state so the user can resolve it and call --continue.
		//
		// When autocommit is on and no conflict-override session vars are set, we must
		// commit the transaction explicitly with those vars temporarily enabled, because
		// a plain autocommit would reject a working set that contains conflicts.
		//
		// If the caller has already set @@dolt_allow_commit_conflicts or
		// @@dolt_force_transaction_commit, the normal autocommit mechanism will handle
		// the SQL transaction commit — no explicit CommitTransaction is needed.
		if needsExplicitConflictCommit(ctx) {
			if err = commitWithConflictsAllowed(ctx, doltSession); err != nil {
				return "", nil, err
			}
		}

		return "", mergeResult, nil
	}

	commitHash, err := createRevertCommit(ctx, dbName, doltSession, revertMessage, authorName, authorEmail)
	if err != nil {
		return "", nil, err
	}

	return commitHash, nil, nil
}

// ContinueRevert continues an in-progress revert after conflicts have been resolved. It checks that all conflicts
// and constraint violations have been resolved and then creates the revert commit.
//
// Returns (commitHash, dataConflicts, schemaConflicts, constraintViolations, error). If any conflicts remain,
// the counts are returned with a nil error so the caller can report them to the user.
func ContinueRevert(ctx *sql.Context, dbName string, authorName, authorEmail string) (string, int, int, int, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)

	ws, err := doltSession.WorkingSet(ctx, dbName)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("fatal: unable to load working set: %w", err)
	}

	if !ws.MergeActive() {
		return "", 0, 0, 0, fmt.Errorf("error: There is no revert in progress")
	}
	if !ws.MergeState().IsRevert() {
		return "", 0, 0, 0, fmt.Errorf("error: There is no revert in progress")
	}

	workingRoot := ws.WorkingRoot()
	stagedRoot := ws.StagedRoot()

	conflictTables, err := doltdb.TablesWithDataConflicts(ctx, workingRoot)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("error: unable to check for conflicts: %w", err)
	}
	dataConflictCount := len(conflictTables)

	schemaConflictCount := len(ws.MergeState().TablesWithSchemaConflicts())

	violationTables, err := doltdb.TablesWithConstraintViolations(ctx, workingRoot)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("error: unable to check for constraint violations: %w", err)
	}
	constraintViolationCount := len(violationTables)

	if dataConflictCount > 0 || schemaConflictCount > 0 || constraintViolationCount > 0 {
		return "", dataConflictCount, schemaConflictCount, constraintViolationCount, nil
	}

	// Check for unstaged changes, but allow ignored tables in the working set.
	// We pass Head=Staged so the diff only examines the Staged→Working delta.
	onlyIgnored, err := diff.WorkingSetContainsOnlyIgnoredTables(ctx, doltdb.Roots{
		Head:    stagedRoot,
		Staged:  stagedRoot,
		Working: workingRoot,
	})
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("error: unable to check for unstaged changes: %w", err)
	}
	if !onlyIgnored {
		return "", 0, 0, 0, fmt.Errorf("error: cannot continue revert with unstaged changes")
	}

	revertCommit := ws.MergeState().Commit()
	if revertCommit == nil {
		return "", 0, 0, 0, fmt.Errorf("error: unable to get original commit from merge state")
	}
	revertMeta, err := revertCommit.GetCommitMeta(ctx)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("error: unable to get commit metadata: %w", err)
	}
	revertMessage := fmt.Sprintf("Revert %q", revertMeta.Description)

	// Clear the merge state before committing. The pending commit is built from
	// the current roots, and clearing merge state first ensures NewPendingCommit
	// doesn't add an extra merge parent. We also need to do this before creating
	// the pending commit so the write session sees a consistent state.
	clearedWs := ws.ClearMerge()
	if err = doltSession.SetWorkingSet(ctx, dbName, clearedWs); err != nil {
		return "", 0, 0, 0, fmt.Errorf("error: failed to clear merge state: %w", err)
	}

	roots, ok := doltSession.GetRoots(ctx, dbName)
	if !ok {
		return "", 0, 0, 0, fmt.Errorf("fatal: unable to load roots for %s", dbName)
	}

	commitProps := actions.CommitStagedProps{
		Message: revertMessage,
		Date:    ctx.QueryTime(),
		// Allow an empty commit: when a user resolves a conflict by keeping the current
		// value, staged == HEAD, so there are no net changes — but the revert commit
		// is still meaningful.
		AllowEmpty: true,
		Name:       authorName,
		Email:      authorEmail,
	}

	pendingCommit, err := doltSession.NewPendingCommit(ctx, dbName, roots, commitProps)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("error: failed to create pending commit: %w", err)
	}
	if pendingCommit == nil {
		return "", 0, 0, 0, fmt.Errorf("error: nothing to commit")
	}

	if doltSession.GetTransaction() == nil {
		if _, err = doltSession.StartTransaction(ctx, sql.ReadWrite); err != nil {
			return "", 0, 0, 0, fmt.Errorf("error: failed to start transaction: %w", err)
		}
	}

	newCommit, err := doltSession.DoltCommit(ctx, dbName, doltSession.GetTransaction(), pendingCommit)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("error: failed to create revert commit: %w", err)
	}

	h, err := newCommit.HashOf()
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("error: failed to get commit hash: %w", err)
	}

	commitHash := h.String()

	return commitHash, 0, 0, 0, nil
}

// AbortRevert aborts an in-progress revert, restoring the working set to its pre-revert state.
func AbortRevert(ctx *sql.Context, dbName string) error {
	doltSession := dsess.DSessFromSess(ctx.Session)

	ws, err := doltSession.WorkingSet(ctx, dbName)
	if err != nil {
		return fmt.Errorf("fatal: unable to load working set: %v", err)
	}

	if !ws.MergeActive() {
		return fmt.Errorf("error: There is no revert in progress")
	}
	if !ws.MergeState().IsRevert() {
		return fmt.Errorf("error: There is no revert in progress")
	}

	roots, ok := doltSession.GetRoots(ctx, dbName)
	if !ok {
		return fmt.Errorf("fatal: unable to load roots for %s", dbName)
	}

	newWs, err := merge.AbortMerge(ctx, ws, roots)
	if err != nil {
		return fmt.Errorf("fatal: unable to abort revert: %v", err)
	}

	return doltSession.SetWorkingSet(ctx, dbName, newWs)
}

// createRevertCommit commits the current staged root with the given revert message and author info.
func createRevertCommit(ctx *sql.Context, dbName string, doltSession *dsess.DoltSession, message, authorName, authorEmail string) (string, error) {
	roots, ok := doltSession.GetRoots(ctx, dbName)
	if !ok {
		return "", fmt.Errorf("fatal: unable to load roots for %s", dbName)
	}

	commitProps := actions.CommitStagedProps{
		Message:    message,
		Date:       ctx.QueryTime(),
		AllowEmpty: false,
		Name:       authorName,
		Email:      authorEmail,
	}

	pendingCommit, err := doltSession.NewPendingCommit(ctx, dbName, roots, commitProps)
	if err != nil {
		return "", err
	}
	if pendingCommit == nil {
		return "", fmt.Errorf("nothing to commit")
	}

	if doltSession.GetTransaction() == nil {
		_, err = doltSession.StartTransaction(ctx, sql.ReadWrite)
		if err != nil {
			return "", err
		}
	}

	newCommit, err := doltSession.DoltCommit(ctx, dbName, doltSession.GetTransaction(), pendingCommit)
	if err != nil {
		return "", err
	}

	h, err := newCommit.HashOf()
	if err != nil {
		return "", err
	}

	return h.String(), nil
}

// stageRevertedTables stages the tables that were successfully merged (no artifacts) as part of a revert.
func stageRevertedTables(ctx *sql.Context, mergeStats map[doltdb.TableName]*merge.MergeStats) error {
	tablesToAdd := make([]doltdb.TableName, 0, len(mergeStats))
	for tableName, stats := range mergeStats {
		if stats.HasArtifacts() {
			continue
		}
		if stats.Operation == merge.TableRemoved {
			tablesToAdd = append([]doltdb.TableName{tableName}, tablesToAdd...)
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

	roots, err := actions.StageTables(ctx, roots, tablesToAdd, true)
	if err != nil {
		return err
	}

	return doltSession.SetRoots(ctx, dbName, roots)
}

// needsExplicitConflictCommit returns true when we must call CommitTransaction explicitly to persist a conflicted
// working set. It returns false when the session already has @@dolt_allow_commit_conflicts or
// @@dolt_force_transaction_commit enabled (so the normal autocommit will succeed), or when autocommit is off
// (so the caller will commit manually).
func needsExplicitConflictCommit(ctx *sql.Context) bool {
	getBool := func(varName string) bool {
		v, err := ctx.GetSessionVariable(ctx, varName)
		if err != nil {
			return false
		}
		i, ok := v.(int8)
		return ok && i == 1
	}
	autocommit := getBool("autocommit")
	if getBool(dsess.AllowCommitConflicts) || getBool(dsess.ForceTransactionCommit) {
		return false
	}
	// If autocommit is off the user controls the commit; no explicit commit needed.
	return autocommit // explicit commit only needed when autocommit=1 and no override vars
}

// commitWithConflictsAllowed persists the current working set by committing the SQL transaction, temporarily enabling
// the session variables that allow commits with data conflicts and constraint violations. Both variables are restored
// to their original values before this function returns, so the caller's session is not permanently modified.
func commitWithConflictsAllowed(ctx *sql.Context, doltSession *dsess.DoltSession) error {
	oldAllowConflicts, err := ctx.GetSessionVariable(ctx, dsess.AllowCommitConflicts)
	if err != nil {
		return err
	}
	oldForceCommit, err := ctx.GetSessionVariable(ctx, dsess.ForceTransactionCommit)
	if err != nil {
		return err
	}

	if err = ctx.SetSessionVariable(ctx, dsess.AllowCommitConflicts, int8(1)); err != nil {
		return err
	}
	if err = ctx.SetSessionVariable(ctx, dsess.ForceTransactionCommit, int8(1)); err != nil {
		ctx.SetSessionVariable(ctx, dsess.AllowCommitConflicts, oldAllowConflicts)
		return err
	}

	commitErr := doltSession.CommitTransaction(ctx, doltSession.GetTransaction())

	ctx.SetSessionVariable(ctx, dsess.AllowCommitConflicts, oldAllowConflicts)
	ctx.SetSessionVariable(ctx, dsess.ForceTransactionCommit, oldForceCommit)

	return commitErr
}

// revertCommit performs a merge that undoes the changes introduced by |commit|.
// It does not return an error when conflicts or constraint violations are encountered.
// Instead, it returns a merge result so the caller can decide how to proceed.
func revertCommit(ctx *sql.Context, tableResolver doltdb.TableResolver, ddb *doltdb.DoltDB, root doltdb.RootValue, commit *doltdb.Commit, opts editor.Options) (*merge.Result, string, error) {
	if len(commit.DatasParents()) == 0 {
		h, err := commit.HashOf()
		if err != nil {
			return nil, "", err
		}
		return nil, "", fmt.Errorf("cannot revert commit with no parents (%s)", h.String())
	}

	baseRoot, err := commit.GetRootValue(ctx)
	if err != nil {
		return nil, "", err
	}
	baseMeta, err := commit.GetCommitMeta(ctx)
	if err != nil {
		return nil, "", err
	}
	revertMessage := fmt.Sprintf("Revert %q", baseMeta.Description)

	optCmt, err := ddb.ResolveParent(ctx, commit, 0)
	if err != nil {
		return nil, "", err
	}
	parentCM, ok := optCmt.ToCommit()
	if !ok {
		return nil, "", doltdb.ErrGhostCommitEncountered
	}

	theirRoot, err := parentCM.GetRootValue(ctx)
	if err != nil {
		return nil, "", err
	}

	result, err := merge.MergeRoots(ctx, tableResolver, root, theirRoot, baseRoot, parentCM, commit, opts, merge.MergeOpts{IsCherryPick: false})
	if err != nil {
		return nil, "", err
	}

	return result, revertMessage, nil
}
