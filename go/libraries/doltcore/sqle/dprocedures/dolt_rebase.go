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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	goerrors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/cherry_pick"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/rebase"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var doltRebaseProcedureSchema = []*sql.Column{
	{
		Name:     "status",
		Type:     types.Int64,
		Nullable: false,
	},
	{
		Name:     "message",
		Type:     types.LongText,
		Nullable: true,
	},
}

var RebaseActionEnumType = types.MustCreateEnumType([]string{
	rebase.RebaseActionDrop,
	rebase.RebaseActionPick,
	rebase.RebaseActionReword,
	rebase.RebaseActionSquash,
	rebase.RebaseActionFixup}, sql.Collation_Default)

var DoltRebaseSystemTableSchema = []*sql.Column{
	{
		Name:       "rebase_order",
		Type:       types.MustCreateDecimalType(6, 2),
		Nullable:   false,
		PrimaryKey: true,
	},
	{
		Name:     "action",
		Type:     RebaseActionEnumType,
		Nullable: false,
	},
	{
		Name:     "commit_hash",
		Type:     types.Text,
		Nullable: false,
	},
	{
		Name:     "commit_message",
		Type:     types.Text,
		Nullable: false,
	},
}

// ErrRebaseUncommittedChanges is used when a rebase is started, but there are uncommitted (and not
// ignored) changes in the working set.
var ErrRebaseUncommittedChanges = goerrors.NewKind("cannot start a rebase with uncommitted changes")

// ErrRebaseUnstagedChanges is used when a rebase is continued, but there are unstaged changes
// in the working set.
var ErrRebaseUnstagedChanges = goerrors.NewKind("cannot continue a rebase with unstaged changes. " +
	"Use dolt_add() to stage tables and then continue the rebase")

// ErrRebaseUnresolvedConflicts is used when a rebase is continued, but there are
// unresolved conflicts still present.
var ErrRebaseUnresolvedConflicts = goerrors.NewKind(
	"conflicts detected in tables %s; resolve conflicts before continuing the rebase")

// ErrRebaseDataConflictsCantBeResolved is used when data conflicts are detected in a rebase, but @@autocommit has not
// been disabled or @@dolt_allow_commit_conflicts has not been enabled, so it's not possible to resolve the conflicts
// since they would get rolled back automatically.
var ErrRebaseDataConflictsCantBeResolved = goerrors.NewKind(
	"data conflicts from rebase, but session settings do not allow preserving conflicts, so they cannot be resolved. " +
		"The rebase has been aborted. Set @@autocommit to 0 or set @@dolt_allow_commit_conflicts to 1 and " +
		"try the rebase again to resolve the conflicts.")

// ErrRebaseDataConflict is used when a data conflict is detected while rebasing a commit.
var ErrRebaseDataConflict = goerrors.NewKind("data conflict detected while rebasing commit %s (%s). \n\n" +
	"Resolve the conflicts and remove them from the dolt_conflicts_<table> tables, " +
	"then continue the rebase by calling dolt_rebase('--continue')")

// ErrRebaseSchemaConflict is used when a schema conflict is detected while rebasing a commit.
var ErrRebaseSchemaConflict = goerrors.NewKind(
	"schema conflict detected while rebasing commit %s. " +
		"the rebase has been automatically aborted")

// ErrRebaseConflictWithAbortError is used when a merge conflict is detected while rebasing a commit,
// and we are unable to cleanly abort the rebase.
var ErrRebaseConflictWithAbortError = goerrors.NewKind(
	"merge conflict detected while rebasing commit %s. " +
		"attempted to abort rebase operation, but encountered error: %w")

// SuccessfulRebaseMessage is used when a rebase finishes successfully. The branch that was rebased should be appended
// to the end of the message.
var SuccessfulRebaseMessage = "Successfully rebased and updated refs/heads/"

var RebaseAbortedMessage = "Interactive rebase aborted"

func doltRebase(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, message, err := doDoltRebase(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res), message), nil
}

func doDoltRebase(ctx *sql.Context, args []string) (int, string, error) {
	if ctx.GetCurrentDatabase() == "" {
		return 1, "", sql.ErrNoDatabaseSelected.New()
	}

	apr, err := cli.CreateRebaseArgParser().Parse(args)
	if err != nil {
		return 1, "", err
	}

	switch {
	case apr.Contains(cli.AbortParam):
		err := abortRebase(ctx)
		if err != nil {
			return 1, "", err
		} else {
			return 0, RebaseAbortedMessage, nil
		}

	case apr.Contains(cli.ContinueFlag):
		rebaseBranch, err := continueRebase(ctx)
		if err != nil {
			return 1, "", err
		} else {
			return 0, SuccessfulRebaseMessage + rebaseBranch, nil
		}

	default:
		commitBecomesEmptyHandling, err := processCommitBecomesEmptyParams(apr)
		if err != nil {
			return 1, "", err
		}

		// The default, in rebase, for handling commits that start off empty is to keep them
		// TODO: Add support for --keep-empty and --no-keep-empty flags
		emptyCommitHandling := doltdb.EmptyCommitHandling(doltdb.KeepEmptyCommit)

		if apr.NArg() == 0 {
			return 1, "", fmt.Errorf("not enough args")
		} else if apr.NArg() > 1 {
			return 1, "", fmt.Errorf("too many args")
		}
		if !apr.Contains(cli.InteractiveFlag) {
			return 1, "", fmt.Errorf("non-interactive rebases not currently supported")
		}
		err = startRebase(ctx, apr.Arg(0), commitBecomesEmptyHandling, emptyCommitHandling)
		if err != nil {
			return 1, "", err
		}

		currentBranch, err := currentBranch(ctx)
		if err != nil {
			return 1, "", err
		}

		return 0, fmt.Sprintf("interactive rebase started on branch %s; "+
			"adjust the rebase plan in the dolt_rebase table, then continue rebasing by "+
			"calling dolt_rebase('--continue')", currentBranch), nil
	}
}

// processCommitBecomesEmptyParams examines the parsed arguments in |apr| for the "empty" arg
// and returns the empty commit handling strategy to use when a commit being rebased becomes
// empty. If an invalid argument value is encountered, an error is returned.
func processCommitBecomesEmptyParams(apr *argparser.ArgParseResults) (doltdb.EmptyCommitHandling, error) {
	commitBecomesEmptyParam, isCommitBecomesEmptySpecified := apr.GetValue(cli.EmptyParam)
	if !isCommitBecomesEmptySpecified {
		// If no option is specified, then by default, commits that become empty are dropped. Git has the same
		// default for non-interactive rebases; for interactive rebases, Git uses the default action of "stop" to
		// let the user examine the changes and decide what to do next. We don't support the "stop" action yet, so
		// we default to "drop" even in the interactive rebase case.
		return doltdb.DropEmptyCommit, nil
	}

	if strings.EqualFold(commitBecomesEmptyParam, "keep") {
		return doltdb.KeepEmptyCommit, nil
	} else if strings.EqualFold(commitBecomesEmptyParam, "drop") {
		return doltdb.DropEmptyCommit, nil
	} else {
		return -1, fmt.Errorf("unsupported option for the empty flag (%s); "+
			"only 'keep' or 'drop' are allowed", commitBecomesEmptyParam)
	}
}

// startRebase starts a new interactive rebase operation. |upstreamPoint| specifies the commit where the new rebased
// commits will be based off of, |commitBecomesEmptyHandling| specifies how to  handle commits that are not empty, but
// do not produce any changes when applied, and |emptyCommitHandling| specifies how to handle empty commits.
func startRebase(ctx *sql.Context, upstreamPoint string, commitBecomesEmptyHandling doltdb.EmptyCommitHandling, emptyCommitHandling doltdb.EmptyCommitHandling) error {
	if upstreamPoint == "" {
		return fmt.Errorf("no upstream branch specified")
	}

	err := validateWorkingSetCanStartRebase(ctx)
	if err != nil {
		return err
	}

	doltSession := dsess.DSessFromSess(ctx.Session)
	headRef, err := doltSession.CWBHeadRef(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	dbData, ok := doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return fmt.Errorf("unable to find database %s", ctx.GetCurrentDatabase())
	}

	rebaseBranch, err := currentBranch(ctx)
	if err != nil {
		return err
	}

	startCommit, err := dbData.Ddb.ResolveCommitRef(ctx, ref.NewBranchRef(rebaseBranch))
	if err != nil {
		return err
	}

	commitSpec, err := doltdb.NewCommitSpec(upstreamPoint)
	if err != nil {
		return err
	}

	optCmt, err := dbData.Ddb.Resolve(ctx, commitSpec, headRef)
	if err != nil {
		return err
	}
	upstreamCommit, ok := optCmt.ToCommit()
	if !ok {
		return doltdb.ErrGhostCommitEncountered
	}

	// rebaseWorkingBranch is the name of the temporary branch used when performing a rebase. In Git, a rebase
	// happens with a detached HEAD, but Dolt doesn't support that, we use a temporary branch.
	rebaseWorkingBranch := "dolt_rebase_" + rebaseBranch
	var rsc doltdb.ReplicationStatusController
	err = actions.CreateBranchWithStartPt(ctx, dbData, rebaseWorkingBranch, upstreamPoint, false, &rsc)
	if err != nil {
		return err
	}
	err = commitTransaction(ctx, doltSession, &rsc)
	if err != nil {
		return err
	}

	// Checkout our new branch
	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(rebaseWorkingBranch))
	if err != nil {
		return err
	}
	err = doltSession.SwitchWorkingSet(ctx, ctx.GetCurrentDatabase(), wsRef)
	if err != nil {
		return err
	}

	dbData, ok = doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return fmt.Errorf("unable to get db datata for database %s", ctx.GetCurrentDatabase())
	}

	db, err := doltSession.Provider().Database(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}

	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}

	branchRoots, err := dbData.Ddb.ResolveBranchRoots(ctx, ref.NewBranchRef(rebaseBranch))
	if err != nil {
		return err
	}

	newWorkingSet, err := workingSet.StartRebase(ctx, upstreamCommit, rebaseBranch, branchRoots.Working,
		commitBecomesEmptyHandling, emptyCommitHandling)
	if err != nil {
		return err
	}

	err = doltSession.SetWorkingSet(ctx, ctx.GetCurrentDatabase(), newWorkingSet)
	if err != nil {
		return err
	}

	// Create the rebase plan and save it in the database
	rebasePlan, err := rebase.CreateDefaultRebasePlan(ctx, startCommit, upstreamCommit)
	if err != nil {
		abortErr := abortRebase(ctx)
		if abortErr != nil {
			return fmt.Errorf("%s: unable to cleanly abort rebase: %s", err.Error(), abortErr.Error())
		}
		return err
	}
	rdb, ok := db.(rebase.RebasePlanDatabase)
	if !ok {
		return fmt.Errorf("expected a dsess.RebasePlanDatabase implementation, but received a %T", db)
	}
	return rdb.SaveRebasePlan(ctx, rebasePlan)
}

// validateRebaseBranchHasntChanged checks that the branch being rebased hasn't been updated since the rebase started,
// and returns an error if any changes are detected.
func validateRebaseBranchHasntChanged(ctx *sql.Context, branch string, rebaseState *doltdb.RebaseState) error {
	doltSession := dsess.DSessFromSess(ctx.Session)
	doltDb, ok := doltSession.GetDoltDB(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return fmt.Errorf("unable to access DoltDB for database %s", ctx.GetCurrentDatabase())
	}

	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(branch))
	if err != nil {
		return err
	}

	resolvedWorkingSet, err := doltDb.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return err
	}
	hash2, err := resolvedWorkingSet.StagedRoot().HashOf()
	if err != nil {
		return err
	}
	hash1, err := rebaseState.PreRebaseWorkingRoot().HashOf()
	if err != nil {
		return err
	}
	if hash1 != hash2 {
		return fmt.Errorf("rebase aborted due to changes in branch %s", branch)
	}

	return nil
}

func validateWorkingSetCanStartRebase(ctx *sql.Context) error {
	doltSession := dsess.DSessFromSess(ctx.Session)

	// Make sure there isn't an active rebase or merge in progress already
	ws, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	if ws.MergeActive() {
		return fmt.Errorf("unable to start rebase while a merge is in progress – abort the current merge before proceeding")
	}
	if ws.RebaseActive() {
		return fmt.Errorf("unable to start rebase while another rebase is in progress – abort the current rebase before proceeding")
	}

	// Make sure the working set doesn't contain any uncommitted changes
	roots, ok := doltSession.GetRoots(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return fmt.Errorf("unable to get roots for database %s", ctx.GetCurrentDatabase())
	}
	wsOnlyHasIgnoredTables, err := diff.WorkingSetContainsOnlyIgnoredTables(ctx, roots)
	if err != nil {
		return err
	}
	if !wsOnlyHasIgnoredTables {
		return ErrRebaseUncommittedChanges.New()
	}

	return nil
}

func abortRebase(ctx *sql.Context) error {
	doltSession := dsess.DSessFromSess(ctx.Session)

	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	if !workingSet.RebaseActive() {
		return fmt.Errorf("no rebase in progress")
	}

	// Clear the rebase state (even though we're going to delete this branch next)
	rebaseState := workingSet.RebaseState()
	workingSet = workingSet.AbortRebase()
	err = doltSession.SetWorkingSet(ctx, ctx.GetCurrentDatabase(), workingSet)
	if err != nil {
		return err
	}

	// Delete the working branch
	var rsc doltdb.ReplicationStatusController
	dbData, ok := doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return fmt.Errorf("unable to get DbData for database %s", ctx.GetCurrentDatabase())
	}
	headRef, err := doltSession.CWBHeadRef(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	err = actions.DeleteBranch(ctx, dbData, headRef.GetPath(), actions.DeleteOptions{
		Force:                      true,
		AllowDeletingCurrentBranch: true,
	}, doltSession.Provider(), &rsc)
	if err != nil {
		return err
	}

	// Switch back to the original branch head
	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(rebaseState.Branch()))
	if err != nil {
		return err
	}

	return doltSession.SwitchWorkingSet(ctx, ctx.GetCurrentDatabase(), wsRef)
}

func validateActiveRebase(ctx *sql.Context) error {
	doltSession := dsess.DSessFromSess(ctx.Session)
	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	if !workingSet.RebaseActive() {
		return fmt.Errorf("no rebase in progress")
	}
	return nil
}

func validateNoConflicts(ctx *sql.Context) error {
	doltSession := dsess.DSessFromSess(ctx.Session)
	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}

	tablesWithDataConflicts, err := doltdb.TablesWithDataConflicts(ctx, workingSet.WorkingRoot())
	if err != nil {
		return err
	}
	if len(tablesWithDataConflicts) > 0 {
		return ErrRebaseUnresolvedConflicts.New(
			doltdb.TableNamesAsString(tablesWithDataConflicts))
	}

	return nil
}

// loadRebasePlan loads the rebase plan from the current database for the current session and validates it.
func loadRebasePlan(ctx *sql.Context) (*rebase.RebasePlan, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	db, err := doltSession.Provider().Database(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}

	rdb, ok := db.(rebase.RebasePlanDatabase)
	if !ok {
		return nil, fmt.Errorf("expected a dsess.RebasePlanDatabase implementation, but received a %T", db)
	}
	rebasePlan, err := rdb.LoadRebasePlan(ctx)
	if err != nil {
		return nil, err
	}
	err = rebase.ValidateRebasePlan(ctx, rebasePlan)
	if err != nil {
		return nil, err
	}

	return rebasePlan, nil
}

// filterIgnoredTables iterates over the specified |unstagedTableDeltas|, filters out any TableDeltas for
// ignored tables, and returns the filtered list.
func filterIgnoredTables(ctx *sql.Context, unstagedTableDeltas []diff.TableDelta) ([]diff.TableDelta, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	roots, ok := doltSession.GetRoots(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return nil, fmt.Errorf("unable to get roots for current session")
	}

	filteredTableDeltas := make([]diff.TableDelta, 0, len(unstagedTableDeltas))
	ignorePatterns, err := doltdb.GetIgnoredTablePatterns(ctx, roots)
	if err != nil {
		return nil, err
	}

	for _, tableDelta := range unstagedTableDeltas {
		isIgnored, err := ignorePatterns.IsTableNameIgnored(tableDelta.ToName)
		if err != nil {
			return nil, err
		}
		if isIgnored != doltdb.Ignore {
			filteredTableDeltas = append(filteredTableDeltas, tableDelta)
		}
	}

	return filteredTableDeltas, nil
}

// workingSetStatus checks the workspace status and returns whether there are any staged changes and unstaged changes.
func workingSetStatus(ctx *sql.Context) (stagedChanges, unstagedChanges bool, err error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	roots, ok := doltSession.GetRoots(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return false, false, fmt.Errorf("unable to get roots for current session")
	}

	stagedTables, unstagedTables, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return false, false, err
	}

	unstagedTables, err = filterIgnoredTables(ctx, unstagedTables)
	if err != nil {
		return false, false, err
	}

	return len(stagedTables) > 0, len(unstagedTables) > 0, nil
}

// recordCurrentStep updates working set metadata to record the current rebase plan step number as well
// as the rebase started flag indicating that execution of the rebase plan has been started. This
// information is all stored in the RebaseState of the WorkingSet.
func recordCurrentStep(ctx *sql.Context, step rebase.RebasePlanStep) error {
	doltSession := dsess.DSessFromSess(ctx.Session)
	if doltSession.GetTransaction() == nil {
		_, err := doltSession.StartTransaction(ctx, sql.ReadWrite)
		if err != nil {
			return err
		}
	}

	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}

	// Update the current step in the working set, so that we can continue the merge if this hits a conflict
	newWorkingSet := workingSet.WithRebaseState(workingSet.RebaseState().
		WithLastAttemptedStep(step.RebaseOrderAsFloat()).
		WithRebasingStarted(true))
	err = doltSession.SetWorkingSet(ctx, ctx.GetCurrentDatabase(), newWorkingSet)
	if err != nil {
		return err
	}

	// Commit the SQL transaction with the LastAttemptedStep update to set that in the branch head's working set
	if doltSession.GetTransaction() != nil {
		err = doltSession.CommitTransaction(ctx, doltSession.GetTransaction())
		if err != nil {
			return err
		}
	}

	return nil
}

func continueRebase(ctx *sql.Context) (string, error) {
	// Validate that we are in an interactive rebase
	if err := validateActiveRebase(ctx); err != nil {
		return "", err
	}

	// If there are conflicts, stop the rebase with an error message about resolving the conflicts before continuing
	if err := validateNoConflicts(ctx); err != nil {
		return "", err
	}

	// After we've checked for conflicts in the working set, commit the SQL transaction to ensure
	// our session is in sync with the latest changes on the branch
	doltSession := dsess.DSessFromSess(ctx.Session)
	if doltSession.GetTransaction() == nil {
		_, err := doltSession.StartTransaction(ctx, sql.ReadWrite)
		if err != nil {
			return "", err
		}
	}
	if err := doltSession.CommitTransaction(ctx, doltSession.GetTransaction()); err != nil {
		return "", err
	}

	rebasePlan, err := loadRebasePlan(ctx)
	if err != nil {
		return "", err
	}

	for _, step := range rebasePlan.Steps {
		workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
		if err != nil {
			return "", err
		}

		rebaseStepOrder := step.RebaseOrderAsFloat()
		lastAttemptedStep := workingSet.RebaseState().LastAttemptedStep()
		rebasingStarted := workingSet.RebaseState().RebasingStarted()

		hasStagedChanges, hasUnstagedChanges, err := workingSetStatus(ctx)
		if err != nil {
			return "", err
		}

		// If the rebase is just starting but there are any uncommitted changes in the working set,
		// tell the user they need to commit them before we can start executing the rebase plan.
		if !rebasingStarted && (hasStagedChanges || hasUnstagedChanges) {
			return "", ErrRebaseUncommittedChanges.New()
		}

		// If we've already executed this step, move to the next plan step
		if rebasingStarted && rebaseStepOrder < lastAttemptedStep {
			continue
		}

		// If the rebase is continued, but not all working set changes are staged, then tell the user
		// they need to explicitly stage the tables before the rebase can be continued.
		if hasUnstagedChanges {
			return "", ErrRebaseUnstagedChanges.New()
		}

		// If we've already executed this step, but the working set has staged changes,
		// then we need to make the commit for the manual changes made for this step.
		if rebasingStarted && rebaseStepOrder == lastAttemptedStep && hasStagedChanges {
			if err = commitManuallyStagedChangesForStep(ctx, step); err != nil {
				return "", err
			}
			continue
		}

		// If rebasing hasn't started yet or this step is greater than the last attempted step,
		// go ahead and execute this step.
		if !rebasingStarted || rebaseStepOrder > lastAttemptedStep {
			if err = recordCurrentStep(ctx, step); err != nil {
				return "", err
			}

			doltSession := dsess.DSessFromSess(ctx.Session)
			workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
			if err != nil {
				return "", err
			}

			err = processRebasePlanStep(ctx, &step,
				workingSet.RebaseState().CommitBecomesEmptyHandling(),
				workingSet.RebaseState().EmptyCommitHandling())
			if err != nil {
				return "", err
			}
		}

		// Ensure a transaction has been started, so that the session is in sync with the latest changes
		if doltSession.GetTransaction() == nil {
			_, err = doltSession.StartTransaction(ctx, sql.ReadWrite)
			if err != nil {
				return "", err
			}
		}
	}

	// Update the branch being rebased to point to the same commit as our temporary working branch
	rebaseBranchWorkingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return "", err
	}
	dbData, ok := doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return "", fmt.Errorf("unable to get db data for database %s", ctx.GetCurrentDatabase())
	}

	rebaseBranch := rebaseBranchWorkingSet.RebaseState().Branch()
	rebaseWorkingBranch := "dolt_rebase_" + rebaseBranch

	// Check that the branch being rebased hasn't been updated since the rebase started
	err = validateRebaseBranchHasntChanged(ctx, rebaseBranch, rebaseBranchWorkingSet.RebaseState())
	if err != nil {
		return "", err
	}

	// TODO: copyABranch (and the underlying call to doltdb.NewBranchAtCommit) has a race condition
	//       where another session can set the branch head AFTER doltdb.NewBranchAtCommit updates
	//       the branch head, but BEFORE doltdb.NewBranchAtCommit retrieves the working set for the
	//       branch and updates the working root and staged root for the working set. We may be able
	//       to fix this race condition by changing doltdb.NewBranchAtCommit to use
	//       database.CommitWithWorkingSet, since it updates a branch head and working set atomically.
	err = copyABranch(ctx, dbData, rebaseWorkingBranch, rebaseBranch, true, nil)
	if err != nil {
		return "", err
	}

	// Checkout the branch being rebased
	previousBranchWorkingSetRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(rebaseBranchWorkingSet.RebaseState().Branch()))
	if err != nil {
		return "", err
	}
	err = doltSession.SwitchWorkingSet(ctx, ctx.GetCurrentDatabase(), previousBranchWorkingSetRef)
	if err != nil {
		return "", err
	}

	// Start a new transaction so the session will see the changes to the branch pointer
	if _, err = doltSession.StartTransaction(ctx, sql.ReadWrite); err != nil {
		return "", err
	}

	// delete the temporary working branch
	dbData, ok = doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return "", fmt.Errorf("unable to lookup dbdata")
	}
	return rebaseBranch, actions.DeleteBranch(ctx, dbData, rebaseWorkingBranch, actions.DeleteOptions{
		Force: true,
	}, doltSession.Provider(), nil)
}

// commitManuallyStagedChangesForStep handles committing staged changes after a conflict has been manually
// resolved by the caller before rebasing has been continued. This involves building the correct commit
// message based on the details of the rebase plan |step| and then creating the commit.
func commitManuallyStagedChangesForStep(ctx *sql.Context, step rebase.RebasePlanStep) error {
	doltSession := dsess.DSessFromSess(ctx.Session)
	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}

	options, err := createCherryPickOptionsForRebaseStep(ctx, &step, workingSet.RebaseState().CommitBecomesEmptyHandling(),
		workingSet.RebaseState().EmptyCommitHandling())

	commitProps, err := cherry_pick.CreateCommitStagedPropsFromCherryPickOptions(ctx, *options)
	if err != nil {
		return err
	}

	// If the commit message wasn't set when we created the cherry-pick options, then set it to the step's commit
	// message. For fixup commits, we don't use their commit message, so we keep it empty, and let the amend commit
	// codepath use the previous commit's message.
	if commitProps.Message == "" && step.Action != rebase.RebaseActionFixup {
		commitProps.Message = step.CommitMsg
	}

	roots, ok := doltSession.GetRoots(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return fmt.Errorf("unable to get roots for current session")
	}
	pendingCommit, err := doltSession.NewPendingCommit(ctx, ctx.GetCurrentDatabase(), roots, *commitProps)
	if err != nil {
		return err
	}

	// Ensure a SQL transaction is set in the session
	if doltSession.GetTransaction() == nil {
		if _, err = doltSession.StartTransaction(ctx, sql.ReadWrite); err != nil {
			return err
		}
	}
	_, err = doltSession.DoltCommit(ctx, ctx.GetCurrentDatabase(), doltSession.GetTransaction(), pendingCommit)
	return err
}

func processRebasePlanStep(ctx *sql.Context, planStep *rebase.RebasePlanStep,
	commitBecomesEmptyHandling doltdb.EmptyCommitHandling, emptyCommitHandling doltdb.EmptyCommitHandling) error {
	// Make sure we have a transaction opened for the session
	// NOTE: After our first call to cherry-pick, the tx is committed, so a new tx needs to be started
	//       as we process additional rebase actions.
	doltSession := dsess.DSessFromSess(ctx.Session)
	if doltSession.GetTransaction() == nil {
		_, err := doltSession.StartTransaction(ctx, sql.ReadWrite)
		if err != nil {
			return err
		}
	}

	// If the action is "drop", then we don't need to do anything
	if planStep.Action == rebase.RebaseActionDrop {
		return nil
	}

	options, err := createCherryPickOptionsForRebaseStep(ctx, planStep, commitBecomesEmptyHandling, emptyCommitHandling)
	if err != nil {
		return err
	}

	return handleRebaseCherryPick(ctx, planStep, *options)
}

func createCherryPickOptionsForRebaseStep(ctx *sql.Context, planStep *rebase.RebasePlanStep, commitBecomesEmptyHandling doltdb.EmptyCommitHandling, emptyCommitHandling doltdb.EmptyCommitHandling) (*cherry_pick.CherryPickOptions, error) {
	// Override the default empty commit handling options for cherry-pick, since
	// rebase has slightly different defaults
	options := cherry_pick.NewCherryPickOptions()
	options.CommitBecomesEmptyHandling = commitBecomesEmptyHandling
	options.EmptyCommitHandling = emptyCommitHandling

	switch planStep.Action {
	case rebase.RebaseActionDrop, rebase.RebaseActionPick:
		// Nothing to do – the drop action doesn't result in a cherry pick and the pick action
		// doesn't require any special options (i.e. no amend, no custom commit message).

	case rebase.RebaseActionReword:
		options.CommitMessage = planStep.CommitMsg

	case rebase.RebaseActionSquash:
		options.Amend = true
		commitMessage, err := squashCommitMessage(ctx, planStep.CommitHash)
		if err != nil {
			return nil, err
		}
		options.CommitMessage = commitMessage

	case rebase.RebaseActionFixup:
		options.Amend = true

	default:
		return nil, fmt.Errorf("rebase action '%s' is not supported", planStep.Action)
	}

	return &options, nil
}

// handleRebaseCherryPick runs a cherry-pick for the specified |commitHash|, using the specified
// cherry-pick |options| and checks the results for any errors or merge conflicts. If a data conflict
// is detected, then the ErrRebaseDataConflict error is returned. If a schema conflict is detected,
// then the ErrRebaseSchemaConflict error is returned.
func handleRebaseCherryPick(ctx *sql.Context, planStep *rebase.RebasePlanStep, options cherry_pick.CherryPickOptions) error {
	_, mergeResult, err := cherry_pick.CherryPick(ctx, planStep.CommitHash, options)

	// TODO: rebase doesn't support schema conflict resolution yet. Ideally, when a schema conflict
	//       is detected, the rebase would be paused and the user would resolve the conflict just
	//       like any other conflict, and then call dolt_rebase --continue to keep going.
	var schemaConflict merge.SchemaConflict
	if errors.As(err, &schemaConflict) {
		if abortErr := abortRebase(ctx); abortErr != nil {
			return ErrRebaseConflictWithAbortError.New(planStep.CommitHash, abortErr)
		}
		return ErrRebaseSchemaConflict.New(planStep.CommitHash)
	}

	doltSession := dsess.DSessFromSess(ctx.Session)
	if mergeResult != nil && mergeResult.HasMergeArtifacts() {
		if err := validateConflictsCanBeResolved(ctx, planStep); err != nil {
			return err
		}

		// If @@dolt_allow_commit_conflicts is enabled, then we need to make a SQL commit here, which
		// will save the conflicts and make them visible to other sessions. This is necessary for the CLI's
		// rebase command support. However, it's also valid to use @@autocommit and resolve the data
		// conflicts within the same session, so in that case, we do NOT make a SQL commit.
		allowCommitConflictsEnabled, err := isAllowCommitConflictsEnabled(ctx)
		if err != nil {
			return err
		}
		if allowCommitConflictsEnabled {
			if doltSession.GetTransaction() == nil {
				_, err := doltSession.StartTransaction(ctx, sql.ReadWrite)
				if err != nil {
					return err
				}
			}

			err = doltSession.CommitTransaction(ctx, doltSession.GetTransaction())
			if err != nil {
				return err
			}
		}

		// Otherwise, let the caller know about the conflict and how to resolve
		return ErrRebaseDataConflict.New(planStep.CommitHash, planStep.CommitMsg)
	}

	return err
}

// squashCommitMessage looks up the commit at HEAD and the commit identified by |nextCommitHash| and squashes their two
// commit messages together.
func squashCommitMessage(ctx *sql.Context, nextCommitHash string) (string, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	headCommit, err := doltSession.GetHeadCommit(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return "", err
	}
	headCommitMeta, err := headCommit.GetCommitMeta(ctx)
	if err != nil {
		return "", err
	}

	ddb, ok := doltSession.GetDoltDB(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return "", fmt.Errorf("unable to get doltdb!")
	}
	spec, err := doltdb.NewCommitSpec(nextCommitHash)
	if err != nil {
		return "", err
	}
	headRef, err := doltSession.CWBHeadRef(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return "", err
	}

	optCmt, err := ddb.Resolve(ctx, spec, headRef)
	if err != nil {
		return "", err
	}
	nextCommit, ok := optCmt.ToCommit()
	if !ok {
		return "", doltdb.ErrGhostCommitEncountered
	}

	nextCommitMeta, err := nextCommit.GetCommitMeta(ctx)
	if err != nil {
		return "", err
	}
	commitMessage := headCommitMeta.Description + "\n\n" + nextCommitMeta.Description

	return commitMessage, nil
}

// currentBranch returns the name of the currently checked out branch, or any error if one was encountered.
func currentBranch(ctx *sql.Context) (string, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	headRef, err := doltSession.CWBHeadRef(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return "", err
	}
	return headRef.GetPath(), nil
}

// validateConflictsCanBeResolved checks to see if conflicts can be recorded in the current session, by looking to see
// if @@autocommit is disabled, or if @@dolt_allow_commit_conflicts has been enabled. If conflicts cannot be recorded
// then the current rebase is aborted, and an error is returned describing how to change the session settings and
// restart the rebase.
func validateConflictsCanBeResolved(ctx *sql.Context, planStep *rebase.RebasePlanStep) error {
	autocommitEnabled, err := isAutocommitEnabled(ctx)
	if err != nil {
		return err
	}

	allowCommitConflictsEnabled, err := isAllowCommitConflictsEnabled(ctx)
	if err != nil {
		return err
	}

	// If @@autocommit is turned off, or if @@dolt_allow_commit_conflicts is enabled, then the user will
	// be able to resolve conflicts in the session.
	if !autocommitEnabled || allowCommitConflictsEnabled {
		return nil
	}

	// Otherwise, abort the rebase and return an error message, since conflicts can't be worked with.
	if abortErr := abortRebase(ctx); abortErr != nil {
		return ErrRebaseConflictWithAbortError.New(planStep.CommitHash, abortErr)
	}
	return ErrRebaseDataConflictsCantBeResolved.New()
}

// isAutocommitEnabled returns true if @@autocommit is enabled in the current session.
func isAutocommitEnabled(ctx *sql.Context) (bool, error) {
	return lookupBoolSysVar(ctx, "autocommit")
}

// isAllowCommitConflictsEnabled returns true if @@dolt_allow_commit_conflicts is enabled in the current session.
func isAllowCommitConflictsEnabled(ctx *sql.Context) (bool, error) {
	return lookupBoolSysVar(ctx, "dolt_allow_commit_conflicts")
}

// lookupBoolSysVar returns the value of the boolean system variable named |systemVarName| for the current session.
func lookupBoolSysVar(ctx *sql.Context, systemVarName string) (bool, error) {
	value, err := ctx.GetSessionVariable(ctx, systemVarName)
	if err != nil {
		return false, err
	}
	boolValue, _, err := types.Boolean.Convert(value)
	if err != nil {
		return false, err
	}

	return boolValue == int8(1) || boolValue == true, nil
}
