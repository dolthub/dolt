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
var ErrRebaseUncommittedChanges = fmt.Errorf("cannot start a rebase with uncommitted changes")

// ErrRebaseConflict is used when a merge conflict is detected while rebasing a commit.
var ErrRebaseConflict = goerrors.NewKind(
	"merge conflict detected while rebasing commit %s. " +
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
	// happens with a detatched HEAD, but Dolt doesn't support that, we use a temporary branch.
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
		return ErrRebaseUncommittedChanges
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

func continueRebase(ctx *sql.Context) (string, error) {
	// TODO: Eventually, when we allow interactive-rebases to be stopped and started (e.g. with the break action,
	//       or for conflict resolution), we'll need to track what step we're at in the rebase plan.

	// Validate that we are in an interactive rebase
	doltSession := dsess.DSessFromSess(ctx.Session)
	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return "", err
	}
	if !workingSet.RebaseActive() {
		return "", fmt.Errorf("no rebase in progress")
	}

	db, err := doltSession.Provider().Database(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return "", err
	}

	rdb, ok := db.(rebase.RebasePlanDatabase)
	if !ok {
		return "", fmt.Errorf("expected a dsess.RebasePlanDatabase implementation, but received a %T", db)
	}
	rebasePlan, err := rdb.LoadRebasePlan(ctx)
	if err != nil {
		return "", err
	}

	err = rebase.ValidateRebasePlan(ctx, rebasePlan)
	if err != nil {
		return "", err
	}

	for _, step := range rebasePlan.Steps {
		err = processRebasePlanStep(ctx, &step,
			workingSet.RebaseState().CommitBecomesEmptyHandling(),
			workingSet.RebaseState().EmptyCommitHandling())
		if err != nil {
			return "", err
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

	// delete the temporary working branch
	dbData, ok = doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return "", fmt.Errorf("unable to lookup dbdata")
	}
	return rebaseBranch, actions.DeleteBranch(ctx, dbData, rebaseWorkingBranch, actions.DeleteOptions{
		Force: true,
	}, doltSession.Provider(), nil)
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

	// Override the default empty commit handling options for cherry-pick, since
	// rebase has slightly different defaults
	options := cherry_pick.NewCherryPickOptions()
	options.CommitBecomesEmptyHandling = commitBecomesEmptyHandling
	options.EmptyCommitHandling = emptyCommitHandling

	switch planStep.Action {
	case rebase.RebaseActionDrop:
		return nil

	case rebase.RebaseActionPick, rebase.RebaseActionReword:
		if planStep.Action == rebase.RebaseActionReword {
			options.CommitMessage = planStep.CommitMsg
		}
		return handleRebaseCherryPick(ctx, planStep.CommitHash, options)

	case rebase.RebaseActionSquash, rebase.RebaseActionFixup:
		options.Amend = true
		if planStep.Action == rebase.RebaseActionSquash {
			commitMessage, err := squashCommitMessage(ctx, planStep.CommitHash)
			if err != nil {
				return err
			}
			options.CommitMessage = commitMessage
		}
		return handleRebaseCherryPick(ctx, planStep.CommitHash, options)

	default:
		return fmt.Errorf("rebase action '%s' is not supported", planStep.Action)
	}
}

// handleRebaseCherryPick runs a cherry-pick for the specified |commitHash|, using the specified
// cherry-pick |options| and checks the results for any errors or merge conflicts. The initial
// version of rebase doesn't support conflict resolution, so if any conflicts are detected, the
// rebase is aborted and an error is returned.
func handleRebaseCherryPick(ctx *sql.Context, commitHash string, options cherry_pick.CherryPickOptions) error {
	_, mergeResult, err := cherry_pick.CherryPick(ctx, commitHash, options)

	var schemaConflict merge.SchemaConflict
	isSchemaConflict := errors.As(err, &schemaConflict)

	if (mergeResult != nil && mergeResult.HasMergeArtifacts()) || isSchemaConflict {
		// TODO: rebase doesn't currently support conflict resolution, but ideally, when a conflict
		//       is detected, the rebase would be paused and the user would resolve the conflict just
		//       like any other conflict, and then call dolt_rebase --continue to keep going.
		abortErr := abortRebase(ctx)
		if abortErr != nil {
			return ErrRebaseConflictWithAbortError.New(commitHash, abortErr)
		}
		return ErrRebaseConflict.New(commitHash)
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
