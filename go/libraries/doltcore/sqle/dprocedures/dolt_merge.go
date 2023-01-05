// Copyright 2022 Dolthub, Inc.
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
	goerrors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const DoltMergeWarningCode int = 1105 // Since this our own custom warning we'll use 1105, the code for an unknown error

const (
	noConflictsOrViolations  int = 0
	hasConflictsOrViolations int = 1
)

const (
	threeWayMerge    = 0
	fastForwardMerge = 1
)

var ErrUncommittedChanges = goerrors.NewKind("cannot merge with uncommitted changes")

// doltMerge is the stored procedure version of the functions `merge` and `dolt_merge`.
func doltMerge(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	hasConflicts, ff, err := doDoltMerge(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(ff), int64(hasConflicts)), nil
}

// doDoltMerge returns has_conflicts and fast_forward status
func doDoltMerge(ctx *sql.Context, args []string) (int, int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return noConflictsOrViolations, threeWayMerge, fmt.Errorf("Empty database name.")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return noConflictsOrViolations, threeWayMerge, err
	}

	sess := dsess.DSessFromSess(ctx.Session)

	apr, err := cli.CreateMergeArgParser().Parse(args)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, err
	}

	if apr.ContainsAll(cli.SquashParam, cli.NoFFParam) {
		return noConflictsOrViolations, threeWayMerge, fmt.Errorf("error: Flags '--%s' and '--%s' cannot be used together.\n", cli.SquashParam, cli.NoFFParam)
	}

	ws, err := sess.WorkingSet(ctx, dbName)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, err
	}
	roots, ok := sess.GetRoots(ctx, dbName)
	if !ok {
		return noConflictsOrViolations, threeWayMerge, sql.ErrDatabaseNotFound.New(dbName)
	}

	if apr.Contains(cli.AbortParam) {
		if !ws.MergeActive() {
			return noConflictsOrViolations, threeWayMerge, fmt.Errorf("fatal: There is no merge to abort")
		}

		ws, err = abortMerge(ctx, ws, roots)
		if err != nil {
			return noConflictsOrViolations, threeWayMerge, err
		}

		err := sess.SetWorkingSet(ctx, dbName, ws)
		if err != nil {
			return noConflictsOrViolations, threeWayMerge, err
		}

		err = sess.CommitWorkingSet(ctx, dbName, sess.GetTransaction())
		if err != nil {
			return noConflictsOrViolations, threeWayMerge, err
		}

		return noConflictsOrViolations, threeWayMerge, nil
	}

	branchName := apr.Arg(0)

	mergeSpec, err := createMergeSpec(ctx, sess, dbName, apr, branchName)
	if err != nil {
		return noConflictsOrViolations, threeWayMerge, err
	}

	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return noConflictsOrViolations, threeWayMerge, fmt.Errorf("Could not load database %s", dbName)
	}
	msg := fmt.Sprintf("Merge branch '%s' into %s", branchName, dbData.Rsr.CWBHeadRef().GetPath())
	if userMsg, mOk := apr.GetValue(cli.MessageArg); mOk {
		msg = userMsg
	}

	ws, conflicts, fastForward, err := performMerge(ctx, sess, roots, ws, dbName, mergeSpec, apr.Contains(cli.NoCommitFlag), msg)
	if err != nil || conflicts != 0 || fastForward != 0 {
		return conflicts, fastForward, err
	}

	return conflicts, fastForward, nil
}

// performMerge encapsulates server merge logic, switching between
// fast-forward, no fast-forward, merge commit, and merging into working set.
// Returns a new WorkingSet, whether there were merge conflicts, and whether a
// fast-forward was performed. This commits the working set if merge is successful and
// 'no-commit' flag is not defined.
// TODO FF merging commit with constraint violations requires `constraint verify`
func performMerge(ctx *sql.Context, sess *dsess.DoltSession, roots doltdb.Roots, ws *doltdb.WorkingSet, dbName string, spec *merge.MergeSpec, noCommit bool, msg string) (*doltdb.WorkingSet, int, int, error) {
	// todo: allow merges even when an existing merge is uncommitted
	if ws.MergeActive() {
		return ws, noConflictsOrViolations, threeWayMerge, doltdb.ErrMergeActive
	}

	err := checkForUncommittedChanges(ctx, roots.Working, roots.Head)
	if err != nil {
		return ws, noConflictsOrViolations, threeWayMerge, err
	}

	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return ws, noConflictsOrViolations, threeWayMerge, fmt.Errorf("failed to get dbData")
	}

	canFF, err := spec.HeadC.CanFastForwardTo(ctx, spec.MergeC)
	if err != nil {
		switch err {
		case doltdb.ErrIsAhead, doltdb.ErrUpToDate:
			ctx.Warn(DoltMergeWarningCode, err.Error())
		default:
			return ws, noConflictsOrViolations, threeWayMerge, err
		}
	}

	if canFF {
		if spec.Noff {
			ws, err = executeNoFFMerge(ctx, sess, spec, dbName, ws, dbData)
			if err == doltdb.ErrUnresolvedConflictsOrViolations {
				// if there are unresolved conflicts, write the resulting working set back to the session and return an
				// error message
				wsErr := sess.SetWorkingSet(ctx, dbName, ws)
				if wsErr != nil {
					return ws, hasConflictsOrViolations, threeWayMerge, wsErr
				}

				ctx.Warn(DoltMergeWarningCode, err.Error())

				return ws, hasConflictsOrViolations, threeWayMerge, nil
			}
			return ws, noConflictsOrViolations, fastForwardMerge, err
		}

		ws, err = executeFFMerge(ctx, dbName, spec.Squash, ws, dbData, spec.MergeC)
		return ws, noConflictsOrViolations, fastForwardMerge, err
	}

	dbState, ok, err := sess.LookupDbState(ctx, dbName)
	if err != nil {
		return ws, noConflictsOrViolations, threeWayMerge, err
	} else if !ok {
		return ws, noConflictsOrViolations, threeWayMerge, sql.ErrDatabaseNotFound.New(dbName)
	}

	ws, err = executeMerge(ctx, spec.Squash, spec.HeadC, spec.MergeC, spec.MergeCSpecStr, ws, dbState.EditOpts())
	if err == doltdb.ErrUnresolvedConflictsOrViolations {
		// if there are unresolved conflicts, write the resulting working set back to the session and return an
		// error message
		wsErr := sess.SetWorkingSet(ctx, dbName, ws)
		if wsErr != nil {
			return ws, hasConflictsOrViolations, threeWayMerge, wsErr
		}

		ctx.Warn(DoltMergeWarningCode, err.Error())

		return ws, hasConflictsOrViolations, threeWayMerge, nil
	} else if err != nil {
		return ws, noConflictsOrViolations, threeWayMerge, err
	}

	err = sess.SetWorkingSet(ctx, dbName, ws)
	if err != nil {
		return ws, noConflictsOrViolations, threeWayMerge, err
	}

	if !noCommit {
		author := fmt.Sprintf("%s <%s>", spec.Name, spec.Email)
		_, err = doDoltCommit(ctx, []string{"-m", msg, "--author", author})
		if err != nil {
			return ws, noConflictsOrViolations, threeWayMerge, fmt.Errorf("dolt_commit failed")
		}
	}

	return ws, noConflictsOrViolations, threeWayMerge, nil
}

func abortMerge(ctx *sql.Context, workingSet *doltdb.WorkingSet, roots doltdb.Roots) (*doltdb.WorkingSet, error) {
	tbls, err := doltdb.UnionTableNames(ctx, roots.Working, roots.Staged, roots.Head)
	if err != nil {
		return nil, err
	}

	roots, err = actions.MoveTablesFromHeadToWorking(ctx, roots, tbls)
	if err != nil {
		return nil, err
	}

	// TODO: this doesn't seem right, it sets the root that we already edited above
	workingSet = workingSet.AbortMerge()
	return workingSet, nil
}

func executeMerge(ctx *sql.Context, squash bool, head, cm *doltdb.Commit, cmSpec string, ws *doltdb.WorkingSet, opts editor.Options) (*doltdb.WorkingSet, error) {
	mergeRoot, mergeStats, err := merge.MergeCommits(ctx, head, cm, opts)

	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return nil, errors.New("Already up to date.")
		case merge.ErrFastForward:
			panic("fast forward merge")
		default:
			return nil, err
		}
	}

	return mergeRootToWorking(squash, ws, mergeRoot, cm, cmSpec, mergeStats)
}

func executeFFMerge(ctx *sql.Context, dbName string, squash bool, ws *doltdb.WorkingSet, dbData env.DbData, cm2 *doltdb.Commit) (*doltdb.WorkingSet, error) {
	rv, err := cm2.GetRootValue(ctx)
	if err != nil {
		return ws, err
	}

	// TODO: This is all incredibly suspect, needs to be replaced with library code that is functional instead of
	//  altering global state
	if !squash {
		err = dbData.Ddb.FastForward(ctx, dbData.Rsr.CWBHeadRef(), cm2)
		if err != nil {
			return ws, err
		}
	}

	ws = ws.WithWorkingRoot(rv).WithStagedRoot(rv)

	// We need to assign the working set to the session but ensure that its state is not labeled as dirty (ffs are clean
	// merges). Hence, we go ahead and commit the working set to the transaction.
	sess := dsess.DSessFromSess(ctx.Session)

	err = sess.SetWorkingSet(ctx, dbName, ws)
	if err != nil {
		return ws, err
	}

	// We only fully commit our transaction when we are not squashing.
	if !squash {
		err = sess.CommitWorkingSet(ctx, dbName, sess.GetTransaction())
		if err != nil {
			return ws, err
		}
	}

	return ws, nil
}

func executeNoFFMerge(
	ctx *sql.Context,
	dSess *dsess.DoltSession,
	spec *merge.MergeSpec,
	dbName string,
	ws *doltdb.WorkingSet,
	dbData env.DbData,
) (*doltdb.WorkingSet, error) {
	mergeRoot, err := spec.MergeC.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	ws, err = mergeRootToWorking(false, ws, mergeRoot, spec.MergeC, spec.MergeCSpecStr, map[string]*merge.MergeStats{})
	if err != nil {
		// This error is recoverable, so we return a working set value along with the error
		return ws, err
	}

	// Save our work so far in the session, as it will be referenced by the commit call below (badly in need of a
	// refactoring)
	err = dSess.SetWorkingSet(ctx, dbName, ws)
	if err != nil {
		return nil, err
	}

	// The roots need refreshing after the above
	roots, _ := dSess.GetRoots(ctx, dbName)

	pendingCommit, err := dSess.NewPendingCommit(ctx, dbName, roots, actions.CommitStagedProps{
		Message:    spec.Msg,
		Date:       spec.Date,
		AllowEmpty: spec.AllowEmpty,
		Force:      spec.Force,
		Name:       spec.Name,
		Email:      spec.Email,
	})
	if err != nil {
		return nil, err
	}

	if pendingCommit == nil {
		return nil, errors.New("nothing to commit")
	}

	_, err = dSess.DoltCommit(ctx, dbName, dSess.GetTransaction(), pendingCommit)
	if err != nil {
		return nil, err
	}

	return ws, nil
}

func createMergeSpec(ctx *sql.Context, sess *dsess.DoltSession, dbName string, apr *argparser.ArgParseResults, commitSpecStr string) (*merge.MergeSpec, error) {
	ddb, ok := sess.GetDoltDB(ctx, dbName)

	dbData, ok := sess.GetDbData(ctx, dbName)

	msg, ok := apr.GetValue(cli.MessageArg)
	if !ok {
		// TODO probably change, but we can't open editor so it'll have to be automated
		msg = "automatic SQL merge"
	}

	var err error
	var name, email string
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
		if err != nil {
			return nil, err
		}
	} else {
		name = sess.Username()
		email = sess.Email()
	}

	t := ctx.QueryTime()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		t, err = cli.ParseDate(commitTimeStr)
		if err != nil {
			return nil, err
		}
	}

	roots, ok := sess.GetRoots(ctx, dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	if apr.Contains(cli.NoCommitFlag) && apr.Contains(cli.CommitFlag) {
		return nil, errors.New("cannot define both 'commit' and 'no-commit' flags at the same time")
	}
	mergeSpec, err := merge.NewMergeSpec(ctx, dbData.Rsr, ddb, roots, name, email, msg, commitSpecStr, apr.Contains(cli.SquashParam), apr.Contains(cli.NoFFParam), apr.Contains(cli.ForceFlag), apr.Contains(cli.NoCommitFlag), apr.Contains(cli.NoEditFlag), t)
	if err != nil {
		return nil, err
	}
	return mergeSpec, nil
}

// TODO: this copied from commands/merge.go because the latter isn't reusable. Fix that.
func mergeRootToWorking(
	squash bool,
	ws *doltdb.WorkingSet,
	mergedRoot *doltdb.RootValue,
	cm2 *doltdb.Commit,
	cm2Spec string,
	mergeStats map[string]*merge.MergeStats,
) (*doltdb.WorkingSet, error) {

	workingRoot := mergedRoot
	if !squash {
		ws = ws.StartMerge(cm2, cm2Spec)
	}

	ws = ws.WithWorkingRoot(workingRoot).WithStagedRoot(workingRoot)
	if checkForConflicts(mergeStats) || checkForViolations(mergeStats) {
		// this error is recoverable in-session, so we return the new ws along with the error
		return ws, doltdb.ErrUnresolvedConflictsOrViolations
	}

	return ws, nil
}

func checkForUncommittedChanges(ctx *sql.Context, root *doltdb.RootValue, headRoot *doltdb.RootValue) error {
	rh, err := root.HashOf()

	if err != nil {
		return err
	}

	hrh, err := headRoot.HashOf()

	if err != nil {
		return err
	}

	if rh != hrh {
		return ErrUncommittedChanges.New()
	}
	return nil
}

func checkForConflicts(tblToStats map[string]*merge.MergeStats) bool {
	for _, stats := range tblToStats {
		if stats.Operation == merge.TableModified && stats.Conflicts > 0 {
			return true
		}
	}

	return false
}

func checkForViolations(tblToStats map[string]*merge.MergeStats) bool {
	for _, stats := range tblToStats {
		if stats.ConstraintViolations > 0 {
			return true
		}
	}
	return false
}
