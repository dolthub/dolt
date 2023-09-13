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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
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
	"github.com/dolthub/dolt/go/store/hash"
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

var doltMergeSchema = []*sql.Column{
	{
		Name:     "hash",
		Type:     gmstypes.LongText,
		Nullable: true,
	},
	{
		Name:     "fast_forward",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
	{
		Name:     "conflicts",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
}

// doltMerge is the stored procedure version for the CLI command `dolt merge`.
func doltMerge(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	commitHash, hasConflicts, ff, err := doDoltMerge(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(commitHash, int64(ff), int64(hasConflicts)), nil
}

// doDoltMerge returns has_conflicts and fast_forward status
func doDoltMerge(ctx *sql.Context, args []string) (string, int, int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return "", noConflictsOrViolations, threeWayMerge, fmt.Errorf("Empty database name.")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return "", noConflictsOrViolations, threeWayMerge, err
	}

	sess := dsess.DSessFromSess(ctx.Session)

	apr, err := cli.CreateMergeArgParser().Parse(args)
	if err != nil {
		return "", noConflictsOrViolations, threeWayMerge, err
	}

	if len(args) == 0 {
		return "", noConflictsOrViolations, threeWayMerge, errors.New("error: Please specify a branch to merge")
	}

	if apr.ContainsAll(cli.SquashParam, cli.NoFFParam) {
		return "", noConflictsOrViolations, threeWayMerge, fmt.Errorf("error: Flags '--%s' and '--%s' cannot be used together.\n", cli.SquashParam, cli.NoFFParam)
	}

	ws, err := sess.WorkingSet(ctx, dbName)
	if err != nil {
		return "", noConflictsOrViolations, threeWayMerge, err
	}
	roots, ok := sess.GetRoots(ctx, dbName)
	if !ok {
		return "", noConflictsOrViolations, threeWayMerge, sql.ErrDatabaseNotFound.New(dbName)
	}

	if apr.Contains(cli.AbortParam) {
		if !ws.MergeActive() {
			return "", noConflictsOrViolations, threeWayMerge, fmt.Errorf("fatal: There is no merge to abort")
		}

		ws, err = abortMerge(ctx, ws, roots)
		if err != nil {
			return "", noConflictsOrViolations, threeWayMerge, err
		}

		err := sess.SetWorkingSet(ctx, dbName, ws)
		if err != nil {
			return "", noConflictsOrViolations, threeWayMerge, err
		}

		err = sess.CommitWorkingSet(ctx, dbName, sess.GetTransaction())
		if err != nil {
			return "", noConflictsOrViolations, threeWayMerge, err
		}

		return "", noConflictsOrViolations, threeWayMerge, nil
	}

	branchName := apr.Arg(0)

	mergeSpec, err := createMergeSpec(ctx, sess, dbName, apr, branchName)
	if err != nil {
		return "", noConflictsOrViolations, threeWayMerge, err
	}

	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return "", noConflictsOrViolations, threeWayMerge, fmt.Errorf("Could not load database %s", dbName)
	}

	headRef, err := dbData.Rsr.CWBHeadRef()
	if err != nil {
		return "", noConflictsOrViolations, threeWayMerge, err
	}
	msg := fmt.Sprintf("Merge branch '%s' into %s", branchName, headRef.GetPath())
	if userMsg, mOk := apr.GetValue(cli.MessageArg); mOk {
		msg = userMsg
	}

	ws, commit, conflicts, fastForward, err := performMerge(ctx, sess, ws, dbName, mergeSpec, apr.Contains(cli.NoCommitFlag), msg)
	if err != nil || conflicts != 0 || fastForward != 0 {
		return commit, conflicts, fastForward, err
	}

	return commit, conflicts, fastForward, nil
}

// performMerge encapsulates server merge logic, switching between
// fast-forward, no fast-forward, merge commit, and merging into working set.
// Returns a new WorkingSet, whether there were merge conflicts, and whether a
// fast-forward was performed. This commits the working set if merge is successful and
// 'no-commit' flag is not defined.
// TODO FF merging commit with constraint violations requires `constraint verify`
func performMerge(
	ctx *sql.Context,
	sess *dsess.DoltSession,
	ws *doltdb.WorkingSet,
	dbName string,
	spec *merge.MergeSpec,
	noCommit bool,
	msg string,
) (*doltdb.WorkingSet, string, int, int, error) {
	// todo: allow merges even when an existing merge is uncommitted
	if ws.MergeActive() {
		return ws, "", noConflictsOrViolations, threeWayMerge, doltdb.ErrMergeActive
	}

	if len(spec.StompedTblNames) != 0 {
		return ws, "", noConflictsOrViolations, threeWayMerge, fmt.Errorf("error: local changes would be stomped by merge:\n\t%s\n Please commit your changes before you merge.", strings.Join(spec.StompedTblNames, "\n\t"))
	}

	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return ws, "", noConflictsOrViolations, threeWayMerge, fmt.Errorf("failed to get dbData")
	}

	canFF, err := spec.HeadC.CanFastForwardTo(ctx, spec.MergeC)
	if err != nil {
		switch err {
		case doltdb.ErrIsAhead, doltdb.ErrUpToDate:
			ctx.Warn(DoltMergeWarningCode, err.Error())
		default:
			return ws, "", noConflictsOrViolations, threeWayMerge, err
		}
	}

	if canFF {
		if spec.NoFF {
			var commit *doltdb.Commit
			ws, commit, err = executeNoFFMerge(ctx, sess, spec, msg, dbName, ws, noCommit)
			if err == doltdb.ErrUnresolvedConflictsOrViolations {
				// if there are unresolved conflicts, write the resulting working set back to the session and return an
				// error message
				wsErr := sess.SetWorkingSet(ctx, dbName, ws)
				if wsErr != nil {
					return ws, "", hasConflictsOrViolations, threeWayMerge, wsErr
				}

				ctx.Warn(DoltMergeWarningCode, err.Error())

				return ws, "", hasConflictsOrViolations, threeWayMerge, nil
			}
			if commit != nil {
				if h, cerr := commit.HashOf(); cerr == nil {
					return ws, h.String(), noConflictsOrViolations, threeWayMerge, err
				}
			}
			return ws, "", noConflictsOrViolations, threeWayMerge, err
		}

		ws, err = executeFFMerge(ctx, dbName, spec.Squash, ws, dbData, spec.MergeC, spec)
		if h, cerr := spec.MergeC.HashOf(); cerr == nil {
			return ws, h.String(), noConflictsOrViolations, fastForwardMerge, err
		}
		return ws, "", noConflictsOrViolations, fastForwardMerge, err
	}

	dbState, ok, err := sess.LookupDbState(ctx, dbName)
	if err != nil {
		return ws, "", noConflictsOrViolations, threeWayMerge, err
	} else if !ok {
		return ws, "", noConflictsOrViolations, threeWayMerge, sql.ErrDatabaseNotFound.New(dbName)
	}

	ws, err = executeMerge(ctx, sess, dbName, spec.Squash, spec.HeadC, spec.MergeC, spec.MergeCSpecStr, ws, dbState.EditOpts(), spec.WorkingDiffs)
	if err == doltdb.ErrUnresolvedConflictsOrViolations {
		// if there are unresolved conflicts, write the resulting working set back to the session and return an
		// error message
		wsErr := sess.SetWorkingSet(ctx, dbName, ws)
		if wsErr != nil {
			return ws, "", hasConflictsOrViolations, threeWayMerge, wsErr
		}

		ctx.Warn(DoltMergeWarningCode, err.Error())

		return ws, "", hasConflictsOrViolations, threeWayMerge, nil
	} else if err != nil {
		return ws, "", noConflictsOrViolations, threeWayMerge, err
	}

	err = sess.SetWorkingSet(ctx, dbName, ws)
	if err != nil {
		return ws, "", noConflictsOrViolations, threeWayMerge, err
	}

	var commit string
	if !noCommit {
		author := fmt.Sprintf("%s <%s>", spec.Name, spec.Email)
		commit, _, err = doDoltCommit(ctx, []string{"-m", msg, "--author", author})
		if err != nil {
			return ws, commit, noConflictsOrViolations, threeWayMerge, fmt.Errorf("dolt_commit failed")
		}
	}

	return ws, commit, noConflictsOrViolations, threeWayMerge, nil
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

	preMergeWorkingRoot := workingSet.MergeState().PreMergeWorkingRoot()
	preMergeWorkingTables, err := preMergeWorkingRoot.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	nonIgnoredTables, err := doltdb.ExcludeIgnoredTables(ctx, roots, preMergeWorkingTables)
	if err != nil {
		return nil, err
	}
	someTablesAreIgnored := len(nonIgnoredTables) != len(preMergeWorkingTables)

	if someTablesAreIgnored {
		newWorking, err := actions.MoveTablesBetweenRoots(ctx, nonIgnoredTables, preMergeWorkingRoot, roots.Working)
		if err != nil {
			return nil, err
		}
		workingSet = workingSet.WithWorkingRoot(newWorking)
	} else {
		workingSet = workingSet.WithWorkingRoot(preMergeWorkingRoot)
	}
	workingSet = workingSet.WithStagedRoot(workingSet.WorkingRoot())
	workingSet = workingSet.ClearMerge()

	return workingSet, nil
}

func executeMerge(
	ctx *sql.Context,
	sess *dsess.DoltSession,
	dbName string,
	squash bool,
	head, cm *doltdb.Commit,
	cmSpec string,
	ws *doltdb.WorkingSet,
	opts editor.Options,
	workingDiffs map[string]hash.Hash,
) (*doltdb.WorkingSet, error) {
	result, err := merge.MergeCommits(ctx, head, cm, opts)
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
	return mergeRootToWorking(ctx, sess, dbName, squash, ws, result, workingDiffs, cm, cmSpec)
}

func executeFFMerge(ctx *sql.Context, dbName string, squash bool, ws *doltdb.WorkingSet, dbData env.DbData, cm2 *doltdb.Commit, spec *merge.MergeSpec) (*doltdb.WorkingSet, error) {
	stagedRoot, err := cm2.GetRootValue(ctx)
	if err != nil {
		return ws, err
	}
	workingRoot := stagedRoot
	if len(spec.WorkingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, stagedRoot, spec.WorkingDiffs)
		if err != nil {
			return ws, err
		}
	}

	// TODO: This is all incredibly suspect, needs to be replaced with library code that is functional instead of
	//  altering global state
	if !squash {
		headRef, err := dbData.Rsr.CWBHeadRef()
		if err != nil {
			return nil, err
		}
		err = dbData.Ddb.FastForward(ctx, headRef, cm2)
		if err != nil {
			return ws, err
		}
	}

	ws = ws.WithWorkingRoot(workingRoot).WithStagedRoot(stagedRoot)

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
	msg string,
	dbName string,
	ws *doltdb.WorkingSet,
	noCommit bool,
) (*doltdb.WorkingSet, *doltdb.Commit, error) {
	mergeRoot, err := spec.MergeC.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}
	result := &merge.Result{Root: mergeRoot, Stats: make(map[string]*merge.MergeStats)}

	ws, err = mergeRootToWorking(ctx, dSess, dbName, false, ws, result, spec.WorkingDiffs, spec.MergeC, spec.MergeCSpecStr)
	if err != nil {
		// This error is recoverable, so we return a working set value along with the error
		return ws, nil, err
	}

	// Save our work so far in the session, as it will be referenced by the commit call below (badly in need of a
	// refactoring)
	err = dSess.SetWorkingSet(ctx, dbName, ws)
	if err != nil {
		return nil, nil, err
	}

	// The roots need refreshing after the above
	roots, _ := dSess.GetRoots(ctx, dbName)

	if noCommit {
		// stage all changes
		roots, err = actions.StageAllTables(ctx, roots, true)
		if err != nil {
			return nil, nil, err
		}

		err = dSess.SetRoots(ctx, dbName, roots)
		if err != nil {
			return nil, nil, err
		}

		return ws.WithStagedRoot(roots.Staged), nil, nil
	}

	pendingCommit, err := dSess.NewPendingCommit(ctx, dbName, roots, actions.CommitStagedProps{
		Message: msg,
		Date:    spec.Date,
		Force:   spec.Force,
		Name:    spec.Name,
		Email:   spec.Email,
	})
	if err != nil {
		return nil, nil, err
	}

	if pendingCommit == nil {
		return nil, nil, errors.New("nothing to commit")
	}

	commit, err := dSess.DoltCommit(ctx, dbName, dSess.GetTransaction(), pendingCommit)
	if err != nil {
		return nil, nil, err
	}

	return ws, commit, nil
}

func createMergeSpec(ctx *sql.Context, sess *dsess.DoltSession, dbName string, apr *argparser.ArgParseResults, commitSpecStr string) (*merge.MergeSpec, error) {
	ddb, ok := sess.GetDoltDB(ctx, dbName)

	dbData, ok := sess.GetDbData(ctx, dbName)

	name, email, err := getNameAndEmail(ctx, apr)
	if err != nil {
		return nil, err
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
	return merge.NewMergeSpec(
		ctx,
		dbData.Rsr,
		ddb,
		roots,
		name,
		email,
		commitSpecStr,
		t,
		merge.WithSquash(apr.Contains(cli.SquashParam)),
		merge.WithNoFF(apr.Contains(cli.NoFFParam)),
		merge.WithForce(apr.Contains(cli.ForceFlag)),
		merge.WithNoCommit(apr.Contains(cli.NoCommitFlag)),
		merge.WithNoEdit(apr.Contains(cli.NoEditFlag)),
	)
}

func getNameAndEmail(ctx *sql.Context, apr *argparser.ArgParseResults) (string, string, error) {
	var err error
	var name, email string
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
		if err != nil {
			return "", "", err
		}
	} else {
		name = ctx.Client().User
		email = fmt.Sprintf("%s@%s", ctx.Client().User, ctx.Client().Address)
	}
	return name, email, nil
}

func mergeRootToWorking(
	ctx *sql.Context,
	dSess *dsess.DoltSession,
	dbName string,
	squash bool,
	ws *doltdb.WorkingSet,
	merged *merge.Result,
	workingDiffs map[string]hash.Hash,
	cm2 *doltdb.Commit,
	cm2Spec string,
) (*doltdb.WorkingSet, error) {
	var err error
	staged, working := merged.Root, merged.Root
	if len(workingDiffs) > 0 {
		working, err = applyChanges(ctx, working, workingDiffs)
		if err != nil {
			return ws, err
		}
	}

	if !squash || merged.HasSchemaConflicts() {
		ws = ws.StartMerge(cm2, cm2Spec)
		tt := merge.SchemaConflictTableNames(merged.SchemaConflicts)
		ws = ws.WithUnmergableTables(tt)
	}

	ws = ws.WithWorkingRoot(working)
	if !merged.HasMergeArtifacts() {
		ws = ws.WithStagedRoot(staged)
	}

	err = dSess.SetWorkingSet(ctx, dbName, ws)
	if err != nil {
		return nil, err
	}

	if merged.HasMergeArtifacts() {
		// this error is recoverable in-session, so we return the new ws along with the error
		return ws, doltdb.ErrUnresolvedConflictsOrViolations
	}

	return ws, nil
}

func applyChanges(ctx *sql.Context, root *doltdb.RootValue, workingDiffs map[string]hash.Hash) (*doltdb.RootValue, error) {
	var err error
	for tblName, h := range workingDiffs {
		root, err = root.SetTableHash(ctx, tblName, h)

		if err != nil {
			return nil, fmt.Errorf("failed to update table; %w", err)
		}
	}

	return root, nil
}
