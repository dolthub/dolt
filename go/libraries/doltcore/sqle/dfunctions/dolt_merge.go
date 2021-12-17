// Copyright 2021 Dolthub, Inc.
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

package dfunctions

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const DoltMergeFuncName = "dolt_merge"

type DoltMergeFunc struct {
	expression.NaryExpression
}

const DoltMergeWarningCode int = 1105 // Since this our own custom warning we'll use 1105, the code for an unknown error

const (
	hasConflicts int = 0
	noConflicts  int = 1
)

func (d DoltMergeFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return noConflicts, fmt.Errorf("Empty database name.")
	}

	sess := dsess.DSessFromSess(ctx.Session)

	ap := cli.CreateMergeArgParser()
	args, err := getDoltArgs(ctx, row, d.Children())

	if err != nil {
		return noConflicts, err
	}

	apr, err := ap.Parse(args)
	if err != nil {
		return noConflicts, err
	}

	if apr.ContainsAll(cli.SquashParam, cli.NoFFParam) {
		return noConflicts, fmt.Errorf("error: Flags '--%s' and '--%s' cannot be used together.\n", cli.SquashParam, cli.NoFFParam)
	}

	ws, err := sess.WorkingSet(ctx, dbName)
	if err != nil {
		return noConflicts, err
	}
	roots, ok := sess.GetRoots(ctx, dbName)
	if !ok {
		return noConflicts, sql.ErrDatabaseNotFound.New(dbName)
	}

	if apr.Contains(cli.AbortParam) {
		if !ws.MergeActive() {
			return noConflicts, fmt.Errorf("fatal: There is no merge to abort")
		}

		ws, err = abortMerge(ctx, ws, roots)
		if err != nil {
			return noConflicts, err
		}

		err := sess.SetWorkingSet(ctx, dbName, ws, nil)
		if err != nil {
			return noConflicts, err
		}

		return noConflicts, nil
	}

	branchName := apr.Arg(0)

	mergeSpec, err := createMergeSpec(ctx, sess, dbName, apr, branchName)
	if err != nil {
		return noConflicts, err
	}
	ws, conflicts, err := mergeIntoWorkingSet(ctx, sess, roots, ws, dbName, mergeSpec)
	if err != nil {
		return conflicts, err
	}

	err = sess.SetWorkingSet(ctx, dbName, ws, nil)
	if err != nil {
		return conflicts, err
	}

	return conflicts, nil
}

// mergeIntoWorkingSet encapsulates server merge logic, switching between fast-forward, no fast-forward, merge commit,
// and merging into working set. Returns a new WorkingSet and whether there were merge conflicts. This currently
// persists merge commits in the database, but expects the caller to update the working set.
// TODO FF merging commit with constraint violations requires `constraint verify`
func mergeIntoWorkingSet(ctx *sql.Context, sess *dsess.DoltSession, roots doltdb.Roots, ws *doltdb.WorkingSet, dbName string, spec *merge.MergeSpec) (*doltdb.WorkingSet, int, error) {
	if conflicts, err := roots.Working.HasConflicts(ctx); err != nil {
		return ws, noConflicts, err
	} else if conflicts {
		return ws, hasConflicts, doltdb.ErrUnresolvedConflicts
	}

	if hasConstraintViolations, err := roots.Working.HasConstraintViolations(ctx); err != nil {
		return ws, hasConflicts, err
	} else if hasConstraintViolations {
		return ws, hasConflicts, doltdb.ErrUnresolvedConstraintViolations
	}

	if ws.MergeActive() {
		return ws, noConflicts, doltdb.ErrMergeActive
	}

	err := checkForUncommittedChanges(roots.Working, roots.Head)
	if err != nil {
		return ws, noConflicts, err
	}

	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return ws, noConflicts, fmt.Errorf("failed to get dbData")
	}

	canFF, err := spec.HeadC.CanFastForwardTo(ctx, spec.MergeC)
	if err != nil {
		switch err {
		case doltdb.ErrIsAhead, doltdb.ErrUpToDate:
			ctx.Warn(DoltMergeWarningCode, err.Error())
		default:
			return ws, noConflicts, err
		}
	}

	if canFF {
		if spec.Noff {
			ws, err = executeNoFFMerge(ctx, sess, spec, dbName, ws, dbData)
			if err == doltdb.ErrUnresolvedConflicts {
				// if there are unresolved conflicts, write the resulting working set back to the session and return an
				// error message
				wsErr := sess.SetWorkingSet(ctx, dbName, ws, nil)
				if wsErr != nil {
					return ws, hasConflicts, wsErr
				}

				ctx.Warn(DoltMergeWarningCode, err.Error())

				// Return 0 indicating there are conflicts
				return ws, hasConflicts, nil
			}
			return ws, noConflicts, err
		}

		ws, err = executeFFMerge(ctx, spec.Squash, ws, dbData, spec.MergeC)
		return ws, noConflicts, err
	}

	dbState, ok, err := sess.LookupDbState(ctx, dbName)
	if err != nil {
		return ws, noConflicts, err
	} else if !ok {
		return ws, noConflicts, sql.ErrDatabaseNotFound.New(dbName)
	}

	ws, err = executeMerge(ctx, spec.Squash, spec.HeadC, spec.MergeC, ws, dbState.EditOpts())
	if err == doltdb.ErrUnresolvedConflicts || err == doltdb.ErrUnresolvedConstraintViolations {
		// if there are unresolved conflicts, write the resulting working set back to the session and return an
		// error message
		wsErr := sess.SetWorkingSet(ctx, dbName, ws, nil)
		if wsErr != nil {
			return ws, hasConflicts, wsErr
		}

		ctx.Warn(DoltMergeWarningCode, err.Error())

		return ws, hasConflicts, nil
	} else if err != nil {
		return ws, noConflicts, err
	}

	return ws, noConflicts, nil
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

func executeMerge(ctx *sql.Context, squash bool, head, cm *doltdb.Commit, ws *doltdb.WorkingSet, opts editor.Options) (*doltdb.WorkingSet, error) {
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

	return mergeRootToWorking(squash, ws, mergeRoot, cm, mergeStats)
}

func executeFFMerge(ctx *sql.Context, squash bool, ws *doltdb.WorkingSet, dbData env.DbData, cm2 *doltdb.Commit) (*doltdb.WorkingSet, error) {
	rv, err := cm2.GetRootValue()
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

	return ws.WithWorkingRoot(rv).WithStagedRoot(rv), nil
}

func executeNoFFMerge(
	ctx *sql.Context,
	dSess *dsess.DoltSession,
	spec *merge.MergeSpec,
	dbName string,
	ws *doltdb.WorkingSet,
	dbData env.DbData,
	//headCommit, mergeCommit *doltdb.Commit,
) (*doltdb.WorkingSet, error) {
	mergeRoot, err := spec.MergeC.GetRootValue()
	if err != nil {
		return nil, err
	}

	ws, err = mergeRootToWorking(false, ws, mergeRoot, spec.MergeC, map[string]*merge.MergeStats{})
	if err != nil {
		// This error is recoverable, so we return a working set value along with the error
		return ws, err
	}

	// Save our work so far in the session, as it will be referenced by the commit call below (badly in need of a
	// refactoring)
	err = dSess.SetWorkingSet(ctx, dbName, ws, nil)
	if err != nil {
		return nil, err
	}

	// The roots need refreshing after the above
	roots, _ := dSess.GetRoots(ctx, dbName)

	var mergeParentCommits []*doltdb.Commit
	if ws.MergeActive() {
		mergeParentCommits = []*doltdb.Commit{ws.MergeState().Commit()}
	}

	// TODO: this does several session state updates, and it really needs to just do one
	//  We also need to commit any pending transaction before we do this.
	_, err = actions.CommitStaged(ctx, roots, ws.MergeActive(), mergeParentCommits, dbData, actions.CommitStagedProps{
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

	return ws, dSess.SetWorkingSet(ctx, dbName, ws.ClearMerge(), nil)
}

func createMergeSpec(ctx *sql.Context, sess *dsess.DoltSession, dbName string, apr *argparser.ArgParseResults, commitSpecStr string) (*merge.MergeSpec, error) {
	ddb, ok := sess.GetDoltDB(ctx, dbName)

	dbData, ok := sess.GetDbData(ctx, dbName)

	msg, ok := apr.GetValue(cli.CommitMessageArg)
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

	mergeSpec, _, err := merge.NewMergeSpec(ctx, dbData.Rsr, ddb, roots, name, email, msg, commitSpecStr, apr.Contains(cli.SquashParam), apr.Contains(cli.NoFFParam), apr.Contains(cli.ForceFlag), t)
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
	mergeStats map[string]*merge.MergeStats,
) (*doltdb.WorkingSet, error) {

	workingRoot := mergedRoot
	if !squash {
		ws = ws.StartMerge(cm2)
	}

	ws = ws.WithWorkingRoot(workingRoot).WithStagedRoot(workingRoot)
	if checkForConflicts(mergeStats) {
		// this error is recoverable in-session, so we return the new ws along with the error
		return ws, doltdb.ErrUnresolvedConflicts
	}

	return ws, nil
}

func checkForConflicts(tblToStats map[string]*merge.MergeStats) bool {
	for _, stats := range tblToStats {
		if stats.Operation == merge.TableModified && stats.Conflicts > 0 {
			return true
		}
	}

	return false
}

func (d DoltMergeFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_MERGE(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltMergeFunc) Type() sql.Type {
	return sql.Boolean
}

func (d DoltMergeFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltMergeFunc(children...)
}

func NewDoltMergeFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltMergeFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}
