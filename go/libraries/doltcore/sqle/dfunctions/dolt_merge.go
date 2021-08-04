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
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const DoltMergeFuncName = "dolt_merge"

type DoltMergeFunc struct {
	expression.NaryExpression
}

const DoltConflictWarningCode int = 1105 // Since this our own custom warning we'll use 1105, the code for an unknown error

const hasConflicts = 0
const noConflicts = 1

func (d DoltMergeFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return noConflicts, fmt.Errorf("Empty database name.")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)

	if !ok {
		return noConflicts, fmt.Errorf("Could not load database %s", dbName)
	}

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

	// logrus.Errorf("heads are working: %s\nhead: %s", roots.Working.DebugString(ctx, true), roots.Head.DebugString(ctx, true))

	if !ok {
		return noConflicts, fmt.Errorf("Could not load database %s", dbName)
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

	ddb, ok := sess.GetDoltDB(ctx, dbName)
	if !ok {
		return noConflicts, sql.ErrDatabaseNotFound.New(dbName)
	}

	if hasConflicts, err := roots.Working.HasConflicts(ctx); err != nil {
		return noConflicts, err
	} else if hasConflicts {
		return noConflicts, doltdb.ErrUnresolvedConflicts
	}

	if hasConstraintViolations, err := roots.Working.HasConstraintViolations(ctx); err != nil {
		return noConflicts, err
	} else if hasConstraintViolations {
		return noConflicts, doltdb.ErrUnresolvedConstraintViolations
	}

	if ws.MergeActive() {
		return noConflicts, doltdb.ErrMergeActive
	}

	err = checkForUncommittedChanges(roots.Working, roots.Head)
	if err != nil {
		return noConflicts, err
	}

	branchName := apr.Arg(0)
	mergeCommit, _, err := getBranchCommit(ctx, branchName, ddb)
	if err != nil {
		return noConflicts, err
	}

	headCommit, err := sess.GetHeadCommit(ctx, dbName)
	if err != nil {
		return noConflicts, err
	}

	canFF, err := headCommit.CanFastForwardTo(ctx, mergeCommit)
	if err != nil {
		return noConflicts, err
	}

	if canFF {
		headRoot, err := headCommit.GetRootValue()
		if err != nil {
			return noConflicts, err
		}
		mergeRoot, err := mergeCommit.GetRootValue()
		if err != nil {
			return noConflicts, err
		}
		if cvPossible, err := merge.MayHaveConstraintViolations(ctx, headRoot, mergeRoot); err != nil {
			return noConflicts, err
		} else if !cvPossible {
			if apr.Contains(cli.NoFFParam) {
				ws, err = executeNoFFMerge(ctx, sess, apr, dbName, ws, dbData, headCommit, mergeCommit)
				if err == doltdb.ErrUnresolvedConflicts {
					// if there are unresolved conflicts, write the resulting working set back to the session and return an
					// error message
					wsErr := sess.SetWorkingSet(ctx, dbName, ws, nil)
					if wsErr != nil {
						return hasConflicts, wsErr
					}

					ctx.Warn(DoltConflictWarningCode, err.Error())

					// Return 0 indicating there are conflicts
					return hasConflicts, nil
				}
			} else {
				err = executeFFMerge(ctx, sess, apr.Contains(cli.SquashParam), dbName, ws, dbData, mergeCommit)
			}

			if err != nil {
				return noConflicts, err
			}
			return noConflicts, err
		}
	}

	ws, err = executeMerge(ctx, apr.Contains(cli.SquashParam), headCommit, mergeCommit, ws)
	if err == doltdb.ErrUnresolvedConflicts {
		// if there are unresolved conflicts, write the resulting working set back to the session and return an
		// error message
		wsErr := sess.SetWorkingSet(ctx, dbName, ws, nil)
		if wsErr != nil {
			return hasConflicts, wsErr
		}

		ctx.Warn(DoltConflictWarningCode, err.Error())

		return hasConflicts, nil
	} else if err != nil {
		return noConflicts, err
	}

	err = sess.SetWorkingSet(ctx, dbName, ws, nil)
	if err != nil {
		return noConflicts, err
	}

	return noConflicts, nil
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

func executeMerge(ctx *sql.Context, squash bool, head, cm *doltdb.Commit, ws *doltdb.WorkingSet) (*doltdb.WorkingSet, error) {
	mergeRoot, mergeStats, err := merge.MergeCommits(ctx, head, cm)

	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return nil, errors.New("Already up to date.")
		case merge.ErrFastForward:
			panic("fast forward merge")
		default:
			return nil, errors.New("Bad merge")
		}
	}

	return mergeRootToWorking(squash, ws, mergeRoot, cm, mergeStats)
}

func executeFFMerge(ctx *sql.Context, sess *dsess.Session, squash bool, dbName string, ws *doltdb.WorkingSet, dbData env.DbData, cm2 *doltdb.Commit) error {
	rv, err := cm2.GetRootValue()
	if err != nil {
		return err
	}

	// TODO: This is all incredibly suspect, needs to be replaced with library code that is functional instead of
	//  altering global state
	if !squash {
		err = dbData.Ddb.FastForward(ctx, dbData.Rsr.CWBHeadRef(), cm2)
		if err != nil {
			return err
		}
	}

	ws = ws.WithWorkingRoot(rv).WithStagedRoot(rv)

	return sess.SetWorkingSet(ctx, dbName, ws, nil)
}

func executeNoFFMerge(
	ctx *sql.Context,
	dSess *dsess.Session,
	apr *argparser.ArgParseResults,
	dbName string,
	ws *doltdb.WorkingSet,
	dbData env.DbData,
	headCommit, mergeCommit *doltdb.Commit,
) (*doltdb.WorkingSet, error) {
	mergeRoot, err := mergeCommit.GetRootValue()
	if err != nil {
		return nil, err
	}

	ws, err = mergeRootToWorking(false, ws, mergeRoot, mergeCommit, map[string]*merge.MergeStats{})
	if err != nil {
		// This error is recoverable, so we return a working set value along with the error
		return ws, err
	}

	msg, msgOk := apr.GetValue(cli.CommitMessageArg)
	if !msgOk {
		hh, err := headCommit.HashOf()
		if err != nil {
			return nil, err
		}

		cmh, err := mergeCommit.HashOf()
		if err != nil {
			return nil, err
		}

		msg = fmt.Sprintf("SQL Generated commit merging %s into %s", hh.String(), cmh.String())
	}

	// TODO: refactor, redundant
	var name, email string
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
		if err != nil {
			return nil, err
		}
	} else {
		name = dSess.Username
		email = dSess.Email
	}

	t := ctx.QueryTime()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		var err error
		t, err = cli.ParseDate(commitTimeStr)
		if err != nil {
			return nil, err
		}
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
		Message:    msg,
		Date:       t,
		AllowEmpty: apr.Contains(cli.AllowEmptyFlag),
		Force:      apr.Contains(cli.ForceFlag),
		Name:       name,
		Email:      email,
	})
	if err != nil {
		return nil, err
	}

	return ws, dSess.SetWorkingSet(ctx, dbName, ws.ClearMerge(), nil)
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

func (d DoltMergeFunc) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewDoltMergeFunc(ctx, children...)
}

func NewDoltMergeFunc(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	return &DoltMergeFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}
