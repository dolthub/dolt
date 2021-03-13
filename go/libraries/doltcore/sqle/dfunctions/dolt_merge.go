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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const DoltMergeFuncName = "dolt_merge"

type DoltMergeFunc struct {
	expression.NaryExpression
}

func (d DoltMergeFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	sess := sqle.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(dbName)

	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	ap := cli.CreateMergeArgParser()
	args, err := getDoltArgs(ctx, row, d.Children())

	if err != nil {
		return nil, err
	}

	apr := cli.ParseArgs(ap, args, nil)

	if apr.ContainsAll(cli.SquashParam, cli.NoFFParam) {
		return 1, fmt.Errorf("error: Flags '--%s' and '--%s' cannot be used together.\n", cli.SquashParam, cli.NoFFParam)
	}

	if apr.Contains(cli.AbortParam) {
		if !dbData.Rsr.IsMergeActive() {
			return 1, fmt.Errorf("fatal: There is no merge to abort")
		}

		err = abortMerge(ctx, dbData)

		if err != nil {
			return 1, err
		}

		return "Merge aborted", nil
	}

	// The first argument should be the branch name.
	branchName := apr.Arg(0)

	ddb, ok := sess.GetDoltDB(dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	root, ok := sess.GetRoot(dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	hasConflicts, err := root.HasConflicts(ctx)
	if err != nil {
		return 1, err
	}

	if hasConflicts {
		return 1, errors.New("error: merge has unresolved conflicts")
	}

	if dbData.Rsr.IsMergeActive() {
		return 1, errors.New("error: merging is not possible because you have not committed an active merge")
	}

	parent, ph, parentRoot, err := getParent(ctx, sess, dbName)
	if err != nil {
		return nil, err
	}

	err = checkForUncommittedChanges(root, parentRoot)
	if err != nil {
		return nil, err
	}

	cm, cmh, err := getBranchCommit(ctx, branchName, ddb)
	if err != nil {
		return nil, err
	}

	// No need to write a merge commit, if the parent can ffw to the commit coming from the branch.
	canFF, err := parent.CanFastForwardTo(ctx, cm)
	if err != nil {
		return nil, err
	}

	if canFF {
		if apr.Contains(cli.NoFFParam) {
			err = executeNoFFMerge(ctx, sess, apr, dbData, parent, cm)
		} else {
			err = executeFFMerge(ctx, apr.Contains(cli.SquashParam), dbData, cm)
		}

		if err != nil {
			return nil, err
		}
		return cmh.String(), err
	}

	err = executeMerge(ctx, apr.Contains(cli.SquashParam), parent, cm, dbData)
	if err != nil {
		return nil, err
	}

	returnMsg := fmt.Sprintf("Updating %s..%s", cmh.String(), ph.String())

	return returnMsg, nil
}

func abortMerge(ctx *sql.Context, dbData env.DbData) error {
	err := actions.CheckoutAllTables(ctx, dbData)

	if err != nil {
		return err
	}

	err = dbData.Rsw.AbortMerge()
	if err != nil {
		return err
	}

	hh, err := dbData.Rsr.CWBHeadHash(ctx)
	if err != nil {
		return err
	}

	return setHeadAndWorkingSessionRoot(ctx, hh.String())
}

func executeMerge(ctx *sql.Context, squash bool, parent, cm *doltdb.Commit, dbData env.DbData) error {
	mergeRoot, mergeStats, err := merge.MergeCommits(ctx, parent, cm)

	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return errors.New("Already up to date.")
		case merge.ErrFastForward:
			panic("fast forward merge")
		default:
			return errors.New("Bad merge")
		}
	}

	return mergeRootToWorking(ctx, squash, dbData, mergeRoot, cm, mergeStats)
}

func executeFFMerge(ctx *sql.Context, squash bool, dbData env.DbData, cm2 *doltdb.Commit) error {
	rv, err := cm2.GetRootValue()

	if err != nil {
		return errors.New("Failed to return root value.")
	}

	stagedHash, err := dbData.Ddb.WriteRootValue(ctx, rv)

	if err != nil {
		return err
	}

	workingHash := stagedHash
	if !squash {
		err = dbData.Ddb.FastForward(ctx, dbData.Rsr.CWBHeadRef(), cm2)

		if err != nil {
			return err
		}
	}

	err = dbData.Rsw.SetWorkingHash(ctx, workingHash)
	if err != nil {
		return err
	}

	err = dbData.Rsw.SetStagedHash(ctx, stagedHash)
	if err != nil {
		return err
	}

	hh, err := dbData.Rsr.CWBHeadHash(ctx)
	if err != nil {
		return err
	}

	if squash {
		return setSessionRootExplicit(ctx, workingHash.String(), sqle.WorkingKeySuffix)
	} else {
		return setHeadAndWorkingSessionRoot(ctx, hh.String())
	}
}

func executeNoFFMerge(ctx *sql.Context, dSess *sqle.DoltSession, apr *argparser.ArgParseResults, dbData env.DbData, pr, cm2 *doltdb.Commit) error {
	mergedRoot, err := cm2.GetRootValue()
	if err != nil {
		return errors.New("Failed to return root value.")
	}

	err = mergeRootToWorking(ctx, false, dbData, mergedRoot, cm2, map[string]*merge.MergeStats{})
	if err != nil {
		return err
	}

	msg, msgOk := apr.GetValue(cli.CommitMessageArg)
	if !msgOk {
		ph, err := pr.HashOf()
		if err != nil {
			return err
		}

		cmh, err := cm2.HashOf()
		if err != nil {
			return err
		}

		msg = fmt.Sprintf("SQL Generated commit merging %s into %s", ph.String(), cmh.String())
	}

	var name, email string
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
		if err != nil {
			return err
		}
	} else {
		name = dSess.Username
		email = dSess.Email
	}

	// Specify the time if the date parameter is not.
	t := ctx.QueryTime()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		var err error
		t, err = cli.ParseDate(commitTimeStr)
		if err != nil {
			return err
		}
	}

	h, err := actions.CommitStaged(ctx, dbData, actions.CommitStagedProps{
		Message:          msg,
		Date:             t,
		AllowEmpty:       apr.Contains(cli.AllowEmptyFlag),
		CheckForeignKeys: !apr.Contains(cli.ForceFlag),
		Name:             name,
		Email:            email,
	})

	if err != nil {
		return err
	}

	return setHeadAndWorkingSessionRoot(ctx, h)
}

func mergeRootToWorking(ctx *sql.Context, squash bool, dbData env.DbData, mergedRoot *doltdb.RootValue, cm2 *doltdb.Commit, mergeStats map[string]*merge.MergeStats) error {
	h2, err := cm2.HashOf()
	if err != nil {
		return err
	}

	workingRoot := mergedRoot
	if !squash {
		err = dbData.Rsw.StartMerge(h2.String())

		if err != nil {
			return err
		}
	}

	workingHash, err := env.UpdateWorkingRoot(ctx, dbData.Ddb, dbData.Rsw, workingRoot)
	if err != nil {
		return err
	}

	hasConflicts := checkForConflicts(mergeStats)

	if hasConflicts {
		return errors.New("merge has conflicts. use the dolt_conflicts table to resolve.")
	}

	_, err = env.UpdateStagedRoot(ctx, dbData.Ddb, dbData.Rsw, workingRoot)
	if err != nil {
		return err
	}

	return setSessionRootExplicit(ctx, workingHash.String(), sqle.WorkingKeySuffix)
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
	return sql.Text
}

func (d DoltMergeFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltMergeFunc(children...)
}

func NewDoltMergeFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltMergeFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}
