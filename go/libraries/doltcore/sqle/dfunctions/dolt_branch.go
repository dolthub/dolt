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
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const DoltBranchFuncName = "dolt_branch"

var EmptyBranchNameErr = errors.New("error: cannot branch empty string")
var InvalidArgErr = errors.New("error: invalid usage")

// Deprecated: please use the version in the dprocedures package
type DoltBranchFunc struct {
	expression.NaryExpression
}

// Deprecated: please use the version in the dprocedures package
func NewDoltBranchFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltBranchFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}

func (d DoltBranchFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_BRANCH(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltBranchFunc) Type() sql.Type {
	return sql.Int8
}

func (d DoltBranchFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltBranchFunc(children...)
}

func (d DoltBranchFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return 1, err
	}
	return DoDoltBranch(ctx, args)
}

func DoDoltBranch(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	apr, err := cli.CreateBranchArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	switch {
	case apr.Contains(cli.CopyFlag):
		err = copyBranch(ctx, dbData, apr)
	case apr.Contains(cli.MoveFlag):
		err = renameBranch(ctx, dbData, apr, dSess, dbName)
	case apr.Contains(cli.DeleteFlag), apr.Contains(cli.DeleteForceFlag):
		err = deleteBranches(ctx, dbData, apr, dSess, dbName)
	default:
		err = createNewBranch(ctx, dbData, apr)
	}

	if err != nil {
		return 1, err
	} else {
		return 0, nil
	}
}

// renameBranch takes DoltSession and database name to try accessing file system for dolt database.
// If the oldBranch being renamed is the current branch on CLI, then RepoState head will be updated with the newBranch ref.
func renameBranch(ctx *sql.Context, dbData env.DbData, apr *argparser.ArgParseResults, sess *dsess.DoltSession, dbName string) error {
	if apr.NArg() != 2 {
		return InvalidArgErr
	}
	oldBranchName, newBranchName := apr.Arg(0), apr.Arg(1)
	if oldBranchName == "" || newBranchName == "" {
		return EmptyBranchNameErr
	}
	if err := branch_control.CanDeleteBranch(ctx, oldBranchName); err != nil {
		return err
	}
	if err := branch_control.CanCreateBranch(ctx, newBranchName); err != nil {
		return err
	}
	force := apr.Contains(cli.ForceFlag)

	if !force {
		err := validateBranchNotActiveInAnySession(ctx, oldBranchName)
		if err != nil {
			return err
		}
	}

	err := actions.RenameBranch(ctx, dbData, loadConfig(ctx), oldBranchName, newBranchName, force)
	if err != nil {
		return err
	}

	// The current branch on CLI can be deleted as user can be on different branch on SQL and delete it from SQL session.
	// To update current head info on RepoState, we need DoltEnv to load CLI environment.
	if fs, err := sess.Provider().FileSystemForDatabase(dbName); err == nil {
		if repoState, err := env.LoadRepoState(fs); err == nil {
			if repoState.Head.Ref.GetPath() == oldBranchName {
				repoState.Head.Ref = ref.NewBranchRef(newBranchName)
				repoState.Save(fs)
			}
		}
	}

	return nil
}

// deleteBranches takes DoltSession and database name to try accessing file system for dolt database.
// If the database is not session state db and the branch being deleted is the current branch on CLI, it will update
// the RepoState to set head as empty branchRef.
func deleteBranches(ctx *sql.Context, dbData env.DbData, apr *argparser.ArgParseResults, sess *dsess.DoltSession, dbName string) error {
	if apr.NArg() == 0 {
		return InvalidArgErr
	}

	// The current branch on CLI can be deleted as user can be on different branch on SQL and delete it from SQL session.
	// To update current head info on RepoState, we need DoltEnv to load CLI environment.
	var rs *env.RepoState
	var headOnCLI string
	fs, err := sess.Provider().FileSystemForDatabase(dbName)
	if err == nil {
		if repoState, err := env.LoadRepoState(fs); err == nil {
			rs = repoState
			headOnCLI = repoState.Head.Ref.GetPath()
		}
	}

	// Verify that we can delete all branches before continuing
	for _, branchName := range apr.Args {
		if err = branch_control.CanDeleteBranch(ctx, branchName); err != nil {
			return err
		}
	}

	var updateFS = false
	for _, branchName := range apr.Args {
		if len(branchName) == 0 {
			return EmptyBranchNameErr
		}
		force := apr.Contains(cli.DeleteForceFlag) || apr.Contains(cli.ForceFlag)

		if !force {
			err = validateBranchNotActiveInAnySession(ctx, branchName)
			if err != nil {
				return err
			}
		}
		err = actions.DeleteBranch(ctx, dbData, loadConfig(ctx), branchName, actions.DeleteOptions{
			Force: force,
		})
		if err != nil {
			return err
		}
		if headOnCLI == branchName {
			updateFS = true
		}
	}

	if fs != nil && updateFS {
		rs.Head.Ref = ref.NewBranchRef("")
		rs.Save(fs)
	}

	return nil
}

// validateBranchNotActiveInAnySessions returns an error if the specified branch is currently
// selected as the active branch for any active server sessions.
func validateBranchNotActiveInAnySession(ctx *sql.Context, branchName string) error {
	currentDbName, _, err := getRevisionForRevisionDatabase(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}

	if currentDbName == "" {
		return nil
	}

	if sqlserver.RunningInServerMode() == false {
		return nil
	}

	runningServer := sqlserver.GetRunningServer()
	if runningServer == nil {
		return nil
	}
	sessionManager := runningServer.SessionManager()
	branchRef := ref.NewBranchRef(branchName)

	return sessionManager.Iter(func(session sql.Session) (bool, error) {
		dsess, ok := session.(*dsess.DoltSession)
		if !ok {
			return false, fmt.Errorf("unexpected session type: %T", session)
		}

		sessionDatabase := dsess.Session.GetCurrentDatabase()
		sessionDbName, _, err := getRevisionForRevisionDatabase(ctx, dsess.GetCurrentDatabase())
		if err != nil {
			return false, err
		}

		if len(sessionDatabase) == 0 || sessionDbName != currentDbName {
			return false, nil
		}

		activeBranchRef, err := dsess.CWBHeadRef(ctx, sessionDatabase)
		if err != nil {
			return false, err
		}

		if ref.Equals(branchRef, activeBranchRef) {
			return false, fmt.Errorf("unsafe to delete or rename branches in use in other sessions; " +
				"use --force to force the change")
		}

		return false, nil
	})
}

func loadConfig(ctx *sql.Context) *env.DoltCliConfig {
	// When executing branch actions from SQL, we don't have access to a DoltEnv like we do from
	// within the CLI. We can fake it here enough to get a DoltCliConfig, but we can't rely on the
	// DoltEnv because tests and production will run with different settings (e.g. in-mem versus file).
	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, "")
	return dEnv.Config
}

func createNewBranch(ctx *sql.Context, dbData env.DbData, apr *argparser.ArgParseResults) error {
	var branchName string
	var startPt = "HEAD"
	if apr.NArg() == 1 {
		branchName = apr.Arg(0)
	} else if apr.NArg() == 2 {
		branchName = apr.Arg(0)
		startPt = apr.Arg(1)
		if len(startPt) == 0 {
			return InvalidArgErr
		}
	}

	if len(branchName) == 0 {
		return EmptyBranchNameErr
	}

	if err := branch_control.CanCreateBranch(ctx, branchName); err != nil {
		return err
	}
	return actions.CreateBranchWithStartPt(ctx, dbData, branchName, startPt, apr.Contains(cli.ForceFlag))
}

func copyBranch(ctx *sql.Context, dbData env.DbData, apr *argparser.ArgParseResults) error {
	if apr.NArg() != 2 {
		return InvalidArgErr
	}

	srcBr := apr.Args[0]
	if len(srcBr) == 0 {
		return EmptyBranchNameErr
	}

	destBr := apr.Args[1]
	if len(destBr) == 0 {
		return EmptyBranchNameErr
	}

	force := apr.Contains(cli.ForceFlag)
	return copyABranch(ctx, dbData, srcBr, destBr, force)
}

func copyABranch(ctx *sql.Context, dbData env.DbData, srcBr string, destBr string, force bool) error {
	if err := branch_control.CanCreateBranch(ctx, destBr); err != nil {
		return err
	}
	err := actions.CopyBranchOnDB(ctx, dbData.Ddb, srcBr, destBr, force)
	if err != nil {
		if err == doltdb.ErrBranchNotFound {
			return errors.New(fmt.Sprintf("fatal: A branch named '%s' not found", srcBr))
		} else if err == actions.ErrAlreadyExists {
			return errors.New(fmt.Sprintf("fatal: A branch named '%s' already exists.", destBr))
		} else if err == doltdb.ErrInvBranchName {
			return errors.New(fmt.Sprintf("fatal: '%s' is not a valid branch name.", destBr))
		} else {
			return errors.New(fmt.Sprintf("fatal: Unexpected error copying branch from '%s' to '%s'", srcBr, destBr))
		}
	}

	return nil
}
