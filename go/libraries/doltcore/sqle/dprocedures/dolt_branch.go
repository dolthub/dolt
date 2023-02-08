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

var (
	EmptyBranchNameErr = errors.New("error: cannot branch empty string")
	InvalidArgErr      = errors.New("error: invalid usage")
)

// doltBranch is the stored procedure version for the CLI command `dolt branch`.
func doltBranch(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltBranch(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltBranch(ctx *sql.Context, args []string) (int, error) {
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
	} else if err := branch_control.CanDeleteBranch(ctx, newBranchName); err != nil {
		// If force is enabled, we can overwrite the destination branch, so we require a permission check here, even if the
		// destination branch doesn't exist. An unauthorized user could simply rerun the command without the force flag.
		return err
	}

	err := actions.RenameBranch(ctx, dbData, oldBranchName, newBranchName, sess.Provider(), force)
	if err != nil {
		return err
	}
	err = branch_control.AddAdminForContext(ctx, newBranchName)
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
	dSess := dsess.DSessFromSess(ctx.Session)
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
		err = actions.DeleteBranch(ctx, dbData, branchName, actions.DeleteOptions{
			Force: force,
		}, dSess.Provider())
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

// TODO: the config should be available via the context, it's unnecessary to do an env.Load here and this should be removed
func loadConfig(ctx *sql.Context) *env.DoltCliConfig {
	// When executing branch actions from SQL, we don't have access to a DoltEnv like we do from
	// within the CLI. We can fake it here enough to get a DoltCliConfig, but we can't rely on the
	// DoltEnv because tests and production will run with different settings (e.g. in-mem versus file).
	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, "")
	return dEnv.Config
}

func createNewBranch(ctx *sql.Context, dbData env.DbData, apr *argparser.ArgParseResults) error {
	if apr.NArg() == 0 || apr.NArg() > 2 {
		return InvalidArgErr
	}

	var branchName = apr.Arg(0)
	var startPt = "HEAD"
	if len(branchName) == 0 {
		return EmptyBranchNameErr
	}
	if apr.NArg() == 2 {
		startPt = apr.Arg(1)
		if len(startPt) == 0 {
			return InvalidArgErr
		}
	}

	var remoteName, remoteBranch string
	var refSpec ref.RefSpec
	var err error
	trackVal, setTrackUpstream := apr.GetValue(cli.TrackFlag)
	if setTrackUpstream {
		if trackVal == "inherit" {
			return fmt.Errorf("--track='inherit' is not supported yet")
		} else if trackVal == "direct" && apr.NArg() != 2 {
			return InvalidArgErr
		}

		if apr.NArg() == 2 {
			// branchName and startPt are already set
			remoteName, remoteBranch = actions.ParseRemoteBranchName(startPt)
			refSpec, err = ref.ParseRefSpecForRemote(remoteName, remoteBranch)
			if err != nil {
				return err
			}
		} else {
			// if track option is defined with no value,
			// the track value can either be starting point name OR branch name
			startPt = trackVal
			remoteName, remoteBranch = actions.ParseRemoteBranchName(startPt)
			refSpec, err = ref.ParseRefSpecForRemote(remoteName, remoteBranch)
			if err != nil {
				branchName = trackVal
				startPt = apr.Arg(0)
				remoteName, remoteBranch = actions.ParseRemoteBranchName(startPt)
				refSpec, err = ref.ParseRefSpecForRemote(remoteName, remoteBranch)
				if err != nil {
					return err
				}
			}
		}
	}

	err = branch_control.CanCreateBranch(ctx, branchName)
	if err != nil {
		return err
	}

	err = actions.CreateBranchWithStartPt(ctx, dbData, branchName, startPt, apr.Contains(cli.ForceFlag))
	if err != nil {
		return err
	}

	if setTrackUpstream {
		// at this point new branch is created
		err = env.SetRemoteUpstreamForRefSpec(dbData.Rsw, refSpec, remoteName, ref.NewBranchRef(branchName))
		if err != nil {
			return err
		}
	}

	return nil
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
	// If force is enabled, we can overwrite the destination branch, so we require a permission check here, even if the
	// destination branch doesn't exist. An unauthorized user could simply rerun the command without the force flag.
	if force {
		if err := branch_control.CanDeleteBranch(ctx, destBr); err != nil {
			return err
		}
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
	err = branch_control.AddAdminForContext(ctx, destBr)
	if err != nil {
		return err
	}

	return nil
}
