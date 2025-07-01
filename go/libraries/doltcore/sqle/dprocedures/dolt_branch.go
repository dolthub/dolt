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

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
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

	// CreateBranchArgParser has the common flags for the command line and the stored procedure.
	// The stored procedure doesn't support all actions, so we have a shorter description for -r.
	ap := cli.CreateBranchArgParser()
	ap.SupportsFlag(cli.RemoteParam, "r", "Delete a remote tracking branch.")
	apr, err := ap.Parse(args)
	if err != nil {
		return 1, err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	var rsc doltdb.ReplicationStatusController

	switch {
	case apr.Contains(cli.CopyFlag):
		err = copyBranch(ctx, dbData, apr, &rsc)
	case apr.Contains(cli.MoveFlag):
		err = renameBranch(ctx, dbData, apr, dSess, dbName, &rsc)
	case apr.Contains(cli.DeleteFlag), apr.Contains(cli.DeleteForceFlag):
		err = deleteBranches(ctx, dbData, apr, dSess, dbName, &rsc)
	case apr.ContainsAny(cli.SetUpstreamFlag, cli.TrackFlag):
		err = setBranchUpstream(ctx, dbData, apr, &rsc)
	default:
		err = createNewBranch(ctx, dbData, apr, &rsc)
	}

	if err != nil {
		return 1, err
	} else {
		return 0, commitTransaction(ctx, dSess, &rsc)
	}
}

func commitTransaction(ctx *sql.Context, dSess *dsess.DoltSession, rsc *doltdb.ReplicationStatusController) error {
	currentTx := ctx.GetTransaction()

	err := dSess.CommitTransaction(ctx, currentTx)
	if err != nil {
		return err
	}
	newTx, err := dSess.StartTransaction(ctx, sql.ReadWrite)
	if err != nil {
		return err
	}
	ctx.SetTransaction(newTx)

	if rsc != nil {
		dsess.WaitForReplicationController(ctx, *rsc)
	}

	return nil
}

// renameBranch takes DoltSession and database name to try accessing file system for dolt database.
// If the oldBranch being renamed is the current branch on CLI, then RepoState head will be updated with the newBranch ref.
func renameBranch(ctx *sql.Context, dbData env.DbData[*sql.Context], apr *argparser.ArgParseResults, sess *dsess.DoltSession, dbName string, rsc *doltdb.ReplicationStatusController) error {
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
		var headOnCLI string
		fs, err := sess.Provider().FileSystemForDatabase(dbName)
		if err == nil {
			if repoState, err := env.LoadRepoState(fs); err == nil {
				headOnCLI = repoState.Head.Ref.GetPath()
			}
		}
		if headOnCLI == oldBranchName && sqlserver.RunningInServerMode() && !shouldAllowDefaultBranchDeletion(ctx) {
			return fmt.Errorf("unable to rename branch '%s', because it is the default branch for "+
				"database '%s'; this can by changed on the command line, by stopping the sql-server, "+
				"running `dolt checkout <another_branch> and restarting the sql-server", oldBranchName, dbName)
		}

	} else if err := branch_control.CanDeleteBranch(ctx, newBranchName); err != nil {
		// If force is enabled, we can overwrite the destination branch, so we require a permission check here, even if the
		// destination branch doesn't exist. An unauthorized user could simply rerun the command without the force flag.
		return err
	}

	headRef, err := dbData.Rsr.CWBHeadRef(ctx)
	if err != nil {
		return err
	}
	activeSessionBranch := headRef.GetPath()

	err = actions.RenameBranch(ctx, dbData, oldBranchName, newBranchName, sess.Provider(), force, rsc)
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

	err = sess.RenameBranchState(ctx, dbName, oldBranchName, newBranchName)
	if err != nil {
		return err
	}

	// If the active branch of the SQL session was renamed, switch to the new branch.
	if oldBranchName == activeSessionBranch {
		wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(newBranchName))
		if err != nil {
			return err
		}

		err = sess.SwitchWorkingSet(ctx, dbName, wsRef)
		if err != nil {
			return err
		}
	}

	// Update init.defaultbranch config if the renamed branch was the default branch
	doltConfig := loadConfig(ctx)
	currentDefaultBranch := doltConfig.GetStringOrDefault(config.InitBranchName, "")
	if currentDefaultBranch == oldBranchName {
		// Get the local config for writing
		if localConfig, ok := doltConfig.GetConfig(env.LocalConfig); ok {
			err = localConfig.SetStrings(map[string]string{config.InitBranchName: newBranchName})
			if err != nil {
				return fmt.Errorf("failed to update init.defaultbranch config: %w", err)
			}
		}
	}

	return nil
}

// deleteBranches takes DoltSession and database name to try accessing file system for dolt database.
// If the database is not session state db and the branch being deleted is the current branch on CLI, it will update
// the RepoState to set head as empty branchRef.
func deleteBranches(ctx *sql.Context, dbData env.DbData[*sql.Context], apr *argparser.ArgParseResults, sess *dsess.DoltSession, dbName string, rsc *doltdb.ReplicationStatusController) error {
	if apr.NArg() == 0 {
		return InvalidArgErr
	}

	currBase, currBranch := dsess.SplitRevisionDbName(ctx.GetCurrentDatabase())

	// The current branch on CLI can be deleted as user can be on different branch on SQL and delete it from SQL session.
	// To update current head info on RepoState, we need DoltEnv to load CLI environment.
	var headOnCLI string
	fs, err := sess.Provider().FileSystemForDatabase(dbName)
	if err == nil {
		if repoState, err := env.LoadRepoState(fs); err == nil {
			headOnCLI = repoState.Head.Ref.GetPath()
		}
	}

	// Verify that we can delete all branches before continuing
	for _, branchName := range apr.Args {
		if err = branch_control.CanDeleteBranch(ctx, branchName); err != nil {
			return err
		}
	}

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

		// If we deleted the branch this client is connected to, change the current branch to the default
		// TODO: this would be nice to do for every other session (or maybe invalidate sessions on this branch)
		if strings.EqualFold(currBranch, branchName) {
			ctx.SetCurrentDatabase(currBase)
		}

		if headOnCLI == branchName && sqlserver.RunningInServerMode() && !shouldAllowDefaultBranchDeletion(ctx) {
			return fmt.Errorf("unable to delete branch '%s', because it is the default branch for "+
				"database '%s'; this can by changed on the command line, by stopping the sql-server, "+
				"running `dolt checkout <another_branch> and restarting the sql-server", branchName, dbName)
		}

		remote := apr.Contains(cli.RemoteParam)

		err = actions.DeleteBranch(ctx, dbData, branchName, actions.DeleteOptions{
			Force:  force,
			Remote: remote,
		}, dSess.Provider(), rsc)
		if err != nil {
			return err
		}

		// If the session has this branch checked out, we need to change that to the default head
		headRef, err := dSess.CWBHeadRef(ctx, currBase)
		if err != nil {
			return err
		}

		if headRef == ref.NewBranchRef(branchName) {
			err = dSess.RemoveBranchState(ctx, currBase, branchName)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// shouldAllowDefaultBranchDeletion returns true if the default branch deletion check should be
// bypassed for testing. This should only ever be true for tests that need to invalidate a databases
// default branch to test recovery from a bad state. We determine if the check should be bypassed by
// looking for the presence of an undocumented dolt user var, dolt_allow_default_branch_deletion.
func shouldAllowDefaultBranchDeletion(ctx *sql.Context) bool {
	_, userVar, _ := ctx.Session.GetUserVariable(ctx, "dolt_allow_default_branch_deletion")
	return userVar != nil
}

// validateBranchNotActiveInAnySession returns an error if the specified branch is currently
// selected as the active branch for any active server sessions.
func validateBranchNotActiveInAnySession(ctx *sql.Context, branchName string) error {
	currentDbName := ctx.GetCurrentDatabase()
	currentDbName, _ = dsess.SplitRevisionDbName(currentDbName)
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
		if session.ID() == ctx.Session.ID() {
			return false, nil
		}

		sess, ok := session.(*dsess.DoltSession)
		if !ok {
			return false, fmt.Errorf("unexpected session type: %T", session)
		}

		sessionDbName := sess.Session.GetCurrentDatabase()
		baseName, _ := dsess.SplitRevisionDbName(sessionDbName)
		if len(baseName) == 0 || baseName != currentDbName {
			return false, nil
		}

		activeBranchRef, err := sess.CWBHeadRef(ctx, sessionDbName)
		if err != nil {
			// The above will throw an error if the current DB doesn't have a head ref, in which case we don't need to
			// consider it
			return false, nil
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

func setBranchUpstream(ctx *sql.Context, dbData env.DbData[*sql.Context], apr *argparser.ArgParseResults, rsc *doltdb.ReplicationStatusController) error {
	var branchName string
	var err error

	if apr.NArg() == 0 {
		branchName, err = currentBranch(ctx)
		if err != nil {
			return err
		}
	} else {
		branchName = apr.Arg(0)
		ok, err := actions.IsBranch(ctx, dbData.Ddb, branchName)
		if err != nil {
			return err
		}
		if !ok {
			return createNewBranch(ctx, dbData, apr, rsc)
		}
	}

	var fullRemote string
	if apr.Contains(cli.TrackFlag) {
		if apr.NArg() < 2 || apr.NArg() > 3 {
			return InvalidArgErr
		}

		fullRemote = apr.Arg(1)
	} else {
		if apr.NArg() > 2 {
			return InvalidArgErr
		}
		var ok bool
		fullRemote, ok = apr.GetValue(cli.SetUpstreamFlag)
		if !ok {
			return fmt.Errorf("could not parse upstream value for dolt branch")
		}
	}

	// Check that the specified remote branch exists. Is there a better way?
	headRef, err := dbData.Rsr.CWBHeadRef(ctx)
	if err != nil {
		return err
	}
	cs, err := doltdb.NewCommitSpec(fullRemote)
	if err != nil {
		return err
	}
	if _, err = dbData.Ddb.Resolve(ctx, cs, headRef); err != nil {
		return err
	}

	remoteName, remoteBranch := actions.ParseRemoteBranchName(fullRemote)
	refSpec, err := ref.ParseRefSpecForRemote(remoteName, remoteBranch)
	if err != nil {
		return err
	}
	err = env.SetRemoteUpstreamForRefSpec(dbData.Rsw, refSpec, remoteName, ref.NewBranchRef(branchName))
	if err != nil {
		return err
	}

	return nil
}

func createNewBranch(ctx *sql.Context, dbData env.DbData[*sql.Context], apr *argparser.ArgParseResults, rsc *doltdb.ReplicationStatusController) error {
	if apr.NArg() == 0 || apr.NArg() > 3 {
		return InvalidArgErr
	}

	var branchName = apr.Arg(0)
	var startPt = "HEAD"
	if len(branchName) == 0 {
		return EmptyBranchNameErr
	}

	var remoteName, remoteBranch string
	var refSpec ref.RefSpec
	var err error
	var trackVal string
	var setTrackUpstream bool
	if apr.Contains(cli.SetUpstreamFlag) && apr.Contains(cli.TrackFlag) {
		return fmt.Errorf("error: --%s and --%s are mutually exclusive options.", cli.SetUpstreamFlag, cli.TrackFlag)
	} else if apr.Contains(cli.SetUpstreamFlag) {
		trackVal, setTrackUpstream = apr.GetValue(cli.SetUpstreamFlag)
	} else if apr.Contains(cli.TrackFlag) {
		if apr.NArg() < 2 { // Must specify both branch and remote name.
			return InvalidArgErr
		}
		setTrackUpstream = true
		trackVal = apr.Arg(1)
	}

	if apr.NArg() == 3 {
		startPt = apr.Arg(2)
	} else if apr.NArg() == 2 && !apr.Contains(cli.TrackFlag) {
		startPt = apr.Arg(1)
	} else if setTrackUpstream { // If a start was not given, and we're setting upstream, we use the remote as the start.
		startPt = trackVal
	}
	if len(startPt) == 0 {
		return InvalidArgErr
	}

	if setTrackUpstream {
		remoteName, remoteBranch = actions.ParseRemoteBranchName(trackVal)
		refSpec, err = ref.ParseRefSpecForRemote(remoteName, remoteBranch)
		if err != nil {
			return err
		}
	}

	err = branch_control.CanCreateBranch(ctx, branchName)
	if err != nil {
		return err
	}
	err = actions.CreateBranchWithStartPt(ctx, dbData, branchName, startPt, apr.Contains(cli.ForceFlag), rsc)
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

func copyBranch(ctx *sql.Context, dbData env.DbData[*sql.Context], apr *argparser.ArgParseResults, rsc *doltdb.ReplicationStatusController) error {
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
	return copyABranch(ctx, dbData, srcBr, destBr, force, rsc)
}

func copyABranch(ctx *sql.Context, dbData env.DbData[*sql.Context], srcBr string, destBr string, force bool, rsc *doltdb.ReplicationStatusController) error {
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
	err := actions.CopyBranchOnDB(ctx, dbData.Ddb, srcBr, destBr, force, rsc)
	if err != nil {
		if err == doltdb.ErrBranchNotFound {
			return fmt.Errorf("fatal: A branch named '%s' not found", srcBr)
		} else if err == actions.ErrAlreadyExists {
			return fmt.Errorf("fatal: A branch named '%s' already exists.", destBr)
		} else if err == doltdb.ErrInvBranchName {
			return fmt.Errorf("fatal: '%s' is not a valid branch name.", destBr)
		} else {
			return fmt.Errorf("fatal: Unexpected error copying branch from '%s' to '%s'", srcBr, destBr)
		}
	}
	err = branch_control.AddAdminForContext(ctx, destBr)
	if err != nil {
		return err
	}

	return nil
}
