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
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrEmptyBranchName = errors.New("error: cannot checkout empty string")

var doltCheckoutSchema = []*sql.Column{
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

// doltCheckout is the stored procedure version for the CLI command `dolt checkout`.
func doltCheckout(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, message, err := doDoltCheckout(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res), message), nil
}

func doDoltCheckout(ctx *sql.Context, args []string) (statusCode int, successMessage string, err error) {
	currentDbName := ctx.GetCurrentDatabase()
	if len(currentDbName) == 0 {
		return 1, "", fmt.Errorf("Empty database name.")
	}

	argParser := cli.CreateCheckoutArgParser()
	// The --move flag is used internally by the `dolt checkout` CLI command. It is not intended for external use.
	// It mimics the behavior of the `dolt checkout` command line, moving the working set into the new branch.
	argParser.SupportsFlag(cli.MoveFlag, "m", "")
	apr, err := argParser.Parse(args)
	if err != nil {
		return 1, "", err
	}

	newBranch, _, err := parseBranchArgs(apr)
	if err != nil {
		return 1, "", err
	}

	branchOrTrack := newBranch != "" || apr.Contains(cli.TrackFlag)
	if apr.Contains(cli.TrackFlag) && apr.NArg() > 0 {
		return 1, "", errors.New("Improper usage. Too many arguments provided.")
	}
	if !branchOrTrack && apr.NArg() == 0 {
		return 1, "", errors.New("Improper usage.")
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, currentDbName)
	if !ok {
		return 1, "", fmt.Errorf("Could not load database %s", currentDbName)
	}

	// Prevent the -b option from being used to create new branches on read-only databases
	readOnlyDatabase, err := isReadOnlyDatabase(ctx, currentDbName)
	if err != nil {
		return 1, "", err
	}
	if newBranch != "" && readOnlyDatabase {
		return 1, "", fmt.Errorf("unable to create new branch in a read-only database")
	}

	updateHead := apr.Contains(cli.MoveFlag)

	var rsc doltdb.ReplicationStatusController
	// If we're switching branches, then we need to clear any Doltgres session objects since they're temporary
	dSess.DoltgresSessObj = nil

	// Checking out new branch.
	if branchOrTrack {
		newBranch, upstream, err := checkoutNewBranch(ctx, currentDbName, dbData, apr, &rsc, updateHead)
		if err != nil {
			return 1, "", err
		} else {
			return 0, generateSuccessMessage(newBranch, upstream), nil
		}
	}

	roots, ok := dSess.GetRoots(ctx, currentDbName)
	if !ok {
		return 1, "", fmt.Errorf("Could not load database %s", currentDbName)
	}

	// firstArg purpose depends on the context, it may be a branch, table, etc.
	firstArg := apr.Arg(0)
	if len(firstArg) == 0 {
		return 1, "", ErrEmptyBranchName
	}

	// check for detached HEAD state early - if the user is trying to checkout a tag or commit hash
	if apr.NArg() == 1 {
		err = validateNoDetachedHead(ctx, dbData.Ddb, firstArg, updateHead)
		if err != nil {
			return 1, "", err
		}
	}

	isModification, err := willModifyDb(ctx, dSess, dbData, currentDbName, firstArg, updateHead)
	if err != nil {
		return 1, "", err
	}
	if !isModification && apr.NArg() == 1 {
		return 0, fmt.Sprintf("Already on branch '%s'", firstArg), nil
	}

	// No ref explicitly specified but table(s) are
	dashDashPos := apr.PositionalArgsSeparatorIndex
	if dashDashPos == 0 {
		err = checkoutTablesFromHead(ctx, roots, currentDbName, apr.Args)
		if err != nil {
			return 1, "", err
		}
		return 0, successMessage, nil
	}

	localRefExists, err := actions.IsBranch(ctx, dbData.Ddb, firstArg)
	if err != nil {
		return 1, "", err
	}

	remoteRefs, err := actions.GetRemoteBranchRef(ctx, dbData.Ddb, firstArg)
	if err != nil {
		return 1, "", fmt.Errorf("unable to read remote refs from data repository: %v", err)
	}

	validRemoteRefExists := remoteRefs != nil && len(remoteRefs) >= 1

	if dashDashPos == 1 && (localRefExists || validRemoteRefExists) {
		// dolt checkout <ref> --: disambiguates a tracking branch when it shares a name with local table(s).
		if apr.NArg() == 1 { // assume some <ref> specified because dashDashPos is 1
			if localRefExists {
				err = checkoutExistingBranchWithWorkingSetFallback(ctx, currentDbName, firstArg, apr, dSess, &rsc)
				if err != nil {
					return 1, "", err
				}
				return 0, generateSuccessMessage(firstArg, ""), nil
			}

			upstream, err := checkoutRemoteBranch(ctx, dSess, currentDbName, dbData, firstArg, apr, &rsc)
			if err != nil {
				return 1, "", err
			}
			return 0, generateSuccessMessage(firstArg, upstream), nil
		}

		// git requires a local ref to already exist to check out tables
		err = checkoutTablesFromCommit(ctx, currentDbName, firstArg, apr.Args[1:], &rsc)
		if err != nil {
			return 1, "", err
		}
		return 0, "", nil
	}

	_, _, firstArgIsTable, err := actions.FindTableInRoots(ctx, roots, firstArg)
	if err != nil {
		return 1, "", err
	}

	// ambiguity `foo` is a table AND matches a tracking branch, but a local branch does not exist already
	if validRemoteRefExists && !localRefExists && firstArgIsTable {
		return 1, "", fmt.Errorf("'%s' could be both a local table and a tracking branch.\n"+
			"Please use -- to disambiguate.", firstArg)
	}

	if apr.NArg() > 1 && !firstArgIsTable {
		err = checkoutTablesFromCommit(ctx, currentDbName, firstArg, apr.Args[1:], &rsc)
		if err != nil {
			return 1, "", err
		}
		return 0, "", nil
	}

	// git prioritizes local and remote refs over tables
	if localRefExists {
		err = checkoutExistingBranchWithWorkingSetFallback(ctx, currentDbName, firstArg, apr, dSess, &rsc)
		if err != nil {
			return 1, "", err
		}
		return 0, generateSuccessMessage(firstArg, ""), nil
	}

	if validRemoteRefExists {
		upstream, err := checkoutRemoteBranch(ctx, dSess, currentDbName, dbData, firstArg, apr, &rsc)
		if err != nil {
			return 1, "", err
		}
		return 0, generateSuccessMessage(firstArg, upstream), nil
	}

	err = checkoutTablesFromHead(ctx, roots, currentDbName, apr.Args)
	if err != nil {
		return 1, "", err
	}

	return 0, "", nil
}

// parseBranchArgs returns the name of the new branch and whether or not it should be created forcibly. This asserts
// that the provided branch name may not be empty, so an empty string is returned where no -b or -B flag is provided.
func parseBranchArgs(apr *argparser.ArgParseResults) (newBranch string, createBranchForcibly bool, err error) {
	if apr.Contains(cli.CheckoutCreateBranch) && apr.Contains(cli.CreateResetBranch) {
		return "", false, errors.New("Improper usage. Cannot use both -b and -B.")
	}

	if newBranch, ok := apr.GetValue(cli.CheckoutCreateBranch); ok {
		if len(newBranch) == 0 {
			return "", false, ErrEmptyBranchName
		}
		return newBranch, false, nil
	}

	if newBranch, ok := apr.GetValue(cli.CreateResetBranch); ok {
		if len(newBranch) == 0 {
			return "", false, ErrEmptyBranchName
		}
		return newBranch, true, nil
	}

	dashDashPos := apr.PositionalArgsSeparatorIndex
	if dashDashPos >= 2 {
		return "", false, fmt.Errorf("only one reference expected, %d given", dashDashPos)
	}

	return "", false, nil
}

// isReadOnlyDatabase returns true if the named database is a read-only database. An error is returned
// if any issues are encountered while looking up the named database.
func isReadOnlyDatabase(ctx *sql.Context, dbName string) (bool, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	db, err := doltSession.Provider().Database(ctx, dbName)
	if err != nil {
		return false, err
	}

	rodb, ok := db.(sql.ReadOnlyDatabase)
	return ok && rodb.IsReadOnly(), nil
}

// createWorkingSetForLocalBranch will make a new working set for a local
// branch ref if one does not already exist. Can be used to fix up local branch
// state when branches have been created without working sets in the past.
//
// This makes it so that dolt_checkout can checkout workingset-less branches,
// the same as `dolt checkout` at the CLI. The semantics of exactly what
// working set gets created in the new case are different, since the CLI takes
// the working set with it.
//
// TODO: This is cribbed heavily from doltdb.*doltDB.NewBranchAtCommit.
func createWorkingSetForLocalBranch(ctx *sql.Context, ddb *doltdb.DoltDB, branchName string) error {
	branchRef := ref.NewBranchRef(branchName)
	commit, err := ddb.ResolveCommitRef(ctx, branchRef)
	if err != nil {
		return err
	}

	commitRoot, err := commit.GetRootValue(ctx)
	if err != nil {
		return err
	}

	wsRef, err := ref.WorkingSetRefForHead(branchRef)
	if err != nil {
		return err
	}

	_, err = ddb.ResolveWorkingSet(ctx, wsRef)
	if err == nil {
		// This already exists. Return...
		return nil
	}
	if !errors.Is(err, doltdb.ErrWorkingSetNotFound) {
		return err
	}

	ws := doltdb.EmptyWorkingSet(wsRef).WithWorkingRoot(commitRoot).WithStagedRoot(commitRoot)
	return ddb.UpdateWorkingSet(ctx, wsRef, ws, hash.Hash{} /* current hash... */, doltdb.TodoWorkingSetMeta(), nil)
}

// checkoutRemoteBranch checks out a remote branch creating a new local branch with the same name as the remote branch
// and set its upstream. The upstream persists out of sql session. Returns the name of the upstream remote and branch.
func checkoutRemoteBranch(
	ctx *sql.Context,
	dSess *dsess.DoltSession,
	dbName string,
	dbData env.DbData[*sql.Context],
	branchName string,
	apr *argparser.ArgParseResults,
	rsc *doltdb.ReplicationStatusController,
) (upstream string, err error) {
	remoteRefs, err := actions.GetRemoteBranchRef(ctx, dbData.Ddb, branchName)
	if err != nil {
		return "", errors.New("fatal: unable to read from data repository")
	}

	if len(remoteRefs) == 0 {
		return "", fmt.Errorf("error: could not find %s", branchName)
	} else if len(remoteRefs) == 1 {
		remoteRef := remoteRefs[0]
		err = actions.CreateBranchWithStartPt(ctx, dbData, branchName, remoteRef.String(), false, rsc)
		if err != nil {
			return "", err
		}

		// We need to commit the transaction here or else the branch we just created isn't visible to the current transaction,
		// and we are about to switch to it. So set the new branch head for the new transaction, then commit this one
		sess := dsess.DSessFromSess(ctx.Session)
		err = commitTransaction(ctx, sess, rsc)
		if err != nil {
			return "", err
		}

		err = checkoutExistingBranch(ctx, dbName, branchName, apr)
		if err != nil {
			return "", err
		}

		// After checking out a new branch, we need to reload the database.
		dbData, ok := dSess.GetDbData(ctx, dbName)
		if !ok {
			return "", fmt.Errorf("Could not reload database %s", dbName)
		}

		refSpec, err := ref.ParseRefSpecForRemote(remoteRef.GetRemote(), remoteRef.GetBranch())
		if err != nil {
			return "", errhand.BuildDError("%s: '%s'", err.Error(), remoteRef.GetRemote()).Build()
		}

		headRef, err := dbData.Rsr.CWBHeadRef(ctx)
		if err != nil {
			return "", err
		}

		err = env.SetRemoteUpstreamForRefSpec(dbData.Rsw, refSpec, remoteRef.GetRemote(), headRef)
		if err != nil {
			return "", err
		}

		dsess.WaitForReplicationController(ctx, *rsc)

		return remoteRef.GetPath(), nil
	} else {
		hint := `hint: If you meant to check out a remote tracking branch, on, e.g., 'origin',
hint: you can do so by fully qualifying the name with the --track option:
hint:
hint:        dolt checkout --track origin/<name>
`
		return "", fmt.Errorf(color.YellowString(hint)+"'%s' matched multiple (%v) remote tracking branches", branchName, len(remoteRefs))
	}
}

// checkoutNewBranch creates a new branch and makes it the active branch for the session.
// If isMove is true, this function also moves the working set from the current branch into the new branch.
// Returns the name of the new branch and the remote upstream branch (empty string if not applicable.)
func checkoutNewBranch(
	ctx *sql.Context,
	dbName string,
	dbData env.DbData[*sql.Context],
	apr *argparser.ArgParseResults,
	rsc *doltdb.ReplicationStatusController,
	isMove bool,
) (newBranchName string, remoteAndBranch string, err error) {
	var remoteName, remoteBranchName string
	var startPt = "head"
	var refSpec ref.RefSpec

	if apr.NArg() == 1 {
		startPt = apr.Arg(0)
	}

	trackVal, setTrackUpstream := apr.GetValue(cli.TrackFlag)
	if setTrackUpstream {
		if trackVal == "inherit" {
			return "", "", fmt.Errorf("--track='inherit' is not supported yet")
		} else if trackVal != "direct" {
			startPt = trackVal
		}
		remoteName, remoteBranchName = actions.ParseRemoteBranchName(startPt)
		refSpec, err = ref.ParseRefSpecForRemote(remoteName, remoteBranchName)
		if err != nil {
			return "", "", err
		}
		newBranchName = remoteBranchName
	}

	// A little wonky behavior here. parseBranchArgs is actually called twice because in this procedure we pass around
	// the parse results, but we also needed to parse the -b and -B flags in the main procedure. It ended up being
	// a little cleaner to just call it again here than to pass the results around.
	var createBranchForcibly bool
	var optionBBranch string
	optionBBranch, createBranchForcibly, err = parseBranchArgs(apr)
	if err != nil {
		return "", "", err
	}
	if optionBBranch != "" {
		newBranchName = optionBBranch
	}

	err = actions.CreateBranchWithStartPt(ctx, dbData, newBranchName, startPt, createBranchForcibly, rsc)
	if err != nil {
		return "", "", err
	}

	if setTrackUpstream {
		err = env.SetRemoteUpstreamForRefSpec(dbData.Rsw, refSpec, remoteName, ref.NewBranchRef(newBranchName))
		if err != nil {
			return "", "", err
		}
	} else if autoSetupMerge, err := loadConfig(ctx).GetString("branch.autosetupmerge"); err != nil || autoSetupMerge != "false" {
		remoteName, remoteBranchName = actions.ParseRemoteBranchName(startPt)
		refSpec, err = ref.ParseRefSpecForRemote(remoteName, remoteBranchName)
		if err == nil {
			err = env.SetRemoteUpstreamForRefSpec(dbData.Rsw, refSpec, remoteName, ref.NewBranchRef(newBranchName))
			if err != nil {
				return "", "", err
			}
		}
	}

	// We need to commit the transaction here or else the branch we just created isn't visible to the current transaction,
	// and we are about to switch to it. So set the new branch head for the new transaction, then commit this one
	sess := dsess.DSessFromSess(ctx.Session)
	err = commitTransaction(ctx, sess, rsc)
	if err != nil {
		return "", "", err
	}

	if remoteName != "" {
		remoteAndBranch = fmt.Sprintf("%s/%s", remoteName, remoteBranchName)
	}

	if isMove {
		return newBranchName, remoteAndBranch, doGlobalCheckout(ctx, newBranchName, apr.Contains(cli.ForceFlag), true)
	} else {

		wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(newBranchName))
		if err != nil {
			return "", "", err
		}

		err = sess.SwitchWorkingSet(ctx, dbName, wsRef)
		if err != nil {
			return "", "", err
		}
	}

	return newBranchName, remoteAndBranch, nil
}

// checkoutExistingBranch updates the active branch reference to point to an already existing branch.
func checkoutExistingBranch(
	ctx *sql.Context,
	dbName string,
	branchName string,
	apr *argparser.ArgParseResults,
) error {
	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(branchName))
	if err != nil {
		return err
	}

	if ctx.GetCurrentDatabase() != dbName {
		ctx.SetCurrentDatabase(dbName)
	}

	dSess := dsess.DSessFromSess(ctx.Session)

	if apr.Contains(cli.MoveFlag) {
		return doGlobalCheckout(ctx, branchName, apr.Contains(cli.ForceFlag), false)
	} else {
		err = dSess.SwitchWorkingSet(ctx, dbName, wsRef)
		if err != nil {
			return err
		}
	}

	return nil
}

// checkoutExistingBranchWithWorkingSetFallback checks out an existing branch, and if the working set does not exist,
// it creates a new working set for the branch pointing to the existing branch HEAD. This resolves the issue where
// a local branch ref was created without a working set, such as when running as a read replica with an old version of dolt.
func checkoutExistingBranchWithWorkingSetFallback(
	ctx *sql.Context,
	dbName string,
	branchName string,
	apr *argparser.ArgParseResults,
	dSess *dsess.DoltSession,
	rsc *doltdb.ReplicationStatusController,
) error {
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return fmt.Errorf("could not load database %s", dbName)
	}

	err := checkoutExistingBranch(ctx, dbName, branchName, apr)
	if errors.Is(err, doltdb.ErrWorkingSetNotFound) {
		// If there is a branch but there is no working set,
		// somehow the local branch ref was created without a
		// working set. This happened with old versions of dolt
		// when running as a read replica, for example. Try to
		// create a working set pointing at the existing branch
		// HEAD and check out the branch again.
		//
		// TODO: This is all quite racey, but so is the
		// handling in doltDB, etc.
		err = createWorkingSetForLocalBranch(ctx, dbData.Ddb, branchName)
		if err != nil {
			return err
		}

		// Since we've created new refs since the transaction began, we need to commit this transaction and
		// start a new one to avoid not found errors after this
		// TODO: this is much worse than other places we do this, because it's two layers of implicit behavior
		sess := dsess.DSessFromSess(ctx.Session)
		err = commitTransaction(ctx, sess, rsc)
		if err != nil {
			return err
		}

		err = checkoutExistingBranch(ctx, dbName, branchName, apr)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	return nil
}

// checkoutTablesFromCommit checks out the tables named from the branch named and overwrites those tables in the
// staged and working roots.
func checkoutTablesFromCommit(
	ctx *sql.Context,
	databaseName string,
	commitRef string,
	tables []string,
	rsc *doltdb.ReplicationStatusController,
) error {
	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, databaseName)
	if !ok {
		return fmt.Errorf("Could not load database %s", ctx.GetCurrentDatabase())
	}

	currentBranch, err := dSess.GetBranch(ctx)
	if err != nil {
		return err
	}
	if currentBranch == "" {
		return fmt.Errorf("error: no current branch")
	}
	currentBranchRef := ref.NewBranchRef(currentBranch)

	cs, err := doltdb.NewCommitSpec(commitRef)
	if err != nil {
		return err
	}

	headCommit, err := dbData.Ddb.Resolve(ctx, cs, currentBranchRef)
	if err != nil {
		return err
	}
	cm, ok := headCommit.ToCommit()
	if !ok {
		return fmt.Errorf("HEAD is not a commit")
	}

	headRoot, err := cm.GetRootValue(ctx)
	if err != nil {
		return err
	}

	ws, err := dSess.WorkingSet(ctx, databaseName)
	if err != nil {
		return err
	}

	var tableNames []doltdb.TableName
	if len(tables) == 1 && tables[0] == "." {
		tableNames, err = doltdb.UnionTableNames(ctx, ws.WorkingRoot())
		if err != nil {
			return err
		}
	} else {
		tableNames = make([]doltdb.TableName, len(tables))
		for i, table := range tables {
			// TODO: we should allow schema-qualified table names here as well
			name, _, tableExistsInHead, err := resolve.Table(ctx, headRoot, table)
			if err != nil {
				return err
			}
			if !tableExistsInHead {
				return fmt.Errorf("tablespec '%s' did not match any table(s) known to dolt", table) // commitref not mentioned in git
			}
			tableNames[i] = name
		}
	}

	newRoot, err := actions.MoveTablesBetweenRoots(ctx, tableNames, headRoot, ws.WorkingRoot())
	if err != nil {
		return err
	}

	dsess.WaitForReplicationController(ctx, *rsc)
	return dSess.SetWorkingSet(ctx, databaseName, ws.WithStagedRoot(newRoot).WithWorkingRoot(newRoot))
}

// doGlobalCheckout implements the behavior of the `dolt checkout` command line, moving the working set into
// the new branch and persisting the checked-out branch into future sessions
func doGlobalCheckout(ctx *sql.Context, branchName string, isForce bool, isNewBranch bool) error {
	err := MoveWorkingSetToBranch(ctx, branchName, isForce, isNewBranch)
	if err != nil && err != doltdb.ErrAlreadyOnBranch {
		return err
	}

	return nil
}

// checkoutTablesFromHead checks out the tables named from the current head and overwrites those tables in the
// working root. The working root is then set as the new staged root. Necessary since tables may exist outside
// of HEAD commit
func checkoutTablesFromHead(ctx *sql.Context, roots doltdb.Roots, name string, tables []string) error {
	var tableNames []doltdb.TableName
	var warningMsg strings.Builder
	var err error

	if len(tables) == 1 && tables[0] == "." {
		tableNames, err = doltdb.UnionTableNames(ctx, roots.Working)
		if err != nil {
			return err
		}
	} else {
		tableNames = make([]doltdb.TableName, 0, len(tables))
		for _, table := range tables {
			tbl, _, exists, err := actions.FindTableInRoots(ctx, roots, table)
			if err != nil {
				return err
			}
			if !exists {
				warningMsg.WriteString(fmt.Sprintf("error: tablespec '%s' did not match any table(s) known to dolt\n", table))
				continue
			}
			tableNames = append(tableNames, tbl)
		}
		if len(tableNames) == 0 && warningMsg.Len() > 0 {
			return errors.New(strings.TrimSuffix(warningMsg.String(), "\n"))
		}
	}

	if len(tableNames) > 0 {
		roots, err = actions.MoveTablesFromHeadToWorking(ctx, roots, tableNames)
		if err != nil {
			if doltdb.IsRootValUnreachable(err) {
				rt := doltdb.GetUnreachableRootType(err)
				return fmt.Errorf("error: unable to read the %s", rt.String())
			} else if actions.IsTblNotExist(err) {
				return fmt.Errorf("error: given tables do not exist")
			} else {
				return fmt.Errorf("fatal: Unexpected error checking out tables")
			}
		}

		dSess := dsess.DSessFromSess(ctx.Session)
		if err := dSess.SetRoots(ctx, name, roots); err != nil {
			return err
		}
		if err := commitTransaction(ctx, dSess, nil); err != nil {
			return err
		}
	}

	if warningMsg.Len() > 0 {
		return errors.New(strings.TrimSuffix(warningMsg.String(), "\n"))
	}
	return nil
}

// validateNoDetachedHead checks if the given branchName refers to a tag or commit hash
// which would result in a detached HEAD state, which Dolt doesn't support.
// Returns an error with appropriate message if it's a detached HEAD state, nil otherwise.
func validateNoDetachedHead(ctx *sql.Context, ddb *doltdb.DoltDB, branchName string, isMoveFlag bool) error {
	if isTag, err := actions.IsTag(ctx, ddb, branchName); err != nil {
		return err
	} else if isTag {
		// User tried to enter a detached head state, which we don't support.
		// Inform and suggest that they check-out a new branch at this tag instead.
		if isMoveFlag {
			return fmt.Errorf(`dolt does not support a detached head state. To create a branch at this tag, run: 
	dolt checkout %s -b {new_branch_name}`, branchName)
		} else {
			return fmt.Errorf(`dolt does not support a detached head state. To create a branch at this tag, run: 
	CALL DOLT_CHECKOUT('%s', '-b', <new_branch_name>)`, branchName)
		}
	}

	if doltdb.IsValidCommitHash(branchName) {
		// User tried to enter a detached head state, which we don't support.
		// Inform and suggest that they check-out a new branch at this commit instead.
		if isMoveFlag {
			return fmt.Errorf(`dolt does not support a detached head state. To create a branch at this commit instead, run:
	dolt checkout %s -b {new_branch_name}`, branchName)
		} else {
			return fmt.Errorf(`dolt does not support a detached head state. To create a branch at this commit instead, run:
	CALL DOLT_CHECKOUT('%s', '-b', <new_branch_name>)`, branchName)
		}
	}

	return nil
}
