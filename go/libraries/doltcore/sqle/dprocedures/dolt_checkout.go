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
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrEmptyBranchName = errors.New("error: cannot checkout empty string")

// doltCheckout is the stored procedure version for the CLI command `dolt checkout`.
func doltCheckout(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltCheckout(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltCheckout(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	dbName, _, err := getRevisionForRevisionDatabase(ctx, dbName)
	if err != nil {
		return -1, err
	}

	apr, err := cli.CreateCheckoutArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	branchOrTrack := apr.Contains(cli.CheckoutCoBranch) || apr.Contains(cli.TrackFlag)
	if (branchOrTrack && apr.NArg() > 1) || (!branchOrTrack && apr.NArg() == 0) {
		return 1, errors.New("Improper usage.")
	}

	// Checking out new branch.
	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	if branchOrTrack {
		err = checkoutNewBranch(ctx, dbName, dbData, apr)
		if err != nil {
			return 1, err
		} else {
			return 0, nil
		}
	}

	name := apr.Arg(0)
	if len(name) == 0 {
		return 1, ErrEmptyBranchName
	}

	// Check if user wants to checkout branch.
	if isBranch, err := actions.IsBranch(ctx, dbData.Ddb, name); err != nil {
		return 1, err
	} else if isBranch {
		err = checkoutBranch(ctx, dbName, name)
		if errors.Is(err, doltdb.ErrWorkingSetNotFound) {
			// If there is a branch but there is no working set,
			// somehow the local branch ref was created without a
			// working set. This happened with old versions of dolt
			// when running as a read replica, for example. Try to
			// create a working set pointing at the existing branch
			// HEAD and check out the branch again.
			//
			// TODO: This is all quite racey, but so is the
			// handling in DoltDB, etc.
			err = createWorkingSetForLocalBranch(ctx, dbData.Ddb, name)
			if err != nil {
				return 1, err
			}

			err = checkoutBranch(ctx, dbName, name)
		}
		if err != nil {
			return 1, err
		}
		return 0, nil
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	err = checkoutTables(ctx, roots, dbName, args)
	if err != nil && apr.NArg() == 1 {
		err = checkoutRemoteBranch(ctx, dbName, dbData, name, apr)
	}

	if err != nil {
		return 1, err
	}

	return 0, nil
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
// TODO: This is cribbed heavily from doltdb.*DoltDB.NewBranchAtCommit.
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
	return ddb.UpdateWorkingSet(ctx, wsRef, ws, hash.Hash{} /* current hash... */, doltdb.TodoWorkingSetMeta())
}

// getRevisionForRevisionDatabase returns the root database name and revision for a database, or just the root database name if the specified db name is not a revision database.
func getRevisionForRevisionDatabase(ctx *sql.Context, dbName string) (string, string, error) {
	doltsess, ok := ctx.Session.(*dsess.DoltSession)
	if !ok {
		return "", "", fmt.Errorf("unexpected session type: %T", ctx.Session)
	}

	provider := doltsess.Provider()
	return provider.GetRevisionForRevisionDatabase(ctx, dbName)
}

// checkoutRemoteBranch checks out a remote branch creating a new local branch with the same name as the remote branch
// and set its upstream. The upstream persists out of sql session.
func checkoutRemoteBranch(ctx *sql.Context, dbName string, dbData env.DbData, branchName string, apr *argparser.ArgParseResults) error {
	remoteRefs, err := actions.GetRemoteBranchRef(ctx, dbData.Ddb, branchName)
	if err != nil {
		return errors.New("fatal: unable to read from data repository")
	}

	if len(remoteRefs) == 0 {
		return fmt.Errorf("error: could not find %s", branchName)
	} else if len(remoteRefs) == 1 {
		remoteRef := remoteRefs[0]
		err := actions.CreateBranchWithStartPt(ctx, dbData, branchName, remoteRef.String(), false)
		if err != nil {
			return err
		}
		err = checkoutBranch(ctx, dbName, branchName)
		if err != nil {
			return err
		}

		refSpec, err := ref.ParseRefSpecForRemote(remoteRef.GetRemote(), remoteRef.GetBranch())
		if err != nil {
			return errhand.BuildDError(fmt.Errorf("%w: '%s'", err, remoteRef.GetRemote()).Error()).Build()
		}

		return env.SetRemoteUpstreamForRefSpec(dbData.Rsw, refSpec, remoteRef.GetRemote(), dbData.Rsr.CWBHeadRef())
	} else {
		return fmt.Errorf("'%s' matched multiple (%v) remote tracking branches", branchName, len(remoteRefs))
	}
}

func checkoutNewBranch(ctx *sql.Context, dbName string, dbData env.DbData, apr *argparser.ArgParseResults) error {
	var newBranchName string
	var remoteName, remoteBranchName string
	var startPt = "head"
	var refSpec ref.RefSpec
	var err error

	if apr.NArg() == 1 {
		startPt = apr.Arg(0)
	}

	trackVal, setTrackUpstream := apr.GetValue(cli.TrackFlag)
	if setTrackUpstream {
		if trackVal == "inherit" {
			return fmt.Errorf("--track='inherit' is not supported yet")
		} else if trackVal != "direct" {
			startPt = trackVal
		}
		remoteName, remoteBranchName = actions.ParseRemoteBranchName(startPt)
		refSpec, err = ref.ParseRefSpecForRemote(remoteName, remoteBranchName)
		if err != nil {
			return err
		}
		newBranchName = remoteBranchName
	}

	if newBranch, ok := apr.GetValue(cli.CheckoutCoBranch); ok {
		if len(newBranch) == 0 {
			return ErrEmptyBranchName
		}
		newBranchName = newBranch
	}

	err = actions.CreateBranchWithStartPt(ctx, dbData, newBranchName, startPt, false)
	if err != nil {
		return err
	}
	err = checkoutBranch(ctx, dbName, newBranchName)
	if err != nil {
		return err
	}

	if setTrackUpstream {
		err = env.SetRemoteUpstreamForRefSpec(dbData.Rsw, refSpec, remoteName, ref.NewBranchRef(remoteBranchName))
		if err != nil {
			return err
		}
	} else if autoSetupMerge, err := loadConfig(ctx).GetString("branch.autosetupmerge"); err != nil || autoSetupMerge != "false" {
		remoteName, remoteBranchName = actions.ParseRemoteBranchName(startPt)
		refSpec, err = ref.ParseRefSpecForRemote(remoteName, remoteBranchName)
		if err != nil {
			return nil
		}
		err = env.SetRemoteUpstreamForRefSpec(dbData.Rsw, refSpec, remoteName, ref.NewBranchRef(remoteBranchName))
		if err != nil {
			return err
		}
	}

	return nil
}

func checkoutBranch(ctx *sql.Context, dbName string, branchName string) error {
	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(branchName))
	if err != nil {
		return err
	}

	if ctx.GetCurrentDatabase() != dbName {
		ctx.SetCurrentDatabase(dbName)
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	return dSess.SwitchWorkingSet(ctx, dbName, wsRef)
}

func checkoutTables(ctx *sql.Context, roots doltdb.Roots, name string, tables []string) error {
	roots, err := actions.MoveTablesFromHeadToWorking(ctx, roots, tables)

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
	return dSess.SetRoots(ctx, name, roots)
}
