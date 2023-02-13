// Copyright 2019 Dolthub, Inc.
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

package commands

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var checkoutDocs = cli.CommandDocumentationContent{
	ShortDesc: `Switch branches or restore working tree tables`,
	LongDesc: `
Updates tables in the working set to match the staged versions. If no paths are given, dolt checkout will also update HEAD to set the specified branch as the current branch.

dolt checkout {{.LessThan}}branch{{.GreaterThan}}
   To prepare for working on {{.LessThan}}branch{{.GreaterThan}}, switch to it by updating the index and the tables in the working tree, and by pointing HEAD at the branch. Local modifications to the tables in the working
   tree are kept, so that they can be committed to the {{.LessThan}}branch{{.GreaterThan}}.

dolt checkout -b {{.LessThan}}new_branch{{.GreaterThan}} [{{.LessThan}}start_point{{.GreaterThan}}]
   Specifying -b causes a new branch to be created as if dolt branch were called and then checked out.

dolt checkout {{.LessThan}}table{{.GreaterThan}}...
  To update table(s) with their values in HEAD `,
	Synopsis: []string{
		`{{.LessThan}}branch{{.GreaterThan}}`,
		`{{.LessThan}}table{{.GreaterThan}}...`,
		`-b {{.LessThan}}new-branch{{.GreaterThan}} [{{.LessThan}}start-point{{.GreaterThan}}]`,
		`--track {{.LessThan}}remote{{.GreaterThan}}/{{.LessThan}}branch{{.GreaterThan}}`,
	},
}

type CheckoutCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CheckoutCmd) Name() string {
	return "checkout"
}

// Description returns a description of the command
func (cmd CheckoutCmd) Description() string {
	return "Checkout a branch or overwrite a table from HEAD."
}

func (cmd CheckoutCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateCheckoutArgParser()
	return cli.NewCommandDocumentation(checkoutDocs, ap)
}

func (cmd CheckoutCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCheckoutArgParser()
}

// EventType returns the type of the event to log
func (cmd CheckoutCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CHECKOUT
}

// Exec executes the command
func (cmd CheckoutCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateCheckoutArgParser()
	helpPrt, usagePrt := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, checkoutDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, helpPrt)
	if dEnv.IsLocked() {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(env.ErrActiveServerLock.New(dEnv.LockFile())), helpPrt)
	}

	branchOrTrack := apr.Contains(cli.CheckoutCoBranch) || apr.Contains(cli.TrackFlag)
	if (branchOrTrack && apr.NArg() > 1) || (!branchOrTrack && apr.NArg() == 0) {
		usagePrt()
		return 1
	}

	if branchOrTrack {
		verr := checkoutNewBranch(ctx, dEnv, apr)
		return HandleVErrAndExitCode(verr, usagePrt)
	}

	name := apr.Arg(0)
	force := apr.Contains(cli.ForceFlag)

	if len(name) == 0 {
		verr := errhand.BuildDError("error: cannot checkout empty string").Build()
		return HandleVErrAndExitCode(verr, usagePrt)
	}

	if isBranch, err := actions.IsBranch(ctx, dEnv.DoltDB, name); err != nil {
		verr := errhand.BuildDError("error: unable to determine type of checkout").AddCause(err).Build()
		return HandleVErrAndExitCode(verr, usagePrt)
	} else if isBranch {
		verr := checkoutBranch(ctx, dEnv, name, force)
		return HandleVErrAndExitCode(verr, usagePrt)
	}

	// Check if the user executed `dolt checkout .`
	if apr.NArg() == 1 && name == "." {
		roots, err := dEnv.Roots(ctx)
		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError(err.Error()).Build(), usagePrt)
		}
		headRef := dEnv.RepoStateReader().CWBHeadRef()
		ws, err := dEnv.WorkingSet(ctx)
		if err != nil {
			HandleVErrAndExitCode(errhand.BuildDError(err.Error()).Build(), usagePrt)
		}
		verr := actions.ResetHard(ctx, dEnv, "HEAD", roots, headRef, ws)
		return handleResetError(verr, usagePrt)
	}

	verr := checkoutTables(ctx, dEnv, args)
	if verr != nil && apr.NArg() == 1 {
		verr = checkoutRemoteBranchOrSuggestNew(ctx, dEnv, name)
	}

	return HandleVErrAndExitCode(verr, usagePrt)
}

func checkoutNewBranch(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	var newBranchName string
	var remoteName string
	var remoteBranchName string
	var startPt = "head"

	if apr.NArg() == 1 {
		startPt = apr.Arg(0)
	}

	trackVal, setTrackUpstream := apr.GetValue(cli.TrackFlag)
	if setTrackUpstream {
		if trackVal != "direct" && trackVal != "inherit" {
			startPt = trackVal
		} else if trackVal == "inherit" {
			return errhand.VerboseErrorFromError(fmt.Errorf("--track='inherit' is not supported yet"))
		}
		remoteName, remoteBranchName = actions.ParseRemoteBranchName(startPt)
		remotes, err := dEnv.RepoStateReader().GetRemotes()
		if err != nil {
			return errhand.BuildDError(err.Error()).Build()
		}
		_, remoteOk := remotes[remoteName]
		if !remoteOk {
			return errhand.BuildDError(fmt.Errorf("'%s' is not a valid remote ref and a branch '%s' cannot be created from it", startPt, remoteBranchName).Error()).Build()
		}
		newBranchName = remoteBranchName
	}

	if newBranch, ok := apr.GetValue(cli.CheckoutCoBranch); ok {
		if len(newBranch) == 0 {
			return errhand.BuildDError("error: cannot checkout empty string").Build()
		}
		newBranchName = newBranch
	}

	verr := checkoutNewBranchFromStartPt(ctx, dEnv, newBranchName, startPt)
	if verr != nil {
		return verr
	}

	// the new branch is checked out at this point
	if setTrackUpstream {
		verr = SetRemoteUpstreamForBranchRef(dEnv, remoteName, remoteBranchName, dEnv.RepoStateReader().CWBHeadRef())
		if verr != nil {
			return verr
		}
	} else if autoSetupMerge, err := dEnv.Config.GetString("branch.autosetupmerge"); err != nil || autoSetupMerge != "false" {
		// do guess remote branch if branch.autosetupmerge is not 'false', or if it is not set, it should default to 'true'.
		// if no remote, it should not return an error
		remotes, err := dEnv.RepoStateReader().GetRemotes()
		if err != nil {
			return nil
		}
		remoteName, remoteBranchName = actions.ParseRemoteBranchName(startPt)
		_, remoteOk := remotes[remoteName]
		if !remoteOk {
			return nil
		}
		verr = SetRemoteUpstreamForBranchRef(dEnv, remoteName, remoteBranchName, dEnv.RepoStateReader().CWBHeadRef())
		if verr != nil {
			return verr
		}
	}

	return nil
}

// checkoutRemoteBranchOrSuggestNew checks out a new branch guessing the remote branch,
// if there is a branch with matching name from exactly one remote.
func checkoutRemoteBranchOrSuggestNew(ctx context.Context, dEnv *env.DoltEnv, name string) errhand.VerboseError {
	remoteRefs, err := actions.GetRemoteBranchRef(ctx, dEnv.DoltDB, name)
	if err != nil {
		return errhand.BuildDError("fatal: unable to read from data repository.").AddCause(err).Build()
	}

	if len(remoteRefs) == 0 {
		// Check if the user is trying to enter a detached head state
		commit, _ := actions.MaybeGetCommit(ctx, dEnv, name)
		if commit != nil {
			// User tried to enter a detached head state, which we don't support.
			// Inform and suggest that they check-out a new branch at this commit instead.

			str := "dolt does not support a detached head state. To create a branch at this commit instead, run:\n\n" +
				"\tdolt checkout %s -b {new_branch_name}\n"

			return errhand.BuildDError(str, name).Build()
		}
		return errhand.BuildDError("error: could not find %s", name).Build()
	} else if len(remoteRefs) == 1 {
		verr := checkoutNewBranchFromStartPt(ctx, dEnv, name, remoteRefs[0].String())
		if verr != nil {
			return verr
		}
		return SetRemoteUpstreamForBranchRef(dEnv, remoteRefs[0].GetRemote(), remoteRefs[0].GetBranch(), dEnv.RepoStateReader().CWBHeadRef())
	} else {
		// TODO : add hint of using `dolt checkout --track <remote>/<branch>` when --track flag is supported
		return errhand.BuildDError("'%s' matched multiple (%v) remote tracking branches", name, len(remoteRefs)).Build()
	}
}

func checkoutNewBranchFromStartPt(ctx context.Context, dEnv *env.DoltEnv, newBranch, startPt string) errhand.VerboseError {
	err := actions.CreateBranchWithStartPt(ctx, dEnv.DbData(), newBranch, startPt, false)
	if err != nil {
		return errhand.BuildDError(err.Error()).Build()
	}

	return checkoutBranch(ctx, dEnv, newBranch, false)
}

func checkoutTables(ctx context.Context, dEnv *env.DoltEnv, tables []string) errhand.VerboseError {
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	err = actions.CheckoutTables(ctx, roots, dEnv.DbData(), tables)

	if err != nil {
		if doltdb.IsRootValUnreachable(err) {
			return unreadableRootToVErr(err)
		} else if actions.IsTblNotExist(err) {
			badTbls := actions.GetTablesForError(err)
			bdr := errhand.BuildDError("")
			for _, tbl := range badTbls {
				bdr.AddDetails("error: table '%s' did not match any table(s) known to dolt.", tbl)
			}
			return bdr.Build()
		} else {
			bdr := errhand.BuildDError("fatal: Unexpected error checking out tables")
			bdr.AddCause(err)
			return bdr.Build()
		}
	}

	return nil
}

func checkoutBranch(ctx context.Context, dEnv *env.DoltEnv, name string, force bool) errhand.VerboseError {
	err := actions.CheckoutBranch(ctx, dEnv, name, force)

	if err != nil {
		if err == doltdb.ErrBranchNotFound {
			return errhand.BuildDError("fatal: Branch '%s' not found.", name).Build()
		} else if doltdb.IsRootValUnreachable(err) {
			return unreadableRootToVErr(err)
		} else if actions.IsCheckoutWouldOverwrite(err) {
			tbls := actions.CheckoutWouldOverwriteTables(err)
			bdr := errhand.BuildDError("error: Your local changes to the following tables would be overwritten by checkout:")
			for _, tbl := range tbls {
				bdr.AddDetails("\t" + tbl)
			}

			bdr.AddDetails("Please commit your changes or stash them before you switch branches.")
			bdr.AddDetails("Aborting")
			return bdr.Build()
		} else if err == doltdb.ErrAlreadyOnBranch {
			// Being on the same branch shouldn't be an error
			cli.Printf("Already on branch '%s'\n", name)
			return nil
		} else if err == actions.ErrWorkingSetsOnBothBranches {
			str := fmt.Sprintf("error: There are uncommitted changes already on branch '%s'.", name) +
				"This can happen when someone modifies that branch in a SQL session." +
				fmt.Sprintf("You have uncommitted changes on this branch, and they would overwrite the uncommitted changes on branch %s on checkout.", name) +
				"To solve this problem, you can " +
				"1) commit or reset your changes on this branch, using `dolt commit` or `dolt reset`, before checking out the other branch, " +
				"2) use the `-f` flag with `dolt checkout` to force an overwrite, or " +
				"3) connect to branch '%s' with the SQL server and revert or commit changes there before proceeding."
			return errhand.BuildDError(str).AddCause(err).Build()
		} else {
			bdr := errhand.BuildDError("fatal: Unexpected error checking out branch '%s'", name)
			bdr.AddCause(err)
			return bdr.Build()
		}
	}

	cli.Printf("Switched to branch '%s'\n", name)

	return nil
}

// SetRemoteUpstreamForBranchRef sets upstream for checked out branch. This applies `dolt checkout <bn>`,
// if <bn> matches any remote branch name. This should not happen for `dolt checkout -b <bn>` case.
func SetRemoteUpstreamForBranchRef(dEnv *env.DoltEnv, remote, remoteBranch string, branchRef ref.DoltRef) errhand.VerboseError {
	refSpec, err := ref.ParseRefSpecForRemote(remote, remoteBranch)
	if err != nil {
		return errhand.BuildDError(fmt.Errorf("%w: '%s'", err, remote).Error()).Build()
	}

	err = env.SetRemoteUpstreamForRefSpec(dEnv.RepoStateWriter(), refSpec, remote, branchRef)
	if err != nil {
		return errhand.BuildDError(err.Error()).Build()
	}
	cli.Printf("branch '%s' set up to track '%s/%s'.\n", branchRef.GetPath(), remote, remoteBranch)

	return nil
}

func unreadableRootToVErr(err error) errhand.VerboseError {
	rt := doltdb.GetUnreachableRootType(err)
	bdr := errhand.BuildDError("error: unable to read the %s", rt.String())
	return bdr.AddCause(doltdb.GetUnreachableRootCause(err)).Build()
}
