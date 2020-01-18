// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

var coShortDesc = `Switch branches or restore working tree tables`
var coLongDesc = `Updates tables in the working set to match the staged versions. If no paths are given, dolt checkout will also update HEAD to set the specified branch as the current branch.

dolt checkout <branch>
   To prepare for working on <branch>, switch to it by updating the index and the tables in the working tree, and by pointing HEAD at the branch. Local modifications to the tables in the working
   tree are kept, so that they can be committed to the <branch>.

dolt checkout -b <new_branch> [<start_point>]
   Specifying -b causes a new branch to be created as if dolt branch were called and then checked out.

dolt checkout <table>...
  To update table(s) with their values in HEAD `

var coSynopsis = []string{
	`<branch>`,
	`<table>...`,
	`-b <new-branch> [<start-point>]`,
}

const coBranchArg = "b"

type CheckoutCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CheckoutCmd) Name() string {
	return "checkout"
}

// Description returns a description of the command
func (cmd CheckoutCmd) Description() string {
	return "Checkout a branch or overwrite a table from HEAD."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd CheckoutCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, coShortDesc, coLongDesc, coSynopsis, ap)
}

func (cmd CheckoutCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(coBranchArg, "", "branch", "Create a new branch named <new_branch> and start it at <start_point>.")
	return ap
}

// EventType returns the type of the event to log
func (cmd CheckoutCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CHECKOUT
}

// Exec executes the command
func (cmd CheckoutCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	helpPrt, usagePrt := cli.HelpAndUsagePrinters(commandStr, coShortDesc, coLongDesc, coSynopsis, ap)
	apr := cli.ParseArgs(ap, args, helpPrt)

	if (apr.Contains(coBranchArg) && apr.NArg() > 1) || (!apr.Contains(coBranchArg) && apr.NArg() == 0) {
		usagePrt()
		return 1
	}

	if apr.ContainsArg(doltdb.DocTableName) {
		verr := errhand.BuildDError("Use dolt checkout <filename> to check out individual docs.").Build()
		return HandleVErrAndExitCode(verr, usagePrt)
	}

	if newBranch, newBranchOk := apr.GetValue(coBranchArg); newBranchOk {
		verr := checkoutNewBranch(ctx, dEnv, newBranch, apr)
		return HandleVErrAndExitCode(verr, usagePrt)
	}

	name := apr.Arg(0)

	if isBranch, err := actions.IsBranch(ctx, dEnv, name); err != nil {
		verr := errhand.BuildDError("error: unable to determine type of checkout").AddCause(err).Build()
		return HandleVErrAndExitCode(verr, usagePrt)
	} else if isBranch {
		verr := checkoutBranch(ctx, dEnv, name)
		return HandleVErrAndExitCode(verr, usagePrt)
	}

	tbls, docs, err := actions.GetTblsAndDocDetails(dEnv, args)
	if err != nil {
		verr := errhand.BuildDError("error: unable to parse arguments.").AddCause(err).Build()
		return HandleVErrAndExitCode(verr, usagePrt)
	}

	verr := checkoutTablesAndDocs(ctx, dEnv, tbls, docs)

	if verr != nil && apr.NArg() == 1 {
		verr = checkoutRemoteBranch(ctx, dEnv, name)
	}

	return HandleVErrAndExitCode(verr, usagePrt)

}

func checkoutRemoteBranch(ctx context.Context, dEnv *env.DoltEnv, name string) errhand.VerboseError {
	if ref, refExists, err := getRemoteBranchRef(ctx, dEnv, name); err != nil {
		return errhand.BuildDError("fatal: unable to read from data repository.").AddCause(err).Build()
	} else if refExists {
		return checkoutNewBranchFromStartPt(ctx, dEnv, name, ref.String())
	} else {
		return errhand.BuildDError("error: could not find %s", name).Build()
	}
}

func getRemoteBranchRef(ctx context.Context, dEnv *env.DoltEnv, name string) (ref.DoltRef, bool, error) {
	refs, err := dEnv.DoltDB.GetRefs(ctx)

	if err != nil {
		return nil, false, err
	}

	for _, rf := range refs {
		if remRef, ok := rf.(ref.RemoteRef); ok && remRef.GetBranch() == name {
			return rf, true, nil
		}
	}

	return nil, false, err
}

func checkoutNewBranchFromStartPt(ctx context.Context, dEnv *env.DoltEnv, newBranch, startPt string) errhand.VerboseError {
	verr := createBranchWithStartPt(ctx, dEnv, newBranch, startPt, false)

	if verr != nil {
		return verr
	}

	return checkoutBranch(ctx, dEnv, newBranch)
}

func checkoutNewBranch(ctx context.Context, dEnv *env.DoltEnv, newBranch string, apr *argparser.ArgParseResults) errhand.VerboseError {
	startPt := "head"
	if apr.NArg() == 1 {
		startPt = apr.Arg(0)
	}

	verr := createBranchWithStartPt(ctx, dEnv, newBranch, startPt, false)

	if verr != nil {
		return verr
	}

	return checkoutBranch(ctx, dEnv, newBranch)
}

func checkoutTablesAndDocs(ctx context.Context, dEnv *env.DoltEnv, tables []string, docs []doltdb.DocDetails) errhand.VerboseError {
	err := actions.CheckoutTablesAndDocs(ctx, dEnv, tables, docs)

	if err != nil {
		if actions.IsRootValUnreachable(err) {
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

func checkoutBranch(ctx context.Context, dEnv *env.DoltEnv, name string) errhand.VerboseError {
	err := actions.CheckoutBranch(ctx, dEnv, name)

	if err != nil {
		if err == doltdb.ErrBranchNotFound {
			return errhand.BuildDError("fatal: Branch '%s' not found.", name).Build()
		} else if actions.IsRootValUnreachable(err) {
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
			return errhand.BuildDError("Already on branch '%s'", name).Build()
		} else {
			bdr := errhand.BuildDError("fatal: Unexpected error checking out branch '%s'", name)
			bdr.AddCause(err)
			return bdr.Build()
		}
	}

	cli.Printf("Switched to branch '%s'\n", name)

	return nil
}

func unreadableRootToVErr(err error) errhand.VerboseError {
	rt := actions.GetUnreachableRootType(err)
	bdr := errhand.BuildDError("error: unable to read the %s", rt.String())
	return bdr.AddCause(actions.GetUnreachableRootCause(err)).Build()
}
