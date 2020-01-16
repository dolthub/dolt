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

func (cmd CheckoutCmd) Name() string {
	return "checkout"
}

func (cmd CheckoutCmd) Description() string {
	return "Checkout a branch or overwrite a table from HEAD."
}

func (cmd CheckoutCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, coShortDesc, coLongDesc, coSynopsis, ap)
}

func (cmd CheckoutCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(coBranchArg, "", "branch", "Create a new branch named <new_branch> and start it at <start_point>.")
	return ap
}

func (cmd CheckoutCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CHECKOUT
}

func (cmd CheckoutCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	helpPrt, usagePrt := cli.HelpAndUsagePrinters(commandStr, coShortDesc, coLongDesc, coSynopsis, ap)
	apr := cli.ParseArgs(ap, args, helpPrt)

	if (apr.Contains(coBranchArg) && apr.NArg() > 1) || (!apr.Contains(coBranchArg) && apr.NArg() == 0) {
		usagePrt()
		return 1
	} else {
		var verr errhand.VerboseError
		newBranch, nbOk := apr.GetValue(coBranchArg)

		if nbOk {
			startPt := "head"
			if apr.NArg() == 1 {
				startPt = apr.Arg(0)
			}

			verr = checkoutNewBranch(ctx, dEnv, newBranch, startPt)
		} else {
			name := apr.Arg(0)
			isBranch, rootsWithTable, err := actions.BranchOrTable(ctx, dEnv, name)

			if err != nil {
				verr = errhand.BuildDError("fatal: unable to read from data repository.").AddCause(err).Build()
			} else if !rootsWithTable.IsEmpty() || apr.NArg() > 1 {
				verr = checkoutTable(ctx, dEnv, apr.Args())
			} else if isBranch {
				verr = checkoutBranch(ctx, dEnv, name)
			} else {
				refs, err := dEnv.DoltDB.GetRefs(ctx)

				if err != nil {
					verr = errhand.BuildDError("fatal: unable to read from data repository.").AddCause(err).Build()
				}

				found := false
				for _, rf := range refs {
					if remRef, ok := rf.(ref.RemoteRef); ok && remRef.GetBranch() == name {
						verr = checkoutNewBranch(ctx, dEnv, name, rf.String())
						found = true
						break
					}
				}

				if !found {
					verr = errhand.BuildDError("error: could not find %s", name).Build()
				}
			}
		}

		if verr != nil {
			cli.PrintErrln(verr.Verbose())
			return 1
		}
	}

	return 0
}

func checkoutNewBranch(ctx context.Context, dEnv *env.DoltEnv, newBranch, startPt string) errhand.VerboseError {
	verr := createBranchWithStartPt(ctx, dEnv, newBranch, startPt, false)

	if verr != nil {
		return verr
	}

	return checkoutBranch(ctx, dEnv, newBranch)
}

func checkoutTable(ctx context.Context, dEnv *env.DoltEnv, tables []string) errhand.VerboseError {
	err := actions.CheckoutTables(ctx, dEnv, tables)

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
