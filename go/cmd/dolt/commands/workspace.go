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
	"sort"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

var workspaceForceFlagDesc = "Reset {{.LessThan}}workspacename{{.GreaterThan}} to {{.LessThan}}startpoint{{.GreaterThan}}, even if {{.LessThan}}workspacename{{.GreaterThan}} exists already. Without {{.EmphasisLeft}}-f{{.EmphasisRight}}, {{.EmphasisLeft}}dolt workspace{{.EmphasisRight}} refuses to change an existing workspace. In combination with {{.EmphasisLeft}}-d{{.EmphasisRight}} (or {{.EmphasisLeft}}--delete{{.EmphasisRight}}), allow deleting the workspace irrespective of its merged status."

var workspaceDocs = cli.CommandDocumentationContent{
	ShortDesc: `List, create, or delete workspaces`,
	LongDesc: `If {{.EmphasisLeft}}--list{{.EmphasisRight}} is given, or if there are no non-option arguments, existing workspaces are listed. The current workspace will be highlighted with an asterisk. {{.EmphasisLeft}}-v{{.EmphasisRight}} causes the hash of the commit that the workspaces are at to be printed as well.

The command's second form creates a new workspace head named {{.LessThan}}workspacename{{.GreaterThan}} which points to the current {{.EmphasisLeft}}HEAD{{.EmphasisRight}}, or {{.LessThan}}start-point{{.GreaterThan}} if given.

Note that this will create the new workpsace, but it will not switch the working tree to it; use {{.EmphasisLeft}}dolt checkout <newworkspace>{{.EmphasisRight}} to switch to the new workspace.

With a {{.EmphasisLeft}}-d{{.EmphasisRight}}, {{.LessThan}}workspacename{{.GreaterThan}} will be deleted. You may specify more than one workspace for deletion.`,
	Synopsis: []string{
		`[--list] [-v]`,
		`dolt branch [-f] <workspacename> [<start-point>]`,
		`-d [-f] {{.LessThan}}workspacename{{.GreaterThan}}...`,
	},
}

type WorkspaceCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd WorkspaceCmd) Name() string {
	return "workspace"
}

// Description returns a description of the command
func (cmd WorkspaceCmd) Description() string {
	return "Create, list, delete workspaces."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd WorkspaceCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, workspaceDocs, ap))
}

func (cmd WorkspaceCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"start-point", "A commit that a new workspace should point at."})
	ap.SupportsFlag(listFlag, "", "List workspaces")
	ap.SupportsFlag(forceFlag, "f", workspaceForceFlagDesc)
	ap.SupportsFlag(deleteFlag, "d", "Delete a workspace. The workspace must be fully merged in its upstream branch.")
	ap.SupportsFlag(deleteForceFlag, "", "Shortcut for {{.EmphasisLeft}}--delete --force{{.EmphasisRight}}.")
	ap.SupportsFlag(verboseFlag, "v", "When in list mode, show the hash and commit subject line for each head")
	return ap
}

// Exec executes the command
func (cmd WorkspaceCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, workspaceDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	switch {
	case apr.Contains(deleteFlag):
		return deleteWorkspaces(ctx, dEnv, apr, usage)
	case apr.Contains(deleteForceFlag):
		return deleteForceWorkspaces(ctx, dEnv, apr, usage)
	case apr.Contains(listFlag):
		return printBranches(ctx, dEnv, apr, usage)
	case apr.NArg() > 0:
		return createWorkspace(ctx, dEnv, apr, usage)
	default:
		return printWorkspaces(ctx, dEnv, apr, usage)
	}
}

func printWorkspaces(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, _ cli.UsagePrinter) int {
	workspaceSet := set.NewStrSet(apr.Args())

	verbose := apr.Contains(verboseFlag)

	refs, err := dEnv.DoltDB.GetRefs(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("error: failed to read refs from db").AddCause(err).Build(), nil)
	}

	currentWorkspace := dEnv.RepoState.CWBHeadRef()
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].String() < refs[j].String()
	})

	for _, dRef := range refs {
		if workspaceSet.Size() > 0 && !workspaceSet.Contains(dRef.GetPath()) {
			continue
		}

		cs, _ := doltdb.NewCommitSpec(dRef.String())

		if dRef.GetType() != ref.WorkspaceRefType {
			continue
		}

		commitStr := ""
		workspaceName := "  " + dRef.GetPath()
		workspaceLen := len(workspaceName)
		if ref.Equals(dRef, currentWorkspace) {
			workspaceName = "* " + color.GreenString(dRef.GetPath())
		}

		if verbose {
			cm, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoState.CWBHeadRef())

			if err == nil {
				h, err := cm.HashOf()

				if err != nil {
					return HandleVErrAndExitCode(errhand.BuildDError("error: failed to hash commit").AddCause(err).Build(), nil)
				}

				commitStr = h.String()
			}
		}

		fmtStr := fmt.Sprintf("%%s%%%ds\t%%s", 48-workspaceLen)
		line := fmt.Sprintf(fmtStr, workspaceName, "", commitStr)

		cli.Println(line)
	}

	return 0
}

func createWorkspace(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, usage cli.UsagePrinter) int {
	if apr.NArg() == 0 || apr.NArg() > 2 {
		usage()
		return 1
	}

	newWorkspace := apr.Arg(0)
	startPt := "head"

	if apr.NArg() == 2 {
		startPt = apr.Arg(1)
	}

	verr := createWorkspaceWithStartPt(ctx, dEnv, newWorkspace, startPt)
	return HandleVErrAndExitCode(verr, usage)
}

func createWorkspaceWithStartPt(ctx context.Context, dEnv *env.DoltEnv, newWorkspace, startPt string) errhand.VerboseError {
	err := actions.CreateWorkspace(ctx, dEnv, newWorkspace, startPt)

	if err != nil {
		if err == actions.ErrAlreadyExists {
			return errhand.BuildDError("fatal: A workspace named '%s' already exists.", newWorkspace).Build()
		} else if err == doltdb.ErrInvWorkspaceName {
			bdr := errhand.BuildDError("fatal: '%s' is an invalid workspace name.", newWorkspace)
			return bdr.Build()
		} else if err == actions.ErrBranchNameExists {
			return errhand.BuildDError("fatal: Workspace name '%s' cannot be the name of an existing branch", newWorkspace).Build()
		} else if err == doltdb.ErrInvHash || doltdb.IsNotACommit(err) {
			bdr := errhand.BuildDError("fatal: '%s' is not a commit and a workspace '%s' cannot be created from it", startPt, newWorkspace)
			return bdr.Build()
		} else {
			bdr := errhand.BuildDError("fatal: Unexpected error creating workspace '%s'", newWorkspace)
			bdr.AddCause(err)
			return bdr.Build()
		}
	}

	success := color.GreenString("successfully created workspace: %s", newWorkspace)

	cli.Println(success)

	return nil
}

func deleteWorkspaces(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, usage cli.UsagePrinter) int {
	return handleDeleteWorkspaces(ctx, dEnv, apr, usage, apr.Contains(forceFlag))
}

func deleteForceWorkspaces(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, usage cli.UsagePrinter) int {
	return handleDeleteWorkspaces(ctx, dEnv, apr, usage, true)
}

func handleDeleteWorkspaces(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, usage cli.UsagePrinter, force bool) int {
	if apr.NArg() == 0 {
		usage()
		return 1
	}
	for i := 0; i < apr.NArg(); i++ {
		workspaceName := apr.Arg(i)

		err := actions.DeleteWorkspace(ctx, dEnv, workspaceName, actions.DeleteOptions{
			Force: force,
		})

		if err != nil {
			var verr errhand.VerboseError
			if err == doltdb.ErrWorkspaceNotFound {
				verr = errhand.BuildDError("fatal: workspace '%s' not found", workspaceName).Build()
			} else if err == actions.ErrCOBranchDelete {
				verr = errhand.BuildDError("error: Cannot delete checked out workspace '%s'", workspaceName).Build()
			} else {
				bdr := errhand.BuildDError("fatal: Unexpected error deleting '%s'", workspaceName)
				verr = bdr.AddCause(err).Build()
			}
			return HandleVErrAndExitCode(verr, usage)
		}
	}

	return HandleVErrAndExitCode(nil, usage)
}
