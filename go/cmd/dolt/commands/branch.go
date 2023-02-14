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
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

var branchDocs = cli.CommandDocumentationContent{
	ShortDesc: `List, create, or delete branches`,
	LongDesc: `If {{.EmphasisLeft}}--list{{.EmphasisRight}} is given, or if there are no non-option arguments, existing branches are listed. The current branch will be highlighted with an asterisk. With no options, only local branches are listed. With {{.EmphasisLeft}}-r{{.EmphasisRight}}, only remote branches are listed. With {{.EmphasisLeft}}-a{{.EmphasisRight}} both local and remote branches are listed. {{.EmphasisLeft}}-v{{.EmphasisRight}} causes the hash of the commit that the branches are at to be printed as well.

The command's second form creates a new branch head named {{.LessThan}}branchname{{.GreaterThan}} which points to the current {{.EmphasisLeft}}HEAD{{.EmphasisRight}}, or {{.LessThan}}start-point{{.GreaterThan}} if given.

Note that this will create the new branch, but it will not switch the working tree to it; use {{.EmphasisLeft}}dolt checkout <newbranch>{{.EmphasisRight}} to switch to the new branch.

With a {{.EmphasisLeft}}-m{{.EmphasisRight}}, {{.LessThan}}oldbranch{{.GreaterThan}} will be renamed to {{.LessThan}}newbranch{{.GreaterThan}}. If {{.LessThan}}newbranch{{.GreaterThan}} exists, -f must be used to force the rename to happen.

The {{.EmphasisLeft}}-c{{.EmphasisRight}} options have the exact same semantics as {{.EmphasisLeft}}-m{{.EmphasisRight}}, except instead of the branch being renamed it will be copied to a new name.

With a {{.EmphasisLeft}}-d{{.EmphasisRight}}, {{.LessThan}}branchname{{.GreaterThan}} will be deleted. You may specify more than one branch for deletion.`,
	Synopsis: []string{
		`[--list] [-v] [-a] [-r]`,
		`[-f] {{.LessThan}}branchname{{.GreaterThan}} [{{.LessThan}}start-point{{.GreaterThan}}]`,
		`-m [-f] [{{.LessThan}}oldbranch{{.GreaterThan}}] {{.LessThan}}newbranch{{.GreaterThan}}`,
		`-c [-f] [{{.LessThan}}oldbranch{{.GreaterThan}}] {{.LessThan}}newbranch{{.GreaterThan}}`,
		`-d [-f] [-r] {{.LessThan}}branchname{{.GreaterThan}}...`,
	},
}

const (
	listFlag        = "list"
	datasetsFlag    = "datasets"
	showCurrentFlag = "show-current"
)

var ErrUnmergedBranchDelete = errors.New("The branch '%s' is not fully merged.\nIf you are sure you want to delete it, run 'dolt branch -D %s'.")

type BranchCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd BranchCmd) Name() string {
	return "branch"
}

// Description returns a description of the command
func (cmd BranchCmd) Description() string {
	return "Create, list, edit, delete branches."
}

func (cmd BranchCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(branchDocs, ap)
}

func (cmd BranchCmd) ArgParser() *argparser.ArgParser {
	ap := cli.CreateBranchArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"start-point", "A commit that a new branch should point at."})
	ap.SupportsFlag(listFlag, "", "List branches")
	ap.SupportsFlag(cli.VerboseFlag, "v", "When in list mode, show the hash and commit subject line for each head")
	ap.SupportsFlag(cli.AllFlag, "a", "When in list mode, shows remote tracked branches")
	ap.SupportsFlag(datasetsFlag, "", "List all datasets in the database")
	ap.SupportsFlag(cli.RemoteParam, "r", "When in list mode, show only remote tracked branches. When with -d, delete a remote tracking branch.")
	ap.SupportsFlag(showCurrentFlag, "", "Print the name of the current branch")
	return ap
}

// EventType returns the type of the event to log
func (cmd BranchCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_BRANCH
}

// Exec executes the command
func (cmd BranchCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, branchDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	switch {
	case apr.Contains(cli.MoveFlag):
		return moveBranch(ctx, dEnv, apr, usage)
	case apr.Contains(cli.CopyFlag):
		return copyBranch(ctx, dEnv, apr, usage)
	case apr.Contains(cli.DeleteFlag):
		return deleteBranches(ctx, dEnv, apr, usage, apr.Contains(cli.ForceFlag))
	case apr.Contains(cli.DeleteForceFlag):
		return deleteBranches(ctx, dEnv, apr, usage, true)
	case apr.Contains(listFlag):
		return printBranches(ctx, dEnv, apr, usage)
	case apr.Contains(showCurrentFlag):
		return printCurrentBranch(dEnv)
	case apr.Contains(datasetsFlag):
		return printAllDatasets(ctx, dEnv)
	case apr.NArg() > 0:
		return createBranch(ctx, dEnv, apr, usage)
	default:
		return printBranches(ctx, dEnv, apr, usage)
	}
}

func printBranches(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, _ cli.UsagePrinter) int {
	branchSet := set.NewStrSet(apr.Args)

	verbose := apr.Contains(cli.VerboseFlag)
	printRemote := apr.Contains(cli.RemoteParam)
	printAll := apr.Contains(cli.AllFlag)

	branches, err := dEnv.DoltDB.GetHeadRefs(ctx)

	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("error: failed to read refs from db").AddCause(err).Build(), nil)
	}

	currentBranch := dEnv.RepoStateReader().CWBHeadRef()
	sort.Slice(branches, func(i, j int) bool {
		return branches[i].String() < branches[j].String()
	})

	for _, branch := range branches {
		if branchSet.Size() > 0 && !branchSet.Contains(branch.GetPath()) {
			continue
		}

		cs, _ := doltdb.NewCommitSpec(branch.String())

		shouldPrint := false
		switch branch.GetType() {
		case ref.BranchRefType:
			shouldPrint = printAll || !printRemote
		case ref.RemoteRefType:
			shouldPrint = printAll || printRemote
		}
		if !shouldPrint {
			continue
		}

		commitStr := ""
		branchName := "  " + branch.GetPath()
		branchLen := len(branchName)
		if ref.Equals(branch, currentBranch) {
			branchName = "* " + color.GreenString(branch.GetPath())
		} else if branch.GetType() == ref.RemoteRefType {
			branchName = "  " + color.RedString("remotes/"+branch.GetPath())
			branchLen += len("remotes/")
		}

		if verbose {
			cm, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoStateReader().CWBHeadRef())

			if err == nil {
				h, err := cm.HashOf()

				if err != nil {
					return HandleVErrAndExitCode(errhand.BuildDError("error: failed to hash commit").AddCause(err).Build(), nil)
				}

				commitStr = h.String()
			}
		}

		fmtStr := fmt.Sprintf("%%s%%%ds\t%%s", 48-branchLen)
		line := fmt.Sprintf(fmtStr, branchName, "", commitStr)

		cli.Println(line)
	}

	return 0
}

func printCurrentBranch(dEnv *env.DoltEnv) int {
	cli.Println(dEnv.RepoStateReader().CWBHeadRef().GetPath())
	return 0
}

func printAllDatasets(ctx context.Context, dEnv *env.DoltEnv) int {
	refs, err := dEnv.DoltDB.GetHeadRefs(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), nil)
	}
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].String() < refs[j].String()
	})
	for _, r := range refs {
		cli.Println("  " + r.String())
	}

	branches, err := dEnv.DoltDB.GetBranches(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), nil)
	}
	sort.Slice(branches, func(i, j int) bool {
		return branches[i].String() < branches[j].String()
	})
	for _, b := range branches {
		var w ref.WorkingSetRef
		w, err = ref.WorkingSetRefForHead(b)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), nil)
		}

		_, err = dEnv.DoltDB.ResolveWorkingSet(ctx, w)
		if errors.Is(err, doltdb.ErrWorkingSetNotFound) {
			continue
		} else if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), nil)
		}
		cli.Println("  " + w.String())
	}
	return 0
}

func moveBranch(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, usage cli.UsagePrinter) int {
	if apr.NArg() != 2 {
		usage()
		return 1
	}

	force := apr.Contains(cli.ForceFlag)
	src := apr.Arg(0)
	dest := apr.Arg(1)
	err := actions.RenameBranch(ctx, dEnv.DbData(), src, apr.Arg(1), dEnv, force)

	var verr errhand.VerboseError
	if err != nil {
		if err == doltdb.ErrBranchNotFound {
			verr = errhand.BuildDError("fatal: branch '%s' not found", src).Build()
		} else if err == actions.ErrAlreadyExists {
			verr = errhand.BuildDError("fatal: A branch named '%s' already exists.", dest).Build()
		} else if err == doltdb.ErrInvBranchName {
			verr = errhand.BuildDError("fatal: '%s' is not a valid branch name.", dest).Build()
		} else if err == actions.ErrCOBranchDelete {
			verr = errhand.BuildDError("error: Cannot delete checked out branch '%s'", src).Build()
		} else {
			bdr := errhand.BuildDError("fatal: Unexpected error moving branch from '%s' to '%s'", src, dest)
			verr = bdr.AddCause(err).Build()
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func copyBranch(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, usage cli.UsagePrinter) int {
	if apr.NArg() != 2 {
		usage()
		return 1
	}

	force := apr.Contains(cli.ForceFlag)
	src := apr.Arg(0)
	dest := apr.Arg(1)
	err := actions.CopyBranch(ctx, dEnv, src, dest, force)

	var verr errhand.VerboseError
	if err != nil {
		if err == doltdb.ErrBranchNotFound {
			verr = errhand.BuildDError("fatal: branch '%s' not found", src).Build()
		} else if err == actions.ErrAlreadyExists {
			verr = errhand.BuildDError("fatal: A branch named '%s' already exists.", dest).Build()
		} else if err == doltdb.ErrInvBranchName {
			verr = errhand.BuildDError("fatal: '%s' is not a valid branch name.", dest).Build()
		} else {
			bdr := errhand.BuildDError("fatal: Unexpected error copying branch from '%s' to '%s'", src, dest)
			verr = bdr.AddCause(err).Build()
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func deleteBranches(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, usage cli.UsagePrinter, force bool) int {
	if apr.NArg() == 0 {
		usage()
		return 1
	}

	for i := 0; i < apr.NArg(); i++ {
		brName := apr.Arg(i)

		err := actions.DeleteBranch(ctx, dEnv.DbData(), brName, actions.DeleteOptions{
			Force:  force,
			Remote: apr.Contains(cli.RemoteParam),
		}, dEnv)

		if err != nil {
			var verr errhand.VerboseError
			if err == doltdb.ErrBranchNotFound {
				verr = errhand.BuildDError("fatal: branch '%s' not found", brName).Build()
			} else if err == actions.ErrUnmergedBranch {
				verr = errhand.BuildDError(ErrUnmergedBranchDelete.Error(), brName, brName).Build()
			} else if err == actions.ErrCOBranchDelete {
				verr = errhand.BuildDError("error: Cannot delete checked out branch '%s'", brName).Build()
			} else {
				bdr := errhand.BuildDError("fatal: Unexpected error deleting '%s'", brName)
				verr = bdr.AddCause(err).Build()
			}
			return HandleVErrAndExitCode(verr, usage)
		}
	}

	return HandleVErrAndExitCode(nil, usage)
}

func createBranch(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, usage cli.UsagePrinter) int {
	if apr.NArg() == 0 || apr.NArg() > 2 {
		usage()
		return 1
	}

	var remote string
	var remoteBranch string
	newBranch := apr.Arg(0)
	startPt := "head"
	if apr.NArg() == 2 {
		startPt = apr.Arg(1)
	}

	trackVal, setTrackUpstream := apr.GetValue(cli.TrackFlag)
	if setTrackUpstream {
		if trackVal == "inherit" {
			return HandleVErrAndExitCode(errhand.BuildDError("--track='inherit' is not supported yet").Build(), usage)
		} else if trackVal == "direct" && apr.NArg() != 2 {
			return HandleVErrAndExitCode(errhand.BuildDError("invalid arguments").Build(), usage)
		}

		remotes, err := dEnv.RepoStateReader().GetRemotes()
		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError(err.Error()).Build(), usage)
		}

		if apr.NArg() == 2 {
			// branchName and startPt are already set
			remote, remoteBranch = actions.ParseRemoteBranchName(startPt)
			_, remoteOk := remotes[remote]
			if !remoteOk {
				return HandleVErrAndExitCode(errhand.BuildDError("'%s' is not a valid remote ref and a branch '%s' cannot be created from it", startPt, newBranch).Build(), usage)
			}
		} else {
			// if track option is defined with no value,
			// the track value can either be starting point name OR branch name
			startPt = trackVal
			remote, remoteBranch = actions.ParseRemoteBranchName(startPt)
			_, remoteOk := remotes[remote]
			if !remoteOk {
				newBranch = trackVal
				startPt = apr.Arg(0)
				remote, remoteBranch = actions.ParseRemoteBranchName(startPt)
				_, remoteOk = remotes[remote]
				if !remoteOk {
					return HandleVErrAndExitCode(errhand.BuildDError("'%s' is not a valid remote ref and a branch '%s' cannot be created from it", startPt, newBranch).Build(), usage)
				}
			}
		}
	}

	err := actions.CreateBranchWithStartPt(ctx, dEnv.DbData(), newBranch, startPt, apr.Contains(cli.ForceFlag))
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError(err.Error()).Build(), usage)
	}

	if setTrackUpstream {
		// at this point new branch is created
		branchRef := ref.NewBranchRef(newBranch)
		verr := SetRemoteUpstreamForBranchRef(dEnv, remote, remoteBranch, branchRef)
		return HandleVErrAndExitCode(verr, usage)
	}

	return 0
}

func HandleVErrAndExitCode(verr errhand.VerboseError, usage cli.UsagePrinter) int {
	if verr != nil {
		if msg := verr.Verbose(); strings.TrimSpace(msg) != "" {
			cli.PrintErrln(msg)
		}

		if verr.ShouldPrintUsage() {
			usage()
		}

		return 1
	}

	return 0
}

// BuildVerrAndExit is a shortcut for building a verbose error and calling HandleVerrAndExitCode with it
func BuildVerrAndExit(errMsg string, cause error) int {
	return HandleVErrAndExitCode(errhand.BuildDError(errMsg).AddCause(cause).Build(), nil)
}
