package commands

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env/actions"
	"sort"
)

var branchShortDesc = `List, create, or delete branches`
var branchLongDesc = `If <b>--list</b> is given, or if there are no non-option arguments, existing branches are listed; the current branch will be highlighted with an asterisk. 

The command's second form creates a new branch head named <branchname> which points to the current <b>HEAD</b>, or <start-point> if given.

Note that this will create the new branch, but it will not switch the working tree to it; use "dolt checkout <newbranch>" to switch to the new branche.

With a <b>-m</b>, <oldbranch> will be renamed to <newbranch>. If <newbranch> exists, -f must be used to force the rename to happen.

The <b>-c</b> options have the exact same semantics as <b>-m</b>, except instead of the branch being renamed it will be copied to a new name.

With a <b>-d</b>, <branchname> will be deleted. You may specify more than one branch for deletion.`

var branchForceFlagDesc = "Reset <branchname> to <startpoint>, even if <branchname> exists already. Without -f, dolt branch " +
	"refuses to change an existing branch. In combination with -d (or --delete), allow deleting the branch irrespective " +
	"of its merged status. In combination with -m (or --move), allow renaming the branch even if the new branch name " +
	"already exists, the same applies for -c (or --copy)."

var branchSynopsis = []string{
	`[--list] [-v]`,
	`[-f] <branchname> [<start-point>]`,
	`-m [-f] [<oldbranch>] <newbranch>`,
	`-c [-f] [<oldbranch>] <newbranch>`,
	`-d [-f] <branchname>...`,
}

const (
	listFlag    = "list"
	forceFlag   = "force"
	copyFlag    = "copy"
	moveFlag    = "move"
	deleteFlag  = "delete"
	verboseFlag = "verbose"
)

func Branch(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["start-point"] = "A commit that a new branch should point at."
	ap.SupportsFlag(listFlag, "", "List branches")
	ap.SupportsFlag(forceFlag, "f", branchForceFlagDesc)
	ap.SupportsFlag(copyFlag, "c", "Create a copy of a branch.")
	ap.SupportsFlag(moveFlag, "m", "Move/rename a branch")
	ap.SupportsFlag(deleteFlag, "d", "Delete a branch. The branch must be fully merged in its upstream branch.")
	ap.SupportsFlag(verboseFlag, "v", "When in list mode, show the hash and commit subject line for each head")
	help, usage := cli.HelpAndUsagePrinters(commandStr, branchShortDesc, branchLongDesc, branchSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	switch {
	case apr.Contains(moveFlag):
		return moveBranch(dEnv, apr, usage)
	case apr.Contains(copyFlag):
		return copyBranch(dEnv, apr, usage)
	case apr.Contains(deleteFlag):
		return deleteBranches(dEnv, apr, usage)
	case apr.NArg() > 0:
		return createBranch(dEnv, apr, usage)
	case apr.Contains(listFlag):
		fallthrough
	default:
		return printBranches(dEnv, apr, usage)
	}
}

func printBranches(dEnv *env.DoltEnv, apr *argparser.ArgParseResults, _ cli.UsagePrinter) int {
	verbose := apr.Contains(verboseFlag)
	branches := dEnv.DoltDB.GetBranches()
	currentBranch := dEnv.RepoState.Branch
	sort.Strings(branches)

	for _, branch := range branches {
		line := ""
		if branch == currentBranch {
			line = fmt.Sprint("* ", color.GreenString("%-32s", branch))
		} else {
			line = fmt.Sprintf("  %-32s", branch)
		}

		if verbose {
			cs, _ := doltdb.NewCommitSpec("HEAD", branch)
			cm, err := dEnv.DoltDB.Resolve(cs)

			if err == nil {
				line = fmt.Sprintf("%s %s", line, cm.HashOf().String())
			}
		}

		cli.Println(line)
	}

	return 0
}

func moveBranch(dEnv *env.DoltEnv, apr *argparser.ArgParseResults, usage cli.UsagePrinter) int {
	if apr.NArg() != 2 {
		cli.Println("Invalid usage.")
		usage()
		return 1
	}

	force := apr.Contains(forceFlag)
	src := apr.Arg(0)
	dest := apr.Arg(1)
	err := actions.MoveBranch(dEnv, src, apr.Arg(1), force)

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

func copyBranch(dEnv *env.DoltEnv, apr *argparser.ArgParseResults, usage cli.UsagePrinter) int {
	if apr.NArg() != 2 {
		cli.Println("Invalid usage.")
		usage()
		return 1
	}

	force := apr.Contains(forceFlag)
	src := apr.Arg(0)
	dest := apr.Arg(1)
	err := actions.CopyBranch(dEnv, src, dest, force)

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

func deleteBranches(dEnv *env.DoltEnv, apr *argparser.ArgParseResults, usage cli.UsagePrinter) int {
	if apr.NArg() != 1 {
		cli.Println("Invalid usage.")
		usage()
		return 1
	}

	force := apr.Contains(forceFlag)
	brName := apr.Arg(0)
	err := actions.DeleteBranch(dEnv, brName, force)

	var verr errhand.VerboseError
	if err != nil {
		if err == doltdb.ErrBranchNotFound {
			verr = errhand.BuildDError("fatal: branch '%s' not found", brName).Build()
		} else if err == actions.ErrCOBranchDelete {
			verr = errhand.BuildDError("error: Cannot delete checked out branch '%s'", brName).Build()
		} else {
			bdr := errhand.BuildDError("fatal: Unexpected error deleting '%s'", brName)
			verr = bdr.AddCause(err).Build()
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func createBranch(dEnv *env.DoltEnv, apr *argparser.ArgParseResults, usage cli.UsagePrinter) int {
	if apr.NArg() == 0 || apr.NArg() > 2 {
		cli.Println("Invalid usage.")
		usage()
		return 1
	}

	newBranch := apr.Arg(0)
	startPt := "head"

	if apr.NArg() == 2 {
		startPt = apr.Arg(1)
	}

	verr := createBranchWithStartPt(dEnv, newBranch, startPt)
	return HandleVErrAndExitCode(verr, usage)
}

func createBranchWithStartPt(dEnv *env.DoltEnv, newBranch, startPt string) errhand.VerboseError {
	err := actions.CreateBranch(dEnv, newBranch, startPt)

	if err != nil {
		if err == actions.ErrAlreadyExists {
			return errhand.BuildDError("fatal: A branch named '%s' already exists.", newBranch).Build()
		} else if err == doltdb.ErrInvBranchName {
			bdr := errhand.BuildDError("fatal: '%s' is an invalid branch name.", newBranch)
			bdr.AddDetails("Branches must match the regex '%s'", doltdb.UserBranchRegexStr)
			return bdr.Build()
		} else if err == doltdb.ErrInvHash || doltdb.IsNotACommit(err) {
			bdr := errhand.BuildDError("fatal: '%s' is not a commit and a branch '%s' cannot be created from it", startPt, newBranch)
			return bdr.Build()
		} else {
			bdr := errhand.BuildDError("fatal: Unexpected error creating branch '%s'", newBranch)
			bdr.AddCause(err)
			return bdr.Build()
		}
	}

	return nil
}

func HandleVErrAndExitCode(verr errhand.VerboseError, usage cli.UsagePrinter) int {
	if verr != nil {
		cli.PrintErrln(verr.Verbose())

		if verr.ShouldPrintUsage() {
			usage()
		}

		return 1
	}

	return 0
}
