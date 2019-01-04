package commands

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env/actions"
	"os"
)

var commitShortDesc = `Record changes to the repository`
var commitLongDesc = `Stores the current contents of the staged tables in a new commit along with a log message from the user describing the changes.

The content to be added can be specified by using dolt add to incrementally "add" changes to the staged tables before using the commit command (Note: even modified files must be "added");`
var commitSynopsis = []string{
	"-m <msg>",
}

func Commit(commandStr string, args []string, dEnv *env.DoltEnv) int {
	const commitMessageArg = "message"
	ap := argparser.NewArgParser()
	ap.SupportsString(commitMessageArg, "m", "msg", "Use the given <msg> as the commit message.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, commitShortDesc, commitLongDesc, commitSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	msg, msgOk := apr.GetValue(commitMessageArg)
	if !msgOk {
		fmt.Fprintln(os.Stderr, color.RedString("Missing required parameter -m"))
		usage()
		return 1
	}

	err := actions.CommitStaged(dEnv, msg)
	return handleCommitErr(err, usage)
}

func handleCommitErr(err error, usage cli.UsagePrinter) int {
	if err == actions.ErrNameNotConfigured {
		bdr := errhand.BuildDError("Could not determine %s.", env.UserNameKey)
		bdr.AddDetails("dolt config [-global|local] -set %[1]s:\"FIRST LAST\"", env.UserNameKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	} else if err == actions.ErrEmailNotConfigured {
		bdr := errhand.BuildDError("Could not determine %s.", env.UserEmailKey)
		bdr.AddDetails("dolt config [-global|local] -set %[1]s:\"EMAIL_ADDRESS\"", env.UserEmailKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	} else if actions.IsNothingStaged(err) {
		notStaged := actions.NothingStagedDiffs(err)
		printDiffsNotStaged(notStaged, false, 0)

		return 1
	} else if err != nil {
		verr := errhand.BuildDError("error: Failed to commit changes.").AddCause(err).Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	return 0
}
