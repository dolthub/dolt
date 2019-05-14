package commands

import (
	"bytes"
	"context"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/editor"
)

var commitShortDesc = `Record changes to the repository`
var commitLongDesc = "Stores the current contents of the staged tables in a new commit along with a log message from the " +
	"user describing the changes.\n" +
	"\n" +
	"The content to be added can be specified by using dolt add to incrementally \"add\" changes to the staged tables " +
	"before using the commit command (Note: even modified files must be \"added\");" +
	"\n" +
	"The log message can be added with the parameter -m <msg>.  If the -m parameter is not provided an editor will be " +
	"opened where you can review the commit and provide a log message.\n"
var commitSynopsis = []string{
	"[-m <msg>]",
}

func Commit(commandStr string, args []string, dEnv *env.DoltEnv) int {
	const (
		allowEmptyFlag   = "allow-empty"
		commitMessageArg = "message"
	)

	ap := argparser.NewArgParser()
	ap.SupportsString(commitMessageArg, "m", "msg", "Use the given <msg> as the commit message.")
	ap.SupportsFlag(allowEmptyFlag, "", "Allow recording a commit that has the exact same data as its sole parent. This is usually a mistake, so it is disabled by default. This option bypasses that safety.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, commitShortDesc, commitLongDesc, commitSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	msg, msgOk := apr.GetValue(commitMessageArg)
	if !msgOk {
		msg = getCommitMessageFromEditor(dEnv)
	}

	err := actions.CommitStaged(context.Background(), dEnv, msg, apr.Contains(allowEmptyFlag))
	return handleCommitErr(err, usage)
}

func handleCommitErr(err error, usage cli.UsagePrinter) int {
	if err == nil {
		return 0
	}

	if err == actions.ErrNameNotConfigured {
		bdr := errhand.BuildDError("Could not determine %s.", env.UserNameKey)
		bdr.AddDetails("dolt config [-global|local] -add %[1]s:\"FIRST LAST\"", env.UserNameKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if err == actions.ErrEmailNotConfigured {
		bdr := errhand.BuildDError("Could not determine %s.", env.UserEmailKey)
		bdr.AddDetails("dolt config [-global|local] -add %[1]s:\"EMAIL_ADDRESS\"", env.UserEmailKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if err == actions.ErrEmptyCommitMessage {
		bdr := errhand.BuildDError("Aborting commit due to empty commit message.")
		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if actions.IsNothingStaged(err) {
		notStaged := actions.NothingStagedDiffs(err)
		n := printDiffsNotStaged(cli.CliOut, notStaged, false, 0, []string{})

		if n == 0 {
			bdr := errhand.BuildDError(`no changes added to commit (use "dolt add")`)
			return HandleVErrAndExitCode(bdr.Build(), usage)
		}
	}

	verr := errhand.BuildDError("error: Failed to commit changes.").AddCause(err).Build()
	return HandleVErrAndExitCode(verr, usage)
}

func getCommitMessageFromEditor(dEnv *env.DoltEnv) string {
	var finalMsg string
	initialMsg := buildInitalCommitMsg(dEnv)
	backupEd := "vim"
	if ed, edSet := os.LookupEnv("EDITOR"); edSet {
		backupEd = ed
	}
	editorStr := dEnv.Config.GetStringOrDefault(env.DoltEditor, backupEd)

	cli.ExecuteWithStdioRestored(func() {
		commitMsg, _ := editor.OpenCommitEditor(*editorStr, initialMsg)
		finalMsg = parseCommitMessage(commitMsg)
	})
	return finalMsg
}

func buildInitalCommitMsg(dEnv *env.DoltEnv) string {
	initialNoColor := color.NoColor
	color.NoColor = true

	currBranch := dEnv.RepoState.Head.Ref
	stagedDiffs, notStagedDiffs, _ := actions.GetTableDiffs(context.Background(), dEnv)
	buf := bytes.NewBuffer([]byte{})

	workingInConflict, _, _, err := actions.GetTablesInConflict(context.Background(), dEnv)

	if err != nil {
		workingInConflict = []string{}
	}

	n := printStagedDiffs(buf, stagedDiffs, true)
	n = printDiffsNotStaged(buf, notStagedDiffs, true, n, workingInConflict)

	initialCommitMessage := "\n" + "# Please enter the commit message for your changes. Lines starting" + "\n" +
		"# with '#' will be ignored, and an empty message aborts the commit." + "\n# On branch " + currBranch.GetPath() + "\n#" + "\n"

	msgLines := strings.Split(buf.String(), "\n")
	for i, msg := range msgLines {
		msgLines[i] = "# " + msg
	}
	statusMsg := strings.Join(msgLines, "\n")

	color.NoColor = initialNoColor
	return initialCommitMessage + statusMsg
}

func parseCommitMessage(cm string) string {
	lines := strings.Split(cm, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if len(line) >= 1 && line[0] == '#' {
			continue
		}
		filtered = append(filtered, line)
	}
	return strings.Join(filtered, "\n")
}
