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
	"bytes"
	"context"
	"os"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/editor"
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

func Commit(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
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
		msg = getCommitMessageFromEditor(ctx, dEnv)
	}

	err := actions.CommitStaged(ctx, dEnv, msg, apr.Contains(allowEmptyFlag))
	if err == nil {
		// if the commit was successful, print it out using the log command
		return Log(ctx, "log", []string{"-n=1"}, dEnv)
	}

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
		notStagedTbls := actions.NothingStagedTblDiffs(err)
		notStagedDcs := actions.NothingStagedDcsDiffs(err)
		n := printDiffsNotStaged(cli.CliOut, notStagedTbls, notStagedDcs, false, 0, []string{}, []string{})

		if n == 0 {
			bdr := errhand.BuildDError(`no changes added to commit (use "dolt add")`)
			return HandleVErrAndExitCode(bdr.Build(), usage)
		}
	}

	verr := errhand.BuildDError("error: Failed to commit changes.").AddCause(err).Build()
	return HandleVErrAndExitCode(verr, usage)
}

func getCommitMessageFromEditor(ctx context.Context, dEnv *env.DoltEnv) string {
	var finalMsg string
	initialMsg := buildInitalCommitMsg(ctx, dEnv)
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

func buildInitalCommitMsg(ctx context.Context, dEnv *env.DoltEnv) string {
	initialNoColor := color.NoColor
	color.NoColor = true

	currBranch := dEnv.RepoState.Head.Ref
	stagedTblDiffs, notStagedTblDiffs, _ := actions.GetTableDiffs(ctx, dEnv)

	workingTblsInConflict, _, _, err := actions.GetTablesInConflict(ctx, dEnv)
	if err != nil {
		workingTblsInConflict = []string{}
	}

	_, notStagedDcDiffs, _ := actions.GetDocDiffs(ctx, dEnv)

	workingDcsInConflict, _, _, err := actions.GetDocsInConflict(ctx, dEnv)
	if err != nil {
		workingDcsInConflict = []string{}
	}

	buf := bytes.NewBuffer([]byte{})
	n := printStagedDiffs(buf, stagedTblDiffs, true)
	n = printDiffsNotStaged(buf, notStagedTblDiffs, notStagedDcDiffs, true, n, workingTblsInConflict, workingDcsInConflict)

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
