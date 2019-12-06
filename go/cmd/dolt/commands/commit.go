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
	"errors"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/editor"
)

const (
	allowEmptyFlag   = "allow-empty"
	dateParam        = "date"
	commitMessageArg = "message"
)

var commitShortDesc = `Record changes to the repository`
var commitLongDesc = "Stores the current contents of the staged tables in a new commit along with a log message from the " +
	"user describing the changes.\n" +
	"\n" +
	"The content to be added can be specified by using dolt add to incrementally \"add\" changes to the staged tables " +
	"before using the commit command (Note: even modified files must be \"added\");" +
	"\n" +
	"The log message can be added with the parameter -m <msg>.  If the -m parameter is not provided an editor will be " +
	"opened where you can review the commit and provide a log message.\n" +
	"\n" +
	"The commit timestamp can be modified using the --date parameter.  Dates can be specified in the formats YYYY-MM-DD " +
	"YYYY-MM-DDTHH:MM:SS, or YYYY-MM-DDTHH:MM:SSZ07:00 (where 07:00 is the time zone offset)."
var commitSynopsis = []string{
	"[options]",
}

func Commit(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {

	ap := argparser.NewArgParser()
	ap.SupportsString(commitMessageArg, "m", "msg", "Use the given <msg> as the commit message.")
	ap.SupportsFlag(allowEmptyFlag, "", "Allow recording a commit that has the exact same data as its sole parent. This is usually a mistake, so it is disabled by default. This option bypasses that safety.")
	ap.SupportsString(dateParam, "", "date", "Specify the date used in the commit. If not specified the current system time is used.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, commitShortDesc, commitLongDesc, commitSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	msg, msgOk := apr.GetValue(commitMessageArg)
	if !msgOk {
		msg = getCommitMessageFromEditor(ctx, dEnv)
	}

	t := time.Now()
	if commitTimeStr, ok := apr.GetValue(dateParam); ok {
		var err error
		t, err = parseDate(commitTimeStr)

		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("error: invalid date").AddCause(err).Build(), usage)
		}
	}

	err := actions.CommitStaged(ctx, dEnv, msg, t, apr.Contains(allowEmptyFlag))
	if err == nil {
		// if the commit was successful, print it out using the log command
		return Log(ctx, "log", []string{"-n=1"}, dEnv)
	}

	return handleCommitErr(err, usage)
}

// we are more permissive than what is documented.
var supportedLayouts = []string{
	"2006/01/02",
	"2006/01/02T15:04:05",
	"2006/01/02T15:04:05Z07:00",

	"2006.01.02",
	"2006.01.02T15:04:05",
	"2006.01.02T15:04:05Z07:00",

	"2006-01-02",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05Z07:00",
}

func parseDate(dateStr string) (time.Time, error) {
	for _, layout := range supportedLayouts {
		t, err := time.Parse(layout, dateStr)

		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, errors.New("error: '" + dateStr + "' is not in a supported format.")
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
	stagedDiffs, notStagedDiffs, _ := actions.GetTableDiffs(ctx, dEnv)
	buf := bytes.NewBuffer([]byte{})

	workingInConflict, _, _, err := actions.GetTablesInConflict(ctx, dEnv)

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
