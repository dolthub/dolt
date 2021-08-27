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

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var commitDocs = cli.CommandDocumentationContent{
	ShortDesc: "Record changes to the repository",
	LongDesc: `
	Stores the current contents of the staged tables in a new commit along with a log message from the user describing the changes.
	
	The content to be added can be specified by using dolt add to incrementally \"add\" changes to the staged tables before using the commit command (Note: even modified files must be \"added\").
	
	The log message can be added with the parameter {{.EmphasisLeft}}-m <msg>{{.EmphasisRight}}.  If the {{.LessThan}}-m{{.GreaterThan}} parameter is not provided an editor will be opened where you can review the commit and provide a log message.
	
	The commit timestamp can be modified using the --date parameter.  Dates can be specified in the formats {{.LessThan}}YYYY-MM-DD{{.GreaterThan}}, {{.LessThan}}YYYY-MM-DDTHH:MM:SS{{.GreaterThan}}, or {{.LessThan}}YYYY-MM-DDTHH:MM:SSZ07:00{{.GreaterThan}} (where {{.LessThan}}07:00{{.GreaterThan}} is the time zone offset)."
	`,
	Synopsis: []string{
		"[options]",
	},
}

type CommitCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CommitCmd) Name() string {
	return "commit"
}

// Description returns a description of the command
func (cmd CommitCmd) Description() string {
	return "Record changes to the repository."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd CommitCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cli.CreateCommitArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, commitDocs, ap))
}

// Exec executes the command
func (cmd CommitCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateCommitArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, commitDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	// Check if the -all param is provided. Stage all tables if so.
	allFlag := apr.Contains(cli.AllFlag)

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Couldn't get working root").AddCause(err).Build(), usage)
	}

	if allFlag {
		roots, err = actions.StageAllTables(ctx, roots, dEnv.Docs)
		if err != nil {
			return handleCommitErr(ctx, dEnv, err, help)
		}
	}

	var name, email string
	// Check if the author flag is provided otherwise get the name and email stored in configs
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
	} else {
		name, email, err = actions.GetNameAndEmail(dEnv.Config)
	}

	if err != nil {
		return handleCommitErr(ctx, dEnv, err, usage)
	}

	msg, msgOk := apr.GetValue(cli.CommitMessageArg)
	if !msgOk {
		msg = actions.GetCommitMessageFromEditor(ctx, dEnv)
	}

	t := doltdb.CommitNowFunc()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		var err error
		t, err = cli.ParseDate(commitTimeStr)

		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("error: invalid date").AddCause(err).Build(), usage)
		}
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Couldn't get working set").AddCause(err).Build(), usage)
	}

	var mergeParentCommits []*doltdb.Commit
	if ws.MergeActive() {
		mergeParentCommits = []*doltdb.Commit{ws.MergeState().Commit()}
	}

	_, err = actions.CommitStaged(ctx, roots, ws.MergeActive(), mergeParentCommits, dEnv.DbData(), actions.CommitStagedProps{
		Message:    msg,
		Date:       t,
		AllowEmpty: apr.Contains(cli.AllowEmptyFlag),
		Force:      apr.Contains(cli.ForceFlag),
		Name:       name,
		Email:      email,
	})

	if err != nil {
		return handleCommitErr(ctx, dEnv, err, usage)
	}

	err = dEnv.ClearMerge(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Couldn't update working set").AddCause(err).Build(), usage)
	}

	// if the commit was successful, print it out using the log command
	return LogCmd{}.Exec(ctx, "log", []string{"-n=1"}, dEnv)
}

func handleCommitErr(ctx context.Context, dEnv *env.DoltEnv, err error, usage cli.UsagePrinter) int {
	if err == nil {
		return 0
	}

	if err == doltdb.ErrNameNotConfigured {
		bdr := errhand.BuildDError("Could not determine %s.", env.UserNameKey)
		bdr.AddDetails("Log into DoltHub: dolt login")
		bdr.AddDetails("OR add name to config: dolt config [--global|--local] --add %[1]s \"FIRST LAST\"", env.UserNameKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if err == doltdb.ErrEmailNotConfigured {
		bdr := errhand.BuildDError("Could not determine %s.", env.UserEmailKey)
		bdr.AddDetails("Log into DoltHub: dolt login")
		bdr.AddDetails("OR add email to config: dolt config [--global|--local] --add %[1]s \"EMAIL_ADDRESS\"", env.UserEmailKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if err == doltdb.ErrEmptyCommitMessage {
		bdr := errhand.BuildDError("Aborting commit due to empty commit message.")
		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if actions.IsNothingStaged(err) {
		notStagedTbls := actions.NothingStagedTblDiffs(err)
		notStagedDocs := actions.NothingStagedDocsDiffs(err)
		n := actions.PrintDiffsNotStaged(ctx, dEnv, cli.CliOut, notStagedTbls, notStagedDocs, false, 0, nil, nil)

		if n == 0 {
			bdr := errhand.BuildDError(`no changes added to commit (use "dolt add")`)
			return HandleVErrAndExitCode(bdr.Build(), usage)
		}
	}

	if actions.IsTblInConflict(err) {
		inConflict := actions.GetTablesForError(err)
		bdr := errhand.BuildDError(`tables %v have unresolved conflicts from the merge. resolve the conflicts before commiting`, inConflict)
		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	verr := errhand.BuildDError("error: Failed to commit changes.").AddCause(err).Build()
	return HandleVErrAndExitCode(verr, usage)
}
