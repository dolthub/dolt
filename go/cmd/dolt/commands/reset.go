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
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
)

const (
	SoftResetParam = "soft"
	HardResetParam = "hard"
)

var resetDocContent = cli.CommandDocumentationContent{
	ShortDesc: "Resets staged tables to their HEAD state",
	LongDesc: `Sets the state of a table in the staging area to be that table's value at HEAD

{{.EmphasisLeft}}dolt reset <tables>...{{.EmphasisRight}}"
	This form resets the values for all staged {{.LessThan}}tables{{.GreaterThan}} to their values at {{.EmphasisLeft}}HEAD{{.EmphasisRight}}. (It does not affect the working tree or
	the current branch.)

	This means that {{.EmphasisLeft}}dolt reset <tables>{{.EmphasisRight}} is the opposite of {{.EmphasisLeft}}dolt add <tables>{{.EmphasisRight}}.

	After running {{.EmphasisLeft}}dolt reset <tables>{{.EmphasisRight}} to update the staged tables, you can use {{.EmphasisLeft}}dolt checkout{{.EmphasisRight}} to check the
	contents out of the staged tables to the working tables.

dolt reset .
	This form resets {{.EmphasisLeft}}all{{.EmphasisRight}} staged tables to their values at HEAD. It is the opposite of {{.EmphasisLeft}}dolt add .{{.EmphasisRight}}`,

	Synopsis: []string{
		"{{.LessThan}}tables{{.GreaterThan}}...",
		"[--hard | --soft]",
	},
}

type ResetCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ResetCmd) Name() string {
	return "reset"
}

// Description returns a description of the command
func (cmd ResetCmd) Description() string {
	return "Remove table changes from the list of staged table changes."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd ResetCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cli.CreateResetArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, resetDocContent, ap))
}

// Exec executes the command
func (cmd ResetCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateResetArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, resetDocContent, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.ContainsArg(doltdb.DocTableName) {
		return HandleDocTableVErrAndExitCode()
	}

	workingRoot, stagedRoot, headRoot, verr := getAllRoots(ctx, dEnv)

	var err error
	if verr == nil {
		if apr.ContainsAll(HardResetParam, SoftResetParam) {
			verr = errhand.BuildDError("error: --%s and --%s are mutually exclusive options.", HardResetParam, SoftResetParam).Build()
			HandleVErrAndExitCode(verr, usage)
		} else if apr.Contains(HardResetParam) {
			err = actions.ResetHard(ctx, dEnv, apr, workingRoot, stagedRoot, headRoot)
		} else {
			stagedRoot, err = actions.ResetSoft(ctx, dEnv, apr, stagedRoot, headRoot)

			if err != nil {
				return handleResetError(err, usage)
			}

			printNotStaged(ctx, dEnv, stagedRoot)
		}
	}

	return handleResetError(err, usage)
}

func printNotStaged(ctx context.Context, dEnv *env.DoltEnv, staged *doltdb.RootValue) {
	// Printing here is best effort.  Fail silently
	working, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return
	}

	notStagedTbls, err := diff.GetTableDeltas(ctx, staged, working)
	if err != nil {
		return
	}

	notStagedDocs, err := diff.NewDocDiffs(ctx, working, nil, nil)
	if err != nil {
		return
	}

	removeModified := 0
	for _, td := range notStagedTbls {
		if !td.IsAdd() {
			removeModified++
		}
	}

	if removeModified+notStagedDocs.NumRemoved+notStagedDocs.NumModified > 0 {
		cli.Println("Unstaged changes after reset:")

		var lines []string
		for _, td := range notStagedTbls {
			if td.IsAdd() {
				//  per Git, unstaged new tables are untracked
				continue
			} else if td.IsDrop() {
				lines = append(lines, fmt.Sprintf("%s\t%s", tblDiffTypeToShortLabel[diff.RemovedTable], td.CurName()))
			} else if td.IsRename() {
				// per Git, unstaged renames are shown as drop + add
				lines = append(lines, fmt.Sprintf("%s\t%s", tblDiffTypeToShortLabel[diff.RemovedTable], td.FromName))
			} else {
				lines = append(lines, fmt.Sprintf("%s\t%s", tblDiffTypeToShortLabel[diff.ModifiedTable], td.CurName()))
			}
		}

		for _, docName := range notStagedDocs.Docs {
			ddt := notStagedDocs.DocToType[docName]
			if ddt != diff.AddedDoc {
				lines = append(lines, fmt.Sprintf("%s\t%s", docDiffTypeToShortLabel[ddt], docName))
			}
		}

		cli.Println(strings.Join(lines, "\n"))
	}
}

func handleResetError(err error, usage cli.UsagePrinter) int {
	if actions.IsTblNotExist(err) {
		tbls := actions.GetTablesForError(err)
		bdr := errhand.BuildDError("Invalid Table(s):")

		for _, tbl := range tbls {
			bdr.AddDetails("\t" + tbl)
		}

		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	var verr errhand.VerboseError = nil
	if err != nil {
		verr = errhand.BuildDError("error: Failed to reset changes.").AddCause(err).Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}

func getAllRoots(ctx context.Context, dEnv *env.DoltEnv) (*doltdb.RootValue, *doltdb.RootValue, *doltdb.RootValue, errhand.VerboseError) {
	workingRoot, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return nil, nil, nil, errhand.BuildDError("Unable to get staged.").AddCause(err).Build()
	}

	stagedRoot, err := dEnv.StagedRoot(ctx)

	if err != nil {
		return nil, nil, nil, errhand.BuildDError("Unable to get staged.").AddCause(err).Build()
	}

	headRoot, err := dEnv.HeadRoot(ctx)

	if err != nil {
		return nil, nil, nil, errhand.BuildDError("Unable to get at HEAD.").AddCause(err).Build()
	}

	return workingRoot, stagedRoot, headRoot, nil
}
