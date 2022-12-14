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

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const (
	SoftResetParam = "soft"
	HardResetParam = "hard"
)

var resetDocContent = cli.CommandDocumentationContent{
	ShortDesc: "Resets staged or working tables to HEAD or a specified commit",
	LongDesc: "{{.EmphasisLeft}}dolt reset <tables>...{{.EmphasisRight}}" +
		"\n\n" +
		"The default form resets the values for all staged {{.LessThan}}tables{{.GreaterThan}} to their values at {{.EmphasisLeft}}HEAD{{.EmphasisRight}}. " +
		"It does not affect the working tree or the current branch." +
		"\n\n" +
		"This means that {{.EmphasisLeft}}dolt reset <tables>{{.EmphasisRight}} is the opposite of {{.EmphasisLeft}}dolt add <tables>{{.EmphasisRight}}." +
		"\n\n" +
		"After running {{.EmphasisLeft}}dolt reset <tables>{{.EmphasisRight}} to update the staged tables, you can use {{.EmphasisLeft}}dolt checkout{{.EmphasisRight}} to check the contents out of the staged tables to the working tables." +
		"\n\n" +
		"{{.EmphasisLeft}}dolt reset [--hard | --soft] <revision>{{.EmphasisRight}}" +
		"\n\n" +
		"This form resets all tables to values in the specified revision (i.e. commit, tag, working set). " +
		"The --soft option resets HEAD to a revision without changing the current working set. " +
		" The --hard option resets all three HEADs to a revision, deleting all uncommitted changes in the current working set." +
		"\n\n" +
		"{{.EmphasisLeft}}dolt reset .{{.EmphasisRight}}" +
		"\n\n" +
		"This form resets {{.EmphasisLeft}}all{{.EmphasisRight}} staged tables to their values at HEAD. It is the opposite of {{.EmphasisLeft}}dolt add .{{.EmphasisRight}}",
	Synopsis: []string{
		"{{.LessThan}}tables{{.GreaterThan}}...",
		"[--hard | --soft] {{.LessThan}}revision{{.GreaterThan}}",
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

func (cmd ResetCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateResetArgParser()
	return cli.NewCommandDocumentation(resetDocContent, ap)
}

func (cmd ResetCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateResetArgParser()
}

// Exec executes the command
func (cmd ResetCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateResetArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, resetDocContent, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if dEnv.IsLocked() {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(env.ErrActiveServerLock.New(dEnv.LockFile())), help)
	}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if apr.ContainsAll(HardResetParam, SoftResetParam) {
		verr := errhand.BuildDError("error: --%s and --%s are mutually exclusive options.", HardResetParam, SoftResetParam).Build()
		HandleVErrAndExitCode(verr, usage)
	} else if apr.Contains(HardResetParam) {
		arg := ""
		if apr.NArg() > 1 {
			return handleResetError(fmt.Errorf("--hard supports at most one additional param"), usage)
		} else if apr.NArg() == 1 {
			arg = apr.Arg(0)
		}

		headRef := dEnv.RepoStateReader().CWBHeadRef()
		ws, err := dEnv.WorkingSet(ctx)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}

		err = actions.ResetHard(ctx, dEnv, arg, roots, headRef, ws)
	} else {
		// Check whether the input argument is a ref.
		if apr.NArg() == 1 {
			argToCheck := apr.Arg(0)

			ok := actions.ValidateIsRef(ctx, argToCheck, dEnv.DoltDB, dEnv.RepoStateReader())

			// This is a valid ref
			if ok {
				err = actions.ResetSoftToRef(ctx, dEnv.DbData(), apr.Arg(0))
				return handleResetError(err, usage)
			}
		}

		tables := apr.Args

		roots, err = actions.ResetSoft(ctx, dEnv.DbData(), tables, roots)
		if err != nil {
			return handleResetError(err, usage)
		}

		err = dEnv.UpdateRoots(ctx, roots)
		if err != nil {
			return handleResetError(err, usage)
		}

		printNotStaged(ctx, dEnv, roots.Staged)
	}

	return handleResetError(err, usage)
}

var tblDiffTypeToShortLabel = map[diff.TableDiffType]string{
	diff.ModifiedTable: "M",
	diff.RemovedTable:  "D",
	diff.AddedTable:    "N",
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

	removeModified := 0
	for _, td := range notStagedTbls {
		if !td.IsAdd() {
			removeModified++
		}
	}

	if removeModified > 0 {
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
		cli.Println(strings.Join(lines, "\n"))
	}
}

func handleResetError(err error, usage cli.UsagePrinter) int {
	if actions.IsTblNotExist(err) {
		tbls := actions.GetTablesForError(err)

		// In case the ref does not exist.
		bdr := errhand.BuildDError("Invalid Ref or Table:")
		if len(tbls) > 1 {
			bdr = errhand.BuildDError("Invalid Table(s):")
		}

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
