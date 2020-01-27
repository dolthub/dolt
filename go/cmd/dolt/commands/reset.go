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
	"context"
	"fmt"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

const (
	SoftResetParam = "soft"
	HardResetParam = "hard"
)

var resetShortDesc = "Resets staged tables to their HEAD state"
var resetLongDesc = `Sets the state of a table in the staging area to be that table's value at HEAD

dolt reset <tables>...
	This form resets the values for all staged <tables> to their values at HEAD. (It does not affect the working tree or
	the current branch.)

	This means that </b>dolt reset <tables></b> is the opposite of <b>dolt add <tables></b>.

	After running <b>dolt reset <tables></b> to update the staged tables, you can use <b>dolt checkout</b> to check the
	contents out of the staged tables to the working tables.

dolt reset .
	This form resets <b>all</b> staged tables to their values at HEAD. It is the opposite of <b>dolt add .</b>`

var resetSynopsis = []string{
	"<tables>...",
	"[--hard | --soft]",
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
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, resetShortDesc, resetLongDesc, resetSynopsis, ap)
}

func (cmd ResetCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(HardResetParam, "", "Resets the working tables and staged tables. Any changes to tracked tables in the working tree since <commit> are discarded.")
	ap.SupportsFlag(SoftResetParam, "", "Does not touch the working tables, but removes all tables staged to be committed.")
	return ap
}

// Exec executes the command
func (cmd ResetCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, resetShortDesc, resetLongDesc, resetSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.ContainsArg(doltdb.DocTableName) {
		return HandleDocTableVErrAndExitCode()
	}

	workingRoot, stagedRoot, headRoot, verr := getAllRoots(ctx, dEnv)

	if verr == nil {
		if apr.ContainsAll(HardResetParam, SoftResetParam) {
			verr = errhand.BuildDError("error: --%s and --%s are mutually exclusive options.", HardResetParam, SoftResetParam).Build()
		} else if apr.Contains(HardResetParam) {
			verr = resetHard(ctx, dEnv, apr, workingRoot, headRoot)
		} else {
			verr = resetSoft(ctx, dEnv, apr, stagedRoot, headRoot)
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func resetHard(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, workingRoot, headRoot *doltdb.RootValue) errhand.VerboseError {
	if apr.NArg() != 0 {
		return errhand.BuildDError("--%s does not support additional params", HardResetParam).SetPrintUsage().Build()
	}

	// need to save the state of files that aren't tracked
	untrackedTables := make(map[string]*doltdb.Table)
	wTblNames, err := workingRoot.GetTableNames(ctx)

	if err != nil {
		return errhand.BuildDError("error: failed to read tables from the working set").AddCause(err).Build()
	}

	for _, tblName := range wTblNames {
		untrackedTables[tblName], _, err = workingRoot.GetTable(ctx, tblName)

		if err != nil {
			return errhand.BuildDError("error: failed to read '%s' from the working set", tblName).AddCause(err).Build()
		}
	}

	headTblNames, err := headRoot.GetTableNames(ctx)

	if err != nil {
		return errhand.BuildDError("error: failed to read tables from head").AddCause(err).Build()
	}

	for _, tblName := range headTblNames {
		delete(untrackedTables, tblName)
	}

	newWkRoot := headRoot
	for tblName, tbl := range untrackedTables {
		if tblName != doltdb.DocTableName {
			newWkRoot, err = newWkRoot.PutTable(ctx, tblName, tbl)
		}
		if err != nil {
			return errhand.BuildDError("error: failed to write table back to database").Build()
		}
	}

	// TODO: update working and staged in one repo_state write.
	err = dEnv.UpdateWorkingRoot(ctx, newWkRoot)

	if err != nil {
		return errhand.BuildDError("error: failed to update the working tables.").AddCause(err).Build()
	}

	_, err = dEnv.UpdateStagedRoot(ctx, headRoot)

	if err != nil {
		return errhand.BuildDError("error: failed to update the staged tables.").AddCause(err).Build()
	}

	err = actions.SaveTrackedDocsFromWorking(ctx, dEnv)
	if err != nil {
		return errhand.BuildDError("error: failed to update docs on the filesystem.").AddCause(err).Build()
	}

	return nil
}

// RemoveDocsTbl takes a slice of table names and returns a new slice with DocTableName removed.
func RemoveDocsTbl(tbls []string) []string {
	var result []string
	for _, tblName := range tbls {
		if tblName != doltdb.DocTableName {
			result = append(result, tblName)
		}
	}
	return result
}

func resetSoft(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults, stagedRoot, headRoot *doltdb.RootValue) errhand.VerboseError {
	tbls := apr.Args()

	if len(tbls) == 0 || (len(tbls) == 1 && tbls[0] == ".") {
		var err error
		tbls, err = actions.AllTables(ctx, stagedRoot, headRoot)

		if err != nil {
			return errhand.BuildDError("error: failed to get all tables").AddCause(err).Build()
		}
	}

	tables, docs, err := actions.GetTblsAndDocDetails(dEnv, tbls)
	if err != nil {
		return errhand.BuildDError("error: failed to get all tables").AddCause(err).Build()
	}

	if len(docs) > 0 {
		tables = RemoveDocsTbl(tables)
	}

	verr := ValidateTablesWithVErr(tables, stagedRoot, headRoot)

	if verr != nil {
		return verr
	}

	stagedRoot, err = resetDocs(ctx, dEnv, headRoot, docs)
	if err != nil {
		return errhand.BuildDError("error: failed to reset docs").AddCause(err).Build()
	}

	stagedRoot, verr = resetStaged(ctx, dEnv, tables, stagedRoot, headRoot)

	if verr != nil {
		return verr
	}

	printNotStaged(ctx, dEnv, stagedRoot)
	return nil
}

func resetDocs(ctx context.Context, dEnv *env.DoltEnv, headRoot *doltdb.RootValue, docDetails env.Docs) (newStgRoot *doltdb.RootValue, err error) {
	docs, err := dEnv.GetDocsWithNewerTextFromRoot(ctx, headRoot, docDetails)
	if err != nil {
		return nil, err
	}

	err = dEnv.PutDocsToWorking(ctx, docs)
	if err != nil {
		return nil, err
	}

	return dEnv.PutDocsToStaged(ctx, docs)
}

func printNotStaged(ctx context.Context, dEnv *env.DoltEnv, staged *doltdb.RootValue) {
	// Printing here is best effort.  Fail silently
	working, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return
	}

	notStagedTbls, err := actions.NewTableDiffs(ctx, working, staged)
	if err != nil {
		return
	}

	notStagedDocs, err := actions.NewDocDiffs(ctx, dEnv, working, nil, nil)
	if err != nil {
		return
	}

	if notStagedTbls.NumRemoved+notStagedTbls.NumModified+notStagedDocs.NumRemoved+notStagedDocs.NumModified > 0 {
		cli.Println("Unstaged changes after reset:")

		lines := make([]string, 0, notStagedTbls.Len()+notStagedDocs.Len())
		for _, tblName := range notStagedTbls.Tables {
			tdt := notStagedTbls.TableToType[tblName]

			if tdt != actions.AddedTable && !sqle.HasDoltPrefix(tblName) {
				lines = append(lines, fmt.Sprintf("%s\t%s", tblDiffTypeToShortLabel[tdt], tblName))
			}
		}

		for _, docName := range notStagedDocs.Docs {
			ddt := notStagedDocs.DocToType[docName]
			if ddt != actions.AddedDoc {
				lines = append(lines, fmt.Sprintf("%s\t%s", docDiffTypeToShortLabel[ddt], docName))
			}
		}

		cli.Println(strings.Join(lines, "\n"))
	}
}

func resetStaged(ctx context.Context, dEnv *env.DoltEnv, tbls []string, staged, head *doltdb.RootValue) (*doltdb.RootValue, errhand.VerboseError) {
	updatedRoot, err := staged.UpdateTablesFromOther(ctx, tbls, head)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to update tables").AddCause(err).Build()
	}

	return updatedRoot, UpdateStagedWithVErr(dEnv, updatedRoot)
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
