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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"
	goisatty "github.com/mattn/go-isatty"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/util/outputpager"
)

var commitDocs = cli.CommandDocumentationContent{
	ShortDesc: "Record changes to the database",
	LongDesc: `
Stores the current contents of the staged tables in a new commit along with a log message from the user describing the changes.

The content to be added can be specified by using dolt add to incrementally \"add\" changes to the staged tables before using the commit command (Note: even modified tables must be \"added\").

The log message can be added with the parameter {{.EmphasisLeft}}-m <msg>{{.EmphasisRight}}.  If the {{.LessThan}}-m{{.GreaterThan}} parameter is not provided an editor will be opened where you can review the commit and provide a log message.

The commit timestamp can be modified using the --date parameter.  Dates can be specified in the formats {{.LessThan}}YYYY-MM-DD{{.GreaterThan}}, {{.LessThan}}YYYY-MM-DDTHH:MM:SS{{.GreaterThan}}, or {{.LessThan}}YYYY-MM-DDTHH:MM:SSZ07:00{{.GreaterThan}} (where {{.LessThan}}07:00{{.GreaterThan}} is the time zone offset)."`,
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

func (cmd CommitCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateCommitArgParser()
	return cli.NewCommandDocumentation(commitDocs, ap)
}

func (cmd CommitCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCommitArgParser()
}

func (cmd CommitCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd CommitCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	res, skipped := performCommit(ctx, commandStr, args, cliCtx, dEnv)
	if res != 0 {
		return res
	}

	if skipped {
		iohelp.WriteLine(cli.CliOut, "Skipping empty commit")
	}

	return 0
}

// performCommit creates a new Dolt commit using the specified |commandStr| and |args|. The response is an integer
// status code indicating success or failure, as well as a boolean that indicates if the commit was skipped
// (e.g. because --skip-empty was specified as an argument).
func performCommit(ctx context.Context, commandStr string, args []string, cliCtx cli.CliContext, temporaryDEnv *env.DoltEnv) (int, bool) {
	ap := cli.CreateCommitArgParser()
	apr, usage, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, commitDocs)
	if terminate {
		return status, false
	}

	if err := cli.VerifyCommitArgs(apr); err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage), false
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.Println(err.Error())
		return 1, false
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	// dolt_commit performs this check as well. This nips the problem in the bud early, but is not required.
	err = branch_control.CheckAccess(sqlCtx, branch_control.Permissions_Write)
	if err != nil {
		cli.Println(err.Error())
		return 1, false
	}

	msg, msgOk := apr.GetValue(cli.MessageArg)
	if !msgOk {
		amendStr := ""
		if apr.Contains(cli.AmendFlag) {
			_, rowIter, _, err := queryist.Query(sqlCtx, "select message from dolt_log() limit 1")
			if err != nil {
				cli.Println(err.Error())
				return 1, false
			}
			row, err := rowIter.Next(sqlCtx)
			if err != nil {
				cli.Println(err.Error())
				return 1, false
			}
			amendStr = row.GetValue(0).(string)
		}
		msg, err = getCommitMessageFromEditor(sqlCtx, queryist, "", amendStr, false, cliCtx)
		if err != nil {
			return handleCommitErr(sqlCtx, queryist, err, usage), false
		}
	}

	// process query through prepared statement to prevent sql injection
	query, params, err := constructParametrizedDoltCommitQuery(msg, apr, cliCtx)
	if err != nil {
		return handleCommitErr(sqlCtx, queryist, err, usage), false
	}
	interpolatedQuery, err := dbr.InterpolateForDialect(query, params, dialect.MySQL)
	if err != nil {
		cli.Println(err.Error())
		return 1, false
	}

	_, rowIter, _, err := queryist.Query(sqlCtx, interpolatedQuery)
	if err != nil {
		return handleCommitErr(sqlCtx, queryist, err, usage), false
	}
	resultRow, err := sql.RowIterToRows(sqlCtx, rowIter)
	if err != nil {
		cli.Println(err.Error())
		return 1, false
	}
	if resultRow == nil {
		return 0, true
	}

	commit, err := getCommitInfo(queryist, sqlCtx, "HEAD")
	if cli.ExecuteWithStdioRestored != nil {
		cli.ExecuteWithStdioRestored(func() {
			pager := outputpager.Start()
			defer pager.Stop()

			PrintCommitInfo(pager, 0, false, false, "auto", commit)
		})
	}

	return 0, false
}

// constructParametrizedDoltCommitQuery generates the sql query necessary to call the DOLT_COMMIT() stored procedure with placeholders
// for arg input. Also returns a list of the inputs in the order in which they appear in the query.
func constructParametrizedDoltCommitQuery(msg string, apr *argparser.ArgParseResults, cliCtx cli.CliContext) (string, []interface{}, error) {
	var params []interface{}
	var param bool

	var buffer bytes.Buffer
	var first bool
	first = true
	buffer.WriteString("CALL DOLT_COMMIT(")

	writeToBuffer := func(s string) {
		if !first {
			buffer.WriteString(", ")
		}
		if !param {
			buffer.WriteString("'")
		}
		buffer.WriteString(s)
		if !param {
			buffer.WriteString("'")
		}
		first = false
		param = false
	}

	if msg != "" {
		writeToBuffer("-m")
		param = true
		writeToBuffer("?")
		params = append(params, msg)
	}

	if apr.Contains(cli.AllowEmptyFlag) {
		writeToBuffer("--allow-empty")
	}

	if apr.Contains(cli.DateParam) {
		writeToBuffer("--date")
		param = true
		writeToBuffer("?")
		date, _ := apr.GetValue(cli.DateParam)
		params = append(params, date)
	}

	if apr.Contains(cli.ForceFlag) {
		writeToBuffer("-f")
	}

	writeToBuffer("--author")
	param = true
	writeToBuffer("?")
	var author string
	if apr.Contains(cli.AuthorParam) {
		author, _ = apr.GetValue(cli.AuthorParam)
	} else {
		name, email, err := env.GetNameAndEmail(cliCtx.Config())
		if err != nil {
			return "", nil, err
		}
		author = name + " <" + email + ">"
	}
	params = append(params, author)

	if apr.Contains(cli.AllFlag) {
		writeToBuffer("-a")
	}

	if apr.Contains(cli.UpperCaseAllFlag) {
		writeToBuffer("-A")
	}

	if apr.Contains(cli.AmendFlag) {
		writeToBuffer("--amend")
	}

	if apr.Contains(cli.SkipEmptyFlag) {
		writeToBuffer("--skip-empty")
	}

	cfgSign := cliCtx.Config().GetStringOrDefault("sqlserver.global.gpgsign", "")
	if apr.Contains(cli.SignFlag) || strings.ToLower(cfgSign) == "true" {
		writeToBuffer("--gpg-sign")

		gpgKey := apr.GetValueOrDefault(cli.SignFlag, "")
		if gpgKey != "" {
			param = true
			writeToBuffer("?")
			params = append(params, gpgKey)
		}
	}

	buffer.WriteString(")")
	return buffer.String(), params, nil
}

func handleCommitErr(sqlCtx *sql.Context, queryist cli.Queryist, err error, usage cli.UsagePrinter) int {
	if err == nil {
		return 0
	}

	if err == datas.ErrNameNotConfigured {
		bdr := errhand.BuildDError("Could not determine %s.", config.UserNameKey)
		bdr.AddDetails("Log into DoltHub: dolt login")
		bdr.AddDetails("OR add name to config: dolt config [--global|--local] --add %[1]s \"FIRST LAST\"", config.UserNameKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if err == datas.ErrEmailNotConfigured {
		bdr := errhand.BuildDError("Could not determine %s.", config.UserEmailKey)
		bdr.AddDetails("Log into DoltHub: dolt login")
		bdr.AddDetails("OR add email to config: dolt config [--global|--local] --add %[1]s \"EMAIL_ADDRESS\"", config.UserEmailKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if err == datas.ErrEmptyCommitMessage {
		bdr := errhand.BuildDError("Aborting commit due to empty commit message.")
		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if err.Error() == "nothing to commit" {
		_, ri, _, err := queryist.Query(sqlCtx, "select table_name, status from dolt_status where staged = false")
		if err != nil {
			cli.Println(err)
			return 1
		}
		notStagedRows, err := sql.RowIterToRows(sqlCtx, ri)
		if err != nil {
			cli.Println(err)
			return 1
		}
		n, newErr := PrintDiffsNotStaged(sqlCtx, queryist, cli.CliOut, notStagedRows, false, false, 0)
		if newErr != nil {
			bdr := errhand.BuildDError(`No changes added to commit (use "dolt add")\nCould not print diff because of additional error`)
			bdr.AddCause(newErr)
			return HandleVErrAndExitCode(bdr.Build(), usage)
		}

		if n == 0 {
			bdr := errhand.BuildDError(`no changes added to commit (use "dolt add")`)
			return HandleVErrAndExitCode(bdr.Build(), usage)
		}
	}

	if actions.IsTblInConflict(err) {
		inConflict := actions.GetTablesForError(err)
		bdr := errhand.BuildDError(`tables %v have unresolved conflicts from the merge. resolve the conflicts before committing`, inConflict)
		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	verr := errhand.BuildDError("error: Failed to commit changes.").AddCause(err).Build()
	return HandleVErrAndExitCode(verr, usage)
}

// getCommitMessageFromEditor opens editor to ask user for commit message if none defined from command line.
// suggestedMsg will be returned if no-edit flag is defined or if this function was called from sql dolt_merge command.
func getCommitMessageFromEditor(sqlCtx *sql.Context, queryist cli.Queryist, suggestedMsg, amendString string, noEdit bool, cliCtx cli.CliContext) (string, error) {
	var finalMsg string
	initialMsg, err := buildInitalCommitMsg(sqlCtx, queryist, suggestedMsg)
	if err != nil {
		return "", err
	}
	if amendString != "" {
		initialMsg = fmt.Sprintf("%s\n%s", amendString, initialMsg)
	}

	if cli.ExecuteWithStdioRestored == nil || noEdit {
		return suggestedMsg, nil
	}

	if !checkIsTerminal() {
		return suggestedMsg, nil
	}

	commitMsg, err := execEditor(initialMsg, "", cliCtx)
	if err != nil {
		return "", fmt.Errorf("Failed to open commit editor: %v \n Check your `EDITOR` environment variable with `echo $EDITOR` or your dolt config with `dolt config --list` to ensure that your editor is valid", err)
	}

	finalMsg = parseCommitMessage(commitMsg)
	return finalMsg, nil
}

func checkIsTerminal() bool {
	isTerminal := false
	cli.ExecuteWithStdioRestored(func() {
		if goisatty.IsTerminal(os.Stdout.Fd()) || os.Getenv(dconfig.EnvTestForceOpenEditor) == "1" {
			isTerminal = true
		}
	})
	return isTerminal
}

func buildInitalCommitMsg(sqlCtx *sql.Context, queryist cli.Queryist, suggestedMsg string) (string, error) {
	initialNoColor := color.NoColor
	color.NoColor = true

	_, ri, _, err := queryist.Query(sqlCtx, "select table_name, status from dolt_status where staged = true")
	if err != nil {
		return "", err
	}
	stagedRows, err := sql.RowIterToRows(sqlCtx, ri)
	if err != nil {
		return "", err
	}

	_, ri, _, err = queryist.Query(sqlCtx, "select table_name, status from dolt_status where staged = false")
	if err != nil {
		return "", err
	}
	notStagedRows, err := sql.RowIterToRows(sqlCtx, ri)
	if err != nil {
		return "", err
	}

	buf := bytes.NewBuffer([]byte{})
	n := printStagedDiffs(buf, stagedRows, true)
	n, err = PrintDiffsNotStaged(sqlCtx, queryist, buf, notStagedRows, true, false, n)
	if err != nil {
		return "", err
	}

	// get current branch
	currBranch, err := getActiveBranchName(sqlCtx, queryist)
	if err != nil {
		return "", err
	}

	initialCommitMessage := fmt.Sprintf("%s\n# Please enter the commit message for your changes. Lines starting"+
		"\n# with '#' will be ignored, and an empty message aborts the commit."+
		"\n# On branch %s\n#\n", suggestedMsg, currBranch)

	msgLines := strings.Split(buf.String(), "\n")
	for i, msg := range msgLines {
		msgLines[i] = "# " + msg
	}
	statusMsg := strings.Join(msgLines, "\n")

	color.NoColor = initialNoColor
	return initialCommitMessage + statusMsg, nil
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

func PrintDiffsNotStaged(
	sqlCtx *sql.Context,
	queryist cli.Queryist,
	wr io.Writer,
	notStagedRows []sql.Row,
	printHelp bool,
	printIgnored bool,
	linesPrinted int,
) (int, error) {
	// get data conflict tables
	_, ri, _, err := queryist.Query(sqlCtx, "select `table` from dolt_conflicts")
	if err != nil {
		return 0, err
	}
	conflictRows, err := sql.RowIterToRows(sqlCtx, ri)
	if err != nil {
		return 0, err
	}
	var conflictTables []string
	for i, _ := range conflictRows {
		conflictTables = append(conflictTables, conflictRows[i].GetValue(0).(string))
	}
	inCnfSet := set.NewStrSet(conflictTables)

	// get schema conflict tables
	_, ri, _, err = queryist.Query(sqlCtx, "select table_name from dolt_status where status = 'schema conflict'")
	if err != nil {
		return 0, err
	}
	schemaConflictRows, err := sql.RowIterToRows(sqlCtx, ri)
	if err != nil {
		return 0, err
	}
	var schemaConflictTables []string
	for i, _ := range schemaConflictRows {
		schemaConflictTables = append(schemaConflictTables, schemaConflictRows[i].GetValue(0).(string))
	}
	inCnfSet.Add(schemaConflictTables...)

	// get constraint violation tables
	_, ri, _, err = queryist.Query(sqlCtx, "select `table` from dolt_constraint_violations")
	if err != nil {
		return 0, err
	}
	constraintViolationRows, err := sql.RowIterToRows(sqlCtx, ri)
	if err != nil {
		return 0, err
	}
	var constraintViolationTables []string
	for i, _ := range constraintViolationRows {
		constraintViolationTables = append(constraintViolationTables, constraintViolationRows[i].GetValue(0).(string))
	}
	violationSet := set.NewStrSet(constraintViolationTables)

	if len(conflictTables) > 0 || len(constraintViolationTables) > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}
		iohelp.WriteLine(wr, unmergedPathsHeader)
		if printHelp {
			iohelp.WriteLine(wr, mergedTableHelp)
		}

		if len(conflictTables) > 0 || len(schemaConflictTables) > 0 {
			lines := make([]string, 0, len(notStagedRows))
			for _, tblName := range schemaConflictTables {
				lines = append(lines, fmt.Sprintf(statusFmt, schemaConflictLabel, tblName))
			}
			for _, tblName := range conflictTables {
				lines = append(lines, fmt.Sprintf(statusFmt, bothModifiedLabel, tblName))
			}
			iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
			linesPrinted += len(lines)
		}

		if len(constraintViolationTables) > 0 {
			violationOnly, _, _ := violationSet.LeftIntersectionRight(inCnfSet)
			lines := make([]string, 0, len(notStagedRows))
			for _, tblName := range violationOnly.AsSortedSlice() {
				lines = append(lines, fmt.Sprintf(statusFmt, "modified", tblName))
			}
			iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
			linesPrinted += len(lines)
		}
	}

	added := 0
	removeModified := 0
	for _, row := range notStagedRows {
		if row.GetValue(1) == "new table" {
			added++
		} else if row.GetValue(1) == "renamed" {
			added++
			removeModified++
		} else {
			removeModified++
		}
	}

	numRemovedOrModified := removeModified

	if numRemovedOrModified-inCnfSet.Size()-violationSet.Size() > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, workingHeader)

		if printHelp {
			iohelp.WriteLine(wr, workingHeaderHelp)
		}

		lines := getModifiedAndRemovedNotStaged(notStagedRows, inCnfSet, violationSet)

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)
	}

	if added > 0 {
		if linesPrinted > 0 {
			cli.Println()
		}

		iohelp.WriteLine(wr, untrackedHeader)

		if printHelp {
			iohelp.WriteLine(wr, untrackedHeaderHelp)
		}

		addedNotStagedTables := getAddedNotStagedTables(notStagedRows)
		filteredTables, err := filterIgnoredTables(sqlCtx, queryist, addedNotStagedTables)
		if err != nil && doltdb.AsDoltIgnoreInConflict(err) == nil {
			return 0, err
		}

		lines := make([]string, len(filteredTables.DontIgnore))
		for i, tableName := range filteredTables.DontIgnore {
			lines[i] = fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.AddedTable], tableName)
		}

		iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
		linesPrinted += len(lines)

		if printIgnored && len(filteredTables.Ignore) > 0 {
			if linesPrinted > 0 {
				cli.Println()
			}

			iohelp.WriteLine(wr, ignoredHeader)

			if printHelp {
				iohelp.WriteLine(wr, ignoredHeaderHelp)
			}

			lines := make([]string, len(filteredTables.Ignore))
			for i, tableName := range filteredTables.Ignore {
				lines[i] = fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.AddedTable], tableName)
			}

			iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
			linesPrinted += len(lines)
		}

		if len(filteredTables.Conflicts) > 0 {
			if linesPrinted > 0 {
				cli.Println()
			}

			iohelp.WriteLine(wr, conflictedIgnoredHeader)

			if printHelp {
				iohelp.WriteLine(wr, conflictedIgnoredHeaderHelp)
			}

			lines := make([]string, len(filteredTables.Conflicts))
			for i, conflict := range filteredTables.Conflicts {
				lines[i] = fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.AddedTable], conflict.Table)
			}

			iohelp.WriteLine(wr, color.RedString(strings.Join(lines, "\n")))
			linesPrinted += len(lines)
		}
	}

	return linesPrinted, nil
}

func getModifiedAndRemovedNotStaged(notStagedRows []sql.Row, inCnfSet, violationSet *set.StrSet) (lines []string) {
	lines = make([]string, 0, len(notStagedRows))
	for _, row := range notStagedRows {
		if row.GetValue(1) == "added" || inCnfSet.Contains(row.GetValue(0).(string)) || violationSet.Contains(row.GetValue(0).(string)) {
			continue
		}
		if row.GetValue(1) == "deleted" {
			lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.RemovedTable], row.GetValue(0).(string)))
		} else if row.GetValue(1) == "renamed" {
			// per Git, unstaged renames are shown as drop + add
			names := strings.Split(row.GetValue(0).(string), " -> ")
			lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.RemovedTable], names[0]))
		} else {
			lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.ModifiedTable], row.GetValue(0).(string)))
		}
	}
	return lines
}

func getAddedNotStagedTables(notStagedRows []sql.Row) (tables []doltdb.TableName) {
	tables = make([]doltdb.TableName, 0, len(notStagedRows))
	for _, row := range notStagedRows {
		if row.GetValue(1) == "added" || row.GetValue(1) == "renamed" {
			names := strings.Split(row.GetValue(0).(string), " -> ")
			// TODO: schema name
			tables = append(tables, doltdb.TableName{Name: names[0]})
		}
	}
	return tables
}

const (
	branchHeader     = "On branch %s\n"
	stagedHeader     = `Changes to be committed:`
	stagedHeaderHelp = `  (use "dolt reset <table>..." to unstage)`

	unmergedTablesHeader = `You have unmerged tables.
  (fix %s and run "dolt commit")
  (use "dolt merge --abort" to abort the merge)
`

	allMergedHeader = `All conflicts and constraint violations fixed but you are still merging.
  (use "dolt commit" to conclude merge)`

	unmergedPathsHeader = `Unmerged paths:`
	mergedTableHelp     = `  (use "dolt add <table>..." to mark resolution)`

	workingHeader     = `Changes not staged for commit:`
	workingHeaderHelp = `  (use "dolt add <table>" to update what will be committed)
  (use "dolt checkout <table>" to discard changes in working directory)`

	untrackedHeader     = `Untracked tables:`
	untrackedHeaderHelp = `  (use "dolt add <table>" to include in what will be committed)`

	ignoredHeader     = `Ignored tables:`
	ignoredHeaderHelp = `  (use "dolt add -f <table>" to include in what will be committed)`

	conflictedIgnoredHeader     = `Tables with conflicting dolt_ignore patterns:`
	conflictedIgnoredHeaderHelp = `  (use "dolt add -f <table>" to include in what will be committed)`

	statusFmt           = "\t%-18s%s"
	statusRenameFmt     = "\t%-18s%s -> %s"
	schemaConflictLabel = "schema conflict:"
	bothModifiedLabel   = "both modified:"
)

var tblDiffTypeToLabel = map[diff.TableDiffType]string{
	diff.ModifiedTable: "modified:",
	diff.RenamedTable:  "renamed:",
	diff.RemovedTable:  "deleted:",
	diff.AddedTable:    "new table:",
}

func printStagedDiffs(wr io.Writer, stagedRows []sql.Row, printHelp bool) int {
	if len(stagedRows) > 0 {
		iohelp.WriteLine(wr, stagedHeader)

		if printHelp {
			iohelp.WriteLine(wr, stagedHeaderHelp)
		}

		lines := make([]string, 0, len(stagedRows))
		for _, row := range stagedRows {
			if !doltdb.IsReadOnlySystemTable(doltdb.TableName{Name: row.GetValue(0).(string)}) {
				switch row.GetValue(1).(string) {
				case "new table":
					lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.AddedTable], row.GetValue(0).(string)))
				case "deleted":
					lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.RemovedTable], row.GetValue(0).(string)))
				case "renamed":
					names := strings.Split(row.GetValue(0).(string), " -> ")
					lines = append(lines, fmt.Sprintf(statusRenameFmt, tblDiffTypeToLabel[diff.RenamedTable], names[0], names[1]))
				default:
					lines = append(lines, fmt.Sprintf(statusFmt, tblDiffTypeToLabel[diff.ModifiedTable], row.GetValue(0).(string)))
				}
			}
		}
		iohelp.WriteLine(wr, color.GreenString(strings.Join(lines, "\n")))
		return len(stagedRows)
	}

	return 0
}

// filterIgnoredTables takes a slice of table names and divides it into new slices based on whether the table is ignored, not ignored, or matches conflicting ignore patterns.
func filterIgnoredTables(sqlCtx *sql.Context, queryist cli.Queryist, addedNotStagedTables []doltdb.TableName) (ignoredTables doltdb.IgnoredTables, err error) {
	ignorePatterns, err := getIgnoredTablePatternsFromSql(queryist, sqlCtx)
	if err != nil {
		return ignoredTables, err
	}
	for _, tableName := range addedNotStagedTables {
		ignored, err := ignorePatterns.IsTableNameIgnored(tableName)
		if conflict := doltdb.AsDoltIgnoreInConflict(err); conflict != nil {
			ignoredTables.Conflicts = append(ignoredTables.Conflicts, *conflict)
		} else if err != nil {
			return ignoredTables, err
		} else if ignored == doltdb.DontIgnore {
			ignoredTables.DontIgnore = append(ignoredTables.DontIgnore, tableName)
		} else if ignored == doltdb.Ignore {
			ignoredTables.Ignore = append(ignoredTables.Ignore, tableName)
		} else {
			return ignoredTables, fmt.Errorf("IsTableNameIgnored returned ErrorOccurred but no error!")
		}
	}

	return ignoredTables, nil
}
