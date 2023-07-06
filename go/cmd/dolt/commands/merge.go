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
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

var mergeDocs = cli.CommandDocumentationContent{
	ShortDesc: "Join two or more development histories together",
	LongDesc: `Incorporates changes from the named commits (since the time their histories diverged from the current branch) into the current branch.

The second syntax ({{.LessThan}}dolt merge --abort{{.GreaterThan}}) can only be run after the merge has resulted in conflicts. dolt merge {{.EmphasisLeft}}--abort{{.EmphasisRight}} will abort the merge process and try to reconstruct the pre-merge state. However, if there were uncommitted changes when the merge started (and especially if those changes were further modified after the merge was started), dolt merge {{.EmphasisLeft}}--abort{{.EmphasisRight}} will in some cases be unable to reconstruct the original (pre-merge) changes. Therefore: 

{{.LessThan}}Warning{{.GreaterThan}}: Running dolt merge with non-trivial uncommitted changes is discouraged: while possible, it may leave you in a state that is hard to back out of in the case of a conflict.
`,

	Synopsis: []string{
		"[--squash] {{.LessThan}}branch{{.GreaterThan}}",
		"--no-ff [-m message] {{.LessThan}}branch{{.GreaterThan}}",
		"--abort",
	},
}

type MergeCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd MergeCmd) Name() string {
	return "merge"
}

// Description returns a description of the command
func (cmd MergeCmd) Description() string {
	return "Merge a branch."
}

func (cmd MergeCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateMergeArgParser()
	return cli.NewCommandDocumentation(mergeDocs, ap)
}

func (cmd MergeCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateMergeArgParser()
}

// EventType returns the type of the event to log
func (cmd MergeCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_MERGE
}

func (cmd MergeCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd MergeCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateMergeArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, mergeDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	ok := validateDoltMergeArgs(apr, usage, cliCtx)
	if ok != 0 {
		return 1
	}

	query, err := constructInterpolatedDoltMergeQuery(apr)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}
	_, _, err = queryist.Query(sqlCtx, query)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}

	// calculate merge stats
	if !apr.Contains(cli.AbortParam) && !apr.Contains(cli.NoCommitFlag) {
		cli.Println("starting merge stats calculation")

		// get diff stats for each table
		diffStats := make(map[string][]diffStatistics)
		mergeStats := make(map[string]*merge.MergeStats)

		diffSummaries, err := getDiffSummariesBetweenRefs(queryist, sqlCtx, "head^1", "head")
		if err != nil {
			cli.Println(err.Error())
			return 1
		}

		//rows, err := GetRowsForSql(queryist, sqlCtx, "show tables")
		/*if err != nil {
			cli.Println(err.Error())
			return 1
		}*/
		for _, summary := range diffSummaries {
			if summary.DiffType == "added" {
				mergeStats[summary.TableName] = &merge.MergeStats{
					Operation: merge.TableAdded,
				}
			} else if summary.DiffType == "dropped" {
				mergeStats[summary.TableName] = &merge.MergeStats{
					Operation: merge.TableRemoved,
				}
			} else if summary.DiffType == "modified" || summary.DiffType == "renamed" {
				mergeStats[summary.TableName] = &merge.MergeStats{
					Operation: merge.TableModified,
				}
				tableStats, err := getTableDiffStats(queryist, sqlCtx, summary.TableName, "head^1", "head")
				if err != nil {
					cli.Println(err.Error())
					return 1
				}
				diffStats[tableStats[0].TableName] = append(diffStats[tableStats[0].TableName], tableStats[0])
			} else {
				mergeStats[summary.TableName] = &merge.MergeStats{
					Operation: merge.TableUnmodified,
				}
			}
		}

		/*diffSummaries, err = getDiffSummariesBetweenRefs(queryist, sqlCtx, "head^2", "head")
		if err != nil {
			cli.Println(err.Error())
			return 1
		}

		for _, summary := range diffSummaries {
			if summary.DiffType == "added" {
				mergeStats[summary.TableName] = &merge.MergeStats{
					Operation: merge.TableAdded,
				}
			} else if summary.DiffType == "dropped" {
				mergeStats[summary.TableName] = &merge.MergeStats{
					Operation: merge.TableRemoved,
				}
			} else if summary.DiffType == "modified" || summary.DiffType == "renamed" {
				mergeStats[summary.TableName] = &merge.MergeStats{
					Operation: merge.TableModified,
				}
				tableStats, err := getTableDiffStats(queryist, sqlCtx, summary.TableName, "head^2", "head")
				if err != nil {
					cli.Println(err.Error())
					return 1
				}
				diffStats[tableStats[0].TableName] = append(diffStats[tableStats[0].TableName], tableStats[0])
			} else {
				mergeStats[summary.TableName] = &merge.MergeStats{
					Operation: merge.TableUnmodified,
				}
			}
		}*/

		mergeStats, err = convertDiffStatsToMergeStats(diffStats, mergeStats, queryist, sqlCtx)
		if err != nil {
			cli.Println(err.Error())
			return 1
		}
		hasConflicts, hasConstraintViolations := printSuccessStats(mergeStats)
		return handleMergeErr(ctx, sqlCtx, queryist, dEnv, nil, hasConflicts, hasConstraintViolations, usage)
	} else if apr.Contains(cli.NoCommitFlag) && apr.Contains(cli.NoFFParam) {
		cli.Println("Automatic merge went well; stopped before committing as requested")
	}

	return 0

	/*var verr errhand.VerboseError
	if apr.Contains(cli.AbortParam) {
		mergeActive, err := isMergeActive(ctx, dEnv)
		if err != nil {
			cli.PrintErrln("fatal:", err.Error())
			return 1
		}

		if !mergeActive {
			cli.PrintErrln("fatal: There is no merge to abort")
			return 1
		}

		verr = abortMerge(ctx, dEnv)
	} else {
		if apr.NArg() != 1 {
			usage()
			return 1
		}

		commitSpecStr := apr.Arg(0)

		t := datas.CommitNowFunc()
		if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
			var err error
			t, err = cli.ParseDate(commitTimeStr)

			if err != nil {
				verr = errhand.BuildDError("error: invalid date").AddCause(err).Build()
				return handleCommitErr(sqlCtx, queryist, verr, usage)
			}
		}

		if verr == nil {
			mergeActive, err := isMergeActive(ctx, dEnv)
			if err != nil {
				cli.PrintErrln(err.Error())
				return 1
			}

			if mergeActive {
				cli.Println("error: Merging is not possible because you have not committed an active merge.")
				cli.Println("hint: add affected tables using 'dolt add <table>' and commit using 'dolt commit -m <msg>'")
				cli.Println("fatal: Exiting because of active merge")
				return 1
			}

			roots, err := dEnv.Roots(ctx)
			if err != nil {
				return handleCommitErr(sqlCtx, queryist, err, usage)
			}

			var name, email string
			if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
				name, email, err = cli.ParseAuthor(authorStr)
			} else {
				name, email, err = env.GetNameAndEmail(dEnv.Config)
			}
			if err != nil {
				return handleCommitErr(sqlCtx, queryist, err, usage)
			}

			headRef, err := dEnv.RepoStateReader().CWBHeadRef()
			if err != nil {
				return handleCommitErr(sqlCtx, queryist, err, usage)
			}

			suggestedMsg := fmt.Sprintf("Merge branch '%s' into %s", commitSpecStr, headRef.GetPath())
			msg := ""
			if m, ok := apr.GetValue(cli.MessageArg); ok {
				msg = m
			}

			if apr.Contains(cli.NoCommitFlag) && apr.Contains(cli.CommitFlag) {
				return HandleVErrAndExitCode(errhand.BuildDError("cannot define both 'commit' and 'no-commit' flags at the same time").Build(), usage)
			}
			spec, err := merge.NewMergeSpec(ctx, dEnv.RepoStateReader(), dEnv.DoltDB, roots, name, email, msg, commitSpecStr, apr.Contains(cli.SquashParam), apr.Contains(cli.NoFFParam), apr.Contains(cli.ForceFlag), apr.Contains(cli.NoCommitFlag), apr.Contains(cli.NoEditFlag), t)
			if err != nil {
				return handleCommitErr(sqlCtx, queryist, errhand.VerboseErrorFromError(err), usage)
			}
			if spec == nil {
				cli.Println("Everything up-to-date")
				return handleCommitErr(sqlCtx, queryist, nil, usage)
			}

			err = validateMergeSpec(ctx, spec)
			if err != nil {
				return handleCommitErr(sqlCtx, queryist, err, usage)
			}

			tblToStats, mergeErr := performMerge(ctx, sqlCtx, queryist, dEnv, spec, suggestedMsg, cliCtx)
			hasConflicts, hasConstraintViolations := printSuccessStats(tblToStats)
			return handleMergeErr(ctx, sqlCtx, queryist, dEnv, mergeErr, hasConflicts, hasConstraintViolations, usage)
		}
	}

	return handleCommitErr(sqlCtx, queryist, verr, usage)*/
}

func validateDoltMergeArgs(apr *argparser.ArgParseResults, usage cli.UsagePrinter, cliCtx cli.CliContext) int {
	if apr.ContainsAll(cli.SquashParam, cli.NoFFParam) {
		cli.PrintErrf("error: Flags '--%s' and '--%s' cannot be used together.\n", cli.SquashParam, cli.NoFFParam)
		return 1
	}

	// This command may create a commit, so we need user identity
	if !cli.CheckUserNameAndEmail(cliCtx.Config()) {
		bdr := errhand.BuildDError("Could not determine name and/or email.")
		bdr.AddDetails("Log into DoltHub: dolt login")
		bdr.AddDetails("OR add name to config: dolt config [--global|--local] --add %[1]s \"FIRST LAST\"", env.UserNameKey)
		bdr.AddDetails("OR add email to config: dolt config [--global|--local] --add %[1]s \"EMAIL_ADDRESS\"", env.UserEmailKey)

		return HandleVErrAndExitCode(bdr.Build(), usage)
	}

	if apr.Contains(cli.SquashParam) {
		if apr.NArg() != 1 {
			usage()
			return 1
		}
	} else if apr.Contains(cli.NoFFParam) {
		if apr.NArg() == 0 || apr.NArg() > 2 {
			usage()
			return 1
		}
	} else if apr.Contains(cli.AbortParam) {
		if apr.NArg() != 0 {
			usage()
			return 1
		}
	}

	if apr.ContainsAll(cli.CommitFlag, cli.NoCommitFlag) {
		return HandleVErrAndExitCode(errhand.BuildDError("cannot define both 'commit' and 'no-commit' flags at the same time").Build(), usage)
	}
	if !apr.Contains(cli.AbortParam) && apr.NArg() == 0 {
		usage()
		return 1
	}

	return 0
}

// constructInterpolatedDoltMergeQuery generates the sql query necessary to call the DOLT_MERGE() stored procedure.
// Also interpolates this query to prevent sql injection.
func constructInterpolatedDoltMergeQuery(apr *argparser.ArgParseResults) (string, error) {
	var params []interface{}
	var param bool

	var buffer bytes.Buffer
	var first bool
	first = true
	buffer.WriteString("CALL DOLT_MERGE(")

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

	if apr.Contains(cli.SquashParam) {
		writeToBuffer("--squash")
		param = true
		writeToBuffer("?")
		params = append(params, apr.Arg(0))
	} else if apr.Contains(cli.NoFFParam) {
		writeToBuffer("--no-ff")
	} else if apr.Contains(cli.AbortParam) {
		writeToBuffer("--abort")
	}

	if apr.Contains(cli.CommitFlag) {
		writeToBuffer("--commit")
	}
	if apr.Contains(cli.NoCommitFlag) {
		writeToBuffer("--no-commit")
	}
	if apr.Contains(cli.NoEditFlag) {
		writeToBuffer("--no-edit")
	}
	if apr.Contains(cli.AuthorParam) {
		writeToBuffer("--author")
		param = true
		writeToBuffer("?")
		author, ok := apr.GetValue(cli.AuthorParam)
		if !ok {
			return "", errors.New("Could not retrieve author")
		}
		params = append(params, author)
	}
	if apr.Contains(cli.DateParam) {
		writeToBuffer("--date")
		param = true
		writeToBuffer("?")
		date, ok := apr.GetValue(cli.DateParam)
		if !ok {
			return "", errors.New("Could not retrieve date")
		}
		params = append(params, date)
	}
	if apr.Contains(cli.MessageArg) {
		writeToBuffer("-m")
		param = true
		writeToBuffer("?")
		msg, ok := apr.GetValue(cli.MessageArg)
		if !ok {
			return "", errors.New("Could not retrieve message")
		}
		params = append(params, msg)
	}

	if !apr.Contains(cli.AbortParam) {
		param = true
		writeToBuffer("?")
		params = append(params, apr.Arg(0))
	}

	buffer.WriteString(")")

	interpolatedQuery, err := dbr.InterpolateForDialect(buffer.String(), params, dialect.MySQL)
	if err != nil {
		return "", err
	}

	return interpolatedQuery, nil
}

func isMergeActive(ctx context.Context, denv *env.DoltEnv) (bool, error) {
	ws, err := denv.WorkingSet(ctx)
	if err != nil {
		return false, err
	}
	return ws.MergeActive(), nil
}

func getUnmergedTableCount(ctx context.Context, ws *doltdb.WorkingSet) (int, error) {
	unmerged := set.NewStrSet(nil)
	if ws.MergeState() != nil {
		unmerged.Add(ws.MergeState().TablesWithSchemaConflicts()...)
	}

	conflicted, err := ws.WorkingRoot().TablesWithDataConflicts(ctx)
	if err != nil {
		return 0, err
	}
	unmerged.Add(conflicted...)

	cved, err := ws.WorkingRoot().TablesWithConstraintViolations(ctx)
	if err != nil {
		return 0, err
	}
	unmerged.Add(cved...)

	return unmerged.Size(), nil
}

func validateMergeSpec(ctx context.Context, spec *merge.MergeSpec) errhand.VerboseError {
	if spec.HeadH == spec.MergeH {
		//TODO - why is this different for merge/pull?
		// cli.Println("Already up to date.")
		cli.Println("Everything up-to-date.")
		return nil

	}
	cli.Println("Updating", spec.HeadH.String()+".."+spec.MergeH.String())

	if spec.Squash {
		cli.Println("Squash commit -- not updating HEAD")
	}
	if len(spec.StompedTblNames) != 0 {
		bldr := errhand.BuildDError("error: Your local changes to the following tables would be overwritten by merge:")
		for _, tName := range spec.StompedTblNames {
			bldr.AddDetails(tName)
		}
		bldr.AddDetails("Please commit your changes before you merge.")
		return bldr.Build()
	}

	if ok, err := spec.HeadC.CanFastForwardTo(ctx, spec.MergeC); ok {
		ancRoot, err := spec.HeadC.GetRootValue(ctx)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		mergedRoot, err := spec.MergeC.GetRootValue(ctx)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		if _, err := merge.MayHaveConstraintViolations(ctx, ancRoot, mergedRoot); err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		if !spec.Noff {
			cli.Println("Fast-forward")
		}
	} else if err == doltdb.ErrUpToDate || err == doltdb.ErrIsAhead {
		cli.Println("Already up to date.")
	}
	return nil
}

func abortMerge(ctx context.Context, doltEnv *env.DoltEnv) errhand.VerboseError {
	roots, err := doltEnv.Roots(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	roots, err = actions.CheckoutAllTables(ctx, roots)
	if err == nil {
		err = doltEnv.AbortMerge(ctx)

		if err == nil {
			return nil
		}
	}

	err = doltEnv.UpdateWorkingRoot(ctx, roots.Working)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	return errhand.BuildDError("fatal: failed to revert changes").AddCause(err).Build()
}

// convertDiffStatsToMergeStats converts the given slice of diff stats to merge stats.
func convertDiffStatsToMergeStats(diffStats map[string][]diffStatistics, mergeStats map[string]*merge.MergeStats, queryist cli.Queryist, sqlCtx *sql.Context) (map[string]*merge.MergeStats, error) {
	// merge stats for all parents
	for tableName, allStats := range diffStats {
		//mergeStats[tableName] = &merge.MergeStats{}
		for _, stat := range allStats {
			/*if i == 0 {
				if stat.OldRowCount == 0 {
					mergeStats[tableName].Operation = merge.TableAdded
				} else if stat.NewRowCount == 0 {
					mergeStats[tableName].Operation = merge.TableRemoved
				} else if stat.RowsModified > 0 {
					mergeStats[tableName].Operation = merge.TableModified
				} else {
					mergeStats[tableName].Operation = merge.TableUnmodified
				}
				cli.Println(tableName, ": ", mergeStats[tableName].Operation)
			}*/

			mergeStats[tableName].Adds = mergeStats[tableName].Adds + int(stat.RowsAdded)
			mergeStats[tableName].Deletes = mergeStats[tableName].Deletes + int(stat.RowsDeleted)
			mergeStats[tableName].Modifications = mergeStats[tableName].Modifications + int(stat.RowsModified)
		}
	}

	conflicts, err := GetRowsForSql(queryist, sqlCtx, "SELECT * FROM dolt_conflicts")
	if err != nil {
		return nil, err
	}

	for _, conflict := range conflicts {
		tableName := conflict[0].(string)
		dataConflicts, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("select COUNT(*) from dolt_conflicts_%s", tableName))
		if err != nil {
			return nil, err
		}
		if dataConflicts[0][0].(int) > 0 {
			mergeStats[tableName].DataConflicts = dataConflicts[0][0].(int)
		}

		schemaConflicts, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("select COUNT(*) from dolt_schema_conflicts where table_name = %s", tableName))
		if err != nil {
			return nil, err
		}
		if schemaConflicts[0][0].(int) > 0 {
			mergeStats[tableName].SchemaConflicts = schemaConflicts[0][0].(int)
		}

		constraintViolations, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("select COUNT(*) from dolt_constraint_violations_%s", tableName))
		if err != nil {
			return nil, err
		}
		if constraintViolations[0][0].(int) > 0 {
			mergeStats[tableName].ConstraintViolations = constraintViolations[0][0].(int)
		}
	}

	return mergeStats, nil
}

// printSuccessStats returns whether there are conflicts or constraint violations.
func printSuccessStats(tblToStats map[string]*merge.MergeStats) (conflicts bool, constraintViolations bool) {
	printModifications(tblToStats)
	printAdditions(tblToStats)
	printDeletions(tblToStats)
	return printConflictsAndViolations(tblToStats)
}

func printAdditions(tblToStats map[string]*merge.MergeStats) {
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableAdded {
			cli.Println(tblName, "added")
		}
	}
}

func printDeletions(tblToStats map[string]*merge.MergeStats) {
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableRemoved {
			cli.Println(tblName, "deleted")
		}
	}
}

func printConflictsAndViolations(tblToStats map[string]*merge.MergeStats) (conflicts bool, constraintViolations bool) {
	hasConflicts := false
	hasConstraintViolations := false
	for tblName, stats := range tblToStats {
		if stats.HasArtifacts() {
			cli.Println("Auto-merging", tblName)
			if stats.HasDataConflicts() {
				cli.Println("CONFLICT (content): Merge conflict in", tblName)
				hasConflicts = true
			}
			if stats.HasSchemaConflicts() {
				cli.Println("CONFLICT (schema): Merge conflict in", tblName)
				hasConflicts = true
			}
			if stats.HasConstraintViolations() {
				cli.Println("CONSTRAINT VIOLATION (content): Merge created constraint violation in", tblName)
				hasConstraintViolations = true
			}
		}
	}

	return hasConflicts, hasConstraintViolations
}

func printModifications(tblToStats map[string]*merge.MergeStats) {
	maxNameLen := 0
	maxModCount := 0
	rowsAdded := 0
	rowsDeleted := 0
	rowsChanged := 0
	var tbls []string
	for tblName, stats := range tblToStats {
		if stats.Operation == merge.TableModified && stats.DataConflicts == 0 && stats.ConstraintViolations == 0 {
			tbls = append(tbls, tblName)
			nameLen := len(tblName)
			modCount := stats.Adds + stats.Modifications + stats.Deletes + stats.DataConflicts

			if nameLen > maxNameLen {
				maxNameLen = nameLen
			}

			if modCount > maxModCount {
				maxModCount = modCount
			}

			rowsAdded += stats.Adds
			rowsChanged += stats.Modifications + stats.DataConflicts
			rowsDeleted += stats.Deletes
		}
	}

	if len(tbls) == 0 {
		return
	}

	sort.Strings(tbls)
	modCountStrLen := len(strconv.FormatInt(int64(maxModCount), 10))
	format := fmt.Sprintf("%%-%ds | %%-%ds %%s", maxNameLen, modCountStrLen)

	for _, tbl := range tbls {
		stats := tblToStats[tbl]
		if stats.Operation == merge.TableModified {
			modCount := stats.Adds + stats.Modifications + stats.Deletes + stats.DataConflicts
			modCountStr := strconv.FormatInt(int64(modCount), 10)
			visualizedChanges := visualizeChangeTypes(stats, maxModCount)

			cli.Println(fmt.Sprintf(format, tbl, modCountStr, visualizedChanges))
		}
	}

	details := fmt.Sprintf("%d tables changed, %d rows added(+), %d rows modified(*), %d rows deleted(-)", len(tbls), rowsAdded, rowsChanged, rowsDeleted)
	cli.Println(details)
}

func visualizeChangeTypes(stats *merge.MergeStats, maxMods int) string {
	const maxVisLen = 30 //can be a bit longer due to min len and rounding

	resultStr := ""
	if stats.Adds > 0 {
		addLen := int(maxVisLen * (float64(stats.Adds) / float64(maxMods)))
		if addLen > stats.Adds {
			addLen = stats.Adds
		}
		addStr := fillStringWithChar('+', addLen)
		resultStr += color.GreenString(addStr)
	}

	if stats.Modifications > 0 {
		modLen := int(maxVisLen * (float64(stats.Modifications) / float64(maxMods)))
		if modLen > stats.Modifications {
			modLen = stats.Modifications
		}
		modStr := fillStringWithChar('*', modLen)
		resultStr += color.YellowString(modStr)
	}

	if stats.Deletes > 0 {
		delLen := int(maxVisLen * (float64(stats.Deletes) / float64(maxMods)))
		if delLen > stats.Deletes {
			delLen = stats.Deletes
		}
		delStr := fillStringWithChar('-', delLen)
		resultStr += color.GreenString(delStr)
	}

	return resultStr
}

func fillStringWithChar(ch rune, strLen int) string {
	if strLen == 0 {
		strLen = 1
	}

	runes := make([]rune, strLen)
	for i := 0; i < strLen; i++ {
		runes[i] = ch
	}

	return string(runes)
}

func handleMergeErr(ctx context.Context, sqlCtx *sql.Context, queryist cli.Queryist, dEnv *env.DoltEnv, mergeErr error, hasConflicts, hasConstraintViolations bool, usage cli.UsagePrinter) int {
	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}
	unmergedCnt, err := getUnmergedTableCount(ctx, ws)
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}
	if hasConflicts && hasConstraintViolations {
		cli.Printf("Automatic merge failed; %d table(s) are unmerged.\n"+
			"Fix conflicts and constraint violations and then commit the result.\n"+
			"Use 'dolt conflicts' to investigate and resolve conflicts.\n", unmergedCnt)
	} else if hasConflicts {
		cli.Printf("Automatic merge failed; %d table(s) are unmerged.\n"+
			"Use 'dolt conflicts' to investigate and resolve conflicts.\n", unmergedCnt)
	} else if hasConstraintViolations {
		cli.Printf("Automatic merge failed; %d table(s) are unmerged.\n"+
			"Fix constraint violations and then commit the result.\n"+
			"Constraint violations for the working set may be viewed using the 'dolt_constraint_violations' system table.\n"+
			"They may be queried and removed per-table using the 'dolt_constraint_violations_TABLENAME' system table.\n", unmergedCnt)
	}

	if mergeErr != nil {
		var verr errhand.VerboseError
		switch mergeErr {
		case doltdb.ErrIsAhead:
			verr = nil
		default:
			verr = errhand.VerboseErrorFromError(mergeErr)
			cli.Println("Unable to stage changes: add and commit to finish merge")
		}
		return handleCommitErr(sqlCtx, queryist, verr, usage)
	}

	return 0
}

// performMerge applies a merge spec, potentially fast-forwarding the current branch HEAD, and returns a MergeStats object.
// If the merge can be applied as a fast-forward merge, no commit is needed.
// If the merge is a fast-forward merge, but --no-ff has been supplied, the ExecNoFFMerge function will call
// commit after merging. If the merge is not fast-forward, the --no-commit flag is not defined, and there are
// no conflicts and/or constraint violations, this function will call commit after merging.
// TODO: forcing a commit with a constraint violation should warn users that subsequent
//
//	FF merges will not surface constraint violations on their own; constraint verify --all
//	is required to reify violations.
func performMerge(ctx context.Context, sqlCtx *sql.Context, queryist cli.Queryist, dEnv *env.DoltEnv, spec *merge.MergeSpec, suggestedMsg string, cliCtx cli.CliContext) (map[string]*merge.MergeStats, error) {
	if ok, err := spec.HeadC.CanFastForwardTo(ctx, spec.MergeC); err != nil && !errors.Is(err, doltdb.ErrUpToDate) {
		return nil, err
	} else if ok {
		if spec.Noff {
			return executeNoFFMergeAndCommit(ctx, sqlCtx, queryist, dEnv, spec, suggestedMsg, cliCtx)
		}
		return nil, merge.ExecuteFFMerge(ctx, dEnv, spec)
	}
	return executeMergeAndCommit(ctx, sqlCtx, queryist, dEnv, spec, suggestedMsg, cliCtx)
}

func executeNoFFMergeAndCommit(ctx context.Context, sqlCtx *sql.Context, queryist cli.Queryist, dEnv *env.DoltEnv, spec *merge.MergeSpec, suggestedMsg string, cliCtx cli.CliContext) (map[string]*merge.MergeStats, error) {
	tblToStats, err := merge.ExecNoFFMerge(ctx, dEnv, spec)
	if err != nil {
		return tblToStats, err
	}

	if spec.NoCommit {
		cli.Println("Automatic merge went well; stopped before committing as requested")
		return tblToStats, nil
	}

	// Reload roots since the above method writes new values to the working set
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return tblToStats, err
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return tblToStats, err
	}

	var mergeParentCommits []*doltdb.Commit
	if ws.MergeActive() {
		mergeParentCommits = []*doltdb.Commit{ws.MergeState().Commit()}
	}

	msg, err := getCommitMsgForMerge(ctx, sqlCtx, queryist, spec.Msg, suggestedMsg, spec.NoEdit, cliCtx)
	if err != nil {
		return tblToStats, err
	}

	pendingCommit, err := actions.GetCommitStaged(ctx, roots, ws, mergeParentCommits, dEnv.DbData().Ddb, actions.CommitStagedProps{
		Message:    msg,
		Date:       spec.Date,
		AllowEmpty: spec.AllowEmpty,
		Force:      spec.Force,
		Name:       spec.Name,
		Email:      spec.Email,
	})

	headRef, err := dEnv.RepoStateReader().CWBHeadRef()
	if err != nil {
		return tblToStats, err
	}

	wsHash, err := ws.HashOf()
	_, err = dEnv.DoltDB.CommitWithWorkingSet(
		ctx,
		headRef,
		ws.Ref(),
		pendingCommit,
		ws.WithStagedRoot(pendingCommit.Roots.Staged).WithWorkingRoot(pendingCommit.Roots.Working).ClearMerge(),
		wsHash,
		dEnv.NewWorkingSetMeta(msg),
		nil,
	)

	if err != nil {
		return tblToStats, fmt.Errorf("%w; failed to commit", err)
	}

	return tblToStats, err
}

func executeMergeAndCommit(ctx context.Context, sqlCtx *sql.Context, queryist cli.Queryist, dEnv *env.DoltEnv, spec *merge.MergeSpec, suggestedMsg string, cliCtx cli.CliContext) (map[string]*merge.MergeStats, error) {
	tblToStats, err := merge.ExecuteMerge(ctx, dEnv, spec)
	if err != nil {
		return tblToStats, err
	}

	if hasConflictOrViolations(tblToStats) {
		return tblToStats, nil
	}

	if spec.NoCommit {
		cli.Println("Automatic merge went well; stopped before committing as requested")
		return tblToStats, nil
	}

	msg, err := getCommitMsgForMerge(ctx, sqlCtx, queryist, spec.Msg, suggestedMsg, spec.NoEdit, cliCtx)
	if err != nil {
		return tblToStats, err
	}

	author := fmt.Sprintf("%s <%s>", spec.Name, spec.Email)

	res, skipped := performCommit(ctx, "commit", []string{"-m", msg, "--author", author}, cliCtx, dEnv)
	if res != 0 || skipped {
		return nil, fmt.Errorf("dolt commit failed after merging")
	}

	return tblToStats, nil
}

// getCommitMsgForMerge returns user defined message if exists; otherwise, get the commit message from editor.
func getCommitMsgForMerge(ctx context.Context, sqlCtx *sql.Context, queryist cli.Queryist, userDefinedMsg, suggestedMsg string, noEdit bool, cliCtx cli.CliContext) (string, error) {
	if userDefinedMsg != "" {
		return userDefinedMsg, nil
	}

	msg, err := getCommitMessageFromEditor(sqlCtx, queryist, suggestedMsg, "", noEdit, cliCtx)
	if err != nil {
		return msg, err
	}

	if msg == "" {
		return msg, fmt.Errorf("error: Empty commit message.\n" +
			"Not committing merge; use 'dolt commit' to complete the merge.")
	}

	return msg, nil
}

// hasConflictOrViolations checks for conflicts or constraint violation regardless of a table being modified
func hasConflictOrViolations(tblToStats map[string]*merge.MergeStats) bool {
	for _, tblStats := range tblToStats {
		if tblStats.HasArtifacts() {
			return true
		}
	}
	return false
}
