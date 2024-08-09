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
	"sort"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/util/outputpager"
)

var logDocs = cli.CommandDocumentationContent{
	ShortDesc: `Show commit logs`,
	LongDesc: `Shows the commit logs

The command takes options to control what is shown and how. 

{{.EmphasisLeft}}dolt log{{.EmphasisRight}}
  Lists commit logs from current HEAD when no options provided.
	
{{.EmphasisLeft}}dolt log [<revisions>...]{{.EmphasisRight}}
  Lists commit logs starting from revision. If multiple revisions provided, lists logs reachable by all revisions.
	
{{.EmphasisLeft}}dolt log [<revisions>...] -- <table>{{.EmphasisRight}}
  Lists commit logs starting from revisions, only including commits with changes to table.
	
{{.EmphasisLeft}}dolt log <revisionB>..<revisionA>{{.EmphasisRight}}
{{.EmphasisLeft}}dolt log <revisionA> --not <revisionB>{{.EmphasisRight}}
{{.EmphasisLeft}}dolt log ^<revisionB> <revisionA>{{.EmphasisRight}}
  Different ways to list two dot logs. These will list commit logs for revisionA, while excluding commits from revisionB. The table option is not supported for two dot log.
	
{{.EmphasisLeft}}dolt log <revisionB>...<revisionA>{{.EmphasisRight}}
{{.EmphasisLeft}}dolt log <revisionA> <revisionB> --not $(dolt merge-base <revisionA> <revisionB>){{.EmphasisRight}}
  Different ways to list three dot logs. These will list commit logs reachable by revisionA OR revisionB, while excluding commits reachable by BOTH revisionA AND revisionB.`,
	Synopsis: []string{
		`[-n {{.LessThan}}num_commits{{.GreaterThan}}] [{{.LessThan}}revision-range{{.GreaterThan}}] [[--] {{.LessThan}}table{{.GreaterThan}}]`,
	},
}

type LogCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd LogCmd) Name() string {
	return "log"
}

// Description returns a description of the command
func (cmd LogCmd) Description() string {
	return "Show commit logs."
}

// EventType returns the type of the event to log
func (cmd LogCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_LOG
}

func (cmd LogCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(logDocs, ap)
}

func (cmd LogCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateLogArgParser(false)
}

func (cmd LogCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd LogCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	return cmd.logWithLoggerFunc(ctx, commandStr, args, dEnv, cliCtx)
}

func (cmd LogCmd) logWithLoggerFunc(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	apr, _, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, logDocs)
	if terminate {
		return status
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return handleErrAndExit(err)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	query, err := constructInterpolatedDoltLogQuery(apr, queryist, sqlCtx)
	if err != nil {
		return handleErrAndExit(err)
	}
	logRows, err := GetRowsForSql(queryist, sqlCtx, query)
	if err != nil {
		return handleErrAndExit(err)
	}

	return handleErrAndExit(logCommits(apr, logRows, queryist, sqlCtx))
}

// constructInterpolatedDoltLogQuery generates the sql query necessary to call the DOLT_LOG() function.
// Also interpolates this query to prevent sql injection.
func constructInterpolatedDoltLogQuery(apr *argparser.ArgParseResults, queryist cli.Queryist, sqlCtx *sql.Context) (string, error) {
	var params []interface{}

	var buffer bytes.Buffer
	var first bool
	first = true

	buffer.WriteString("select commit_hash from dolt_log(")

	writeToBuffer := func(s string) {
		if !first {
			buffer.WriteString(", ")
		}
		buffer.WriteString(s)
		first = false
	}

	if apr.PositionalArgsSeparatorIndex >= 0 {
		for i := 0; i < apr.PositionalArgsSeparatorIndex; i++ {
			writeToBuffer("?")
			params = append(params, apr.Arg(i))
		}
		var tableNames []string
		for i := apr.PositionalArgsSeparatorIndex; i < apr.NArg(); i++ {
			tableNames = append(tableNames, apr.Arg(i))
		}
		if len(tableNames) > 0 {
			params = append(params, strings.Join(tableNames, ","))
			writeToBuffer("'--tables'")
			writeToBuffer("?")
		}
	} else {
		var existingTables map[string]bool
		seenRevs := make(map[string]bool, apr.NArg())
		finishedRevs := false
		var tableNames []string
		for i, arg := range apr.Args {
			// once we encounter a rev we can't resolve, we assume the rest are table names
			if finishedRevs {
				if _, ok := existingTables[arg]; !ok {
					return "", fmt.Errorf("error: table %s does not exist", arg)
				}
				tableNames = append(tableNames, arg)
			} else {
				if strings.Contains(arg, "..") || strings.HasPrefix(arg, "^") || strings.HasPrefix(arg, "refs/") || strings.HasPrefix(arg, "remotes/") {
					writeToBuffer("?")
					params = append(params, arg)
				} else {
					_, err := GetRowsForSql(queryist, sqlCtx, "select hashof('"+arg+"')")
					if err != nil {
						finishedRevs = true
						existingTables, err = getExistingTables(apr.Args[:i], queryist, sqlCtx)
						if err != nil {
							return "", err
						}

						if _, ok := existingTables[arg]; !ok {
							return "", fmt.Errorf("error: table %s does not exist", arg)
						}
						tableNames = append(tableNames, arg)
					} else {
						if _, ok := seenRevs[arg]; ok {
							finishedRevs = true
							existingTables, err = getExistingTables(apr.Args[:i], queryist, sqlCtx)
							if err != nil {
								return "", err
							}

							if _, ok := existingTables[arg]; !ok {
								return "", fmt.Errorf("error: table %s does not exist", arg)
							}
							tableNames = append(tableNames, arg)
						} else {
							seenRevs[arg] = true
						}
						writeToBuffer("?")
						params = append(params, arg)
					}
				}
			}

		}
		if len(tableNames) > 0 {
			params = append(params, strings.Join(tableNames, ","))
			writeToBuffer("'--tables'")
			writeToBuffer("?")
		}
	}

	if minParents, hasMinParents := apr.GetValue(cli.MinParentsFlag); hasMinParents {
		writeToBuffer("?")
		params = append(params, "--min-parents="+minParents)
	}

	if hasMerges := apr.Contains(cli.MergesFlag); hasMerges {
		writeToBuffer("'--merges'")
	}

	if excludedCommits, hasExcludedCommits := apr.GetValueList(cli.NotFlag); hasExcludedCommits {
		writeToBuffer("'--not'")
		for _, commit := range excludedCommits {
			writeToBuffer("?")
			params = append(params, commit)
		}
	}

	// included to check for invalid --decorate options
	if decorate, hasDecorate := apr.GetValue(cli.DecorateFlag); hasDecorate {
		writeToBuffer("?")
		params = append(params, "--decorate="+decorate)
	}

	buffer.WriteString(")")

	if numLines, hasNumLines := apr.GetValue(cli.NumberFlag); hasNumLines {
		num, err := strconv.Atoi(numLines)
		if err != nil || num < 0 {
			return "", fmt.Errorf("fatal: invalid --number argument: %s", numLines)
		}
		buffer.WriteString(" limit " + numLines)
	}

	interpolatedQuery, err := dbr.InterpolateForDialect(buffer.String(), params, dialect.MySQL)
	if err != nil {
		return "", err
	}

	return interpolatedQuery, nil
}

// getExistingTables returns a map of table names that exist in the commit history of the given revisions
func getExistingTables(revisions []string, queryist cli.Queryist, sqlCtx *sql.Context) (map[string]bool, error) {
	tableNames := make(map[string]bool)

	if len(revisions) == 0 {
		revisions = []string{"HEAD"}
	}

	for _, rev := range revisions {
		rows, err := GetRowsForSql(queryist, sqlCtx, "show tables as of '"+rev+"'")
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			tableNames[r[0].(string)] = true
		}
	}

	return tableNames, nil
}

// logCommits takes a list of sql rows that have only 1 column, commit hash, and retrieves the commit info for each hash to be printed to std out
func logCommits(apr *argparser.ArgParseResults, commitHashes []sql.Row, queryist cli.Queryist, sqlCtx *sql.Context) error {
	var commitsInfo []CommitInfo
	for _, hash := range commitHashes {
		cmHash := hash[0].(string)
		commit, err := getCommitInfo(queryist, sqlCtx, cmHash)
		if commit == nil {
			return fmt.Errorf("no commits found for ref %s", cmHash)
		}
		if err != nil {
			return err
		}
		commitsInfo = append(commitsInfo, *commit)
	}

	return logToStdOut(apr, commitsInfo, sqlCtx, queryist)
}

func logCompact(pager *outputpager.Pager, apr *argparser.ArgParseResults, commits []CommitInfo, sqlCtx *sql.Context, queryist cli.Queryist) error {
	color.NoColor = false
	for _, comm := range commits {
		if len(comm.parentHashes) < apr.GetIntOrDefault(cli.MinParentsFlag, 0) {
			return nil
		}

		chStr := comm.commitHash
		if apr.Contains(cli.ParentsFlag) {
			for _, h := range comm.parentHashes {
				chStr += " " + h
			}
		}

		// TODO: use short hash instead
		// Write commit hash
		pager.Writer.Write([]byte(color.YellowString("%s ", chStr)))

		if decoration := apr.GetValueOrDefault(cli.DecorateFlag, "auto"); decoration != "no" {
			printRefs(pager, &comm, decoration)
		}

		formattedDesc := strings.Replace(comm.commitMeta.Description, "\n", " ", -1) + "\n"
		pager.Writer.Write([]byte(formattedDesc))

		if apr.Contains(cli.StatFlag) {
			if comm.parentHashes != nil && len(comm.parentHashes) == 1 { // don't print stats for merge commits
				diffStats := make(map[string]*merge.MergeStats)
				diffStats, _, err := calculateMergeStats(queryist, sqlCtx, diffStats, comm.parentHashes[0], comm.commitHash)
				if err != nil {
					return err
				}
				printDiffStats(diffStats, pager)
			}
		}
	}

	return nil
}

func logDefault(pager *outputpager.Pager, apr *argparser.ArgParseResults, commits []CommitInfo, sqlCtx *sql.Context, queryist cli.Queryist) error {
	for _, comm := range commits {
		PrintCommitInfo(pager, apr.GetIntOrDefault(cli.MinParentsFlag, 0), apr.Contains(cli.ParentsFlag), apr.GetValueOrDefault(cli.DecorateFlag, "auto"), &comm)
		if apr.Contains(cli.StatFlag) {
			if comm.parentHashes != nil && len(comm.parentHashes) == 1 { // don't print stats for merge commits
				diffStats := make(map[string]*merge.MergeStats)
				diffStats, _, err := calculateMergeStats(queryist, sqlCtx, diffStats, comm.parentHashes[0], comm.commitHash)
				if err != nil {
					return err
				}
				printDiffStats(diffStats, pager)
				pager.Writer.Write([]byte("\n"))
			}
		}
	}

	return nil
}

func logToStdOut(apr *argparser.ArgParseResults, commits []CommitInfo, sqlCtx *sql.Context, queryist cli.Queryist) (err error) {
	if cli.ExecuteWithStdioRestored == nil {
		return nil
	}
	cli.ExecuteWithStdioRestored(func() {
		pager := outputpager.Start()
		defer pager.Stop()
		if apr.Contains(cli.GraphFlag) {
			logGraph(pager, apr, commits)
		} else if apr.Contains(cli.OneLineFlag) {
			err = logCompact(pager, apr, commits, sqlCtx, queryist)
		} else {
			err = logDefault(pager, apr, commits, sqlCtx, queryist)
		}
	})

	return
}

// printDiffStats prints the diff stats for a commit to a pager
func printDiffStats(diffStats map[string]*merge.MergeStats, pager *outputpager.Pager) {
	maxNameLen := 0
	maxModCount := 0
	rowsAdded := 0
	rowsDeleted := 0
	rowsChanged := 0
	var tbls []string
	for tblName, stats := range diffStats {
		if stats.Operation == merge.TableModified {
			tbls = append(tbls, tblName)
			nameLen := len(tblName)
			modCount := stats.Adds + stats.Modifications + stats.Deletes

			if nameLen > maxNameLen {
				maxNameLen = nameLen
			}

			if modCount > maxModCount {
				maxModCount = modCount
			}

			rowsAdded += stats.Adds
			rowsChanged += stats.Modifications
			rowsDeleted += stats.Deletes
		}
	}

	if len(tbls) > 0 {
		sort.Strings(tbls)
		modCountStrLen := len(strconv.FormatInt(int64(maxModCount), 10))
		format := fmt.Sprintf(" %%-%ds | %%-%ds %%s\n", maxNameLen, modCountStrLen)

		for _, tbl := range tbls {
			stats := diffStats[tbl]
			if stats.Operation == merge.TableModified {
				modCount := stats.Adds + stats.Modifications + stats.Deletes
				modCountStr := strconv.FormatInt(int64(modCount), 10)
				visualizedChanges := visualizeChangesForLog(stats, maxModCount)

				pager.Writer.Write([]byte(fmt.Sprintf(format, tbl, modCountStr, visualizedChanges)))
			}
		}

		details := fmt.Sprintf(" %d tables changed, %d rows added(+), %d rows modified(*), %d rows deleted(-)\n", len(tbls), rowsAdded, rowsChanged, rowsDeleted)
		pager.Writer.Write([]byte(details))
	}

	for tblName, stats := range diffStats {
		if stats.Operation == merge.TableAdded {
			pager.Writer.Write([]byte(" " + tblName + " added\n"))
		}
	}
	for tblName, stats := range diffStats {
		if stats.Operation == merge.TableRemoved {
			pager.Writer.Write([]byte(" " + tblName + " deleted\n"))
		}
	}
}

// visualizeChangesForLog generates the string with the appropriate symbols to represent the changes in a commit with
// the corresponding color suitable for writing to a pager
func visualizeChangesForLog(stats *merge.MergeStats, maxMods int) string {
	color.NoColor = false
	const maxVisLen = 30 //can be a bit longer due to min len and rounding

	resultStr := ""
	if stats.Adds > 0 {
		addLen := int(maxVisLen * (float64(stats.Adds) / float64(maxMods)))
		if addLen > stats.Adds {
			addLen = stats.Adds
		}
		addStr := fillStringWithChar('+', addLen)
		resultStr += color.HiGreenString("%s", addStr)

	}

	if stats.Modifications > 0 {
		modLen := int(maxVisLen * (float64(stats.Modifications) / float64(maxMods)))
		if modLen > stats.Modifications {
			modLen = stats.Modifications
		}
		modStr := fillStringWithChar('*', modLen)
		resultStr += color.HiYellowString("%s", modStr)
	}

	if stats.Deletes > 0 {
		delLen := int(maxVisLen * (float64(stats.Deletes) / float64(maxMods)))
		if delLen > stats.Deletes {
			delLen = stats.Deletes
		}
		delStr := fillStringWithChar('-', delLen)
		resultStr += color.HiRedString("%s", delStr)
	}

	return resultStr
}

func handleErrAndExit(err error) int {
	if err != nil {
		cli.PrintErrln(strings.ReplaceAll(err.Error(), "Invalid argument to dolt_log: ", ""))
		return 1
	}

	return 0
}
