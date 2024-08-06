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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

// printData structure is used to pass parsed status data to the print function
// This ensures that the business logic is separate from the printing logic: first we parse the data, then we print it.
type printData struct {
	branchName,
	remoteName,
	remoteBranchName string

	ahead,
	behind int64

	conflictsPresent,
	showIgnoredTables,
	statusPresent,
	mergeActive bool

	stagedTables,
	unstagedTables,
	untrackedTables,
	filteredUntrackedTables,
	unmergedTables map[string]string

	constraintViolationTables,
	dataConflictTables,
	schemaConflictTables map[string]bool

	ignorePatterns doltdb.IgnorePatterns
	ignoredTables  doltdb.IgnoredTables
}

var statusDocs = cli.CommandDocumentationContent{
	ShortDesc: "Show the working status",
	LongDesc:  `Displays working tables that differ from the current HEAD commit, tables that differ from the staged tables, and tables that are in the working tree that are not tracked by dolt. The first are what you would commit by running {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}; the second and third are what you could commit by running {{.EmphasisLeft}}dolt add .{{.EmphasisRight}} before running {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}.`,
	Synopsis:  []string{""},
}

type StatusCmd struct{}

func (cmd StatusCmd) RequiresRepo() bool {
	return false
}

var _ cli.RepoNotRequiredCommand = StatusCmd{}
var _ cli.EventMonitoredCommand = StatusCmd{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StatusCmd) Name() string {
	return "status"
}

// Description returns a description of the command
func (cmd StatusCmd) Description() string {
	return "Show the working tree status."
}

func (cmd StatusCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(statusDocs, ap)
}

func (cmd StatusCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsFlag(cli.ShowIgnoredFlag, "", "Show tables that are ignored (according to dolt_ignore)")
	return ap
}

func (cmd StatusCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_STATUS
}

func (cmd StatusCmd) Exec(ctx context.Context, commandStr string, args []string, _ *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	apr, _, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, statusDocs)
	if terminate {
		return status
	}

	showIgnoredTables := apr.Contains(cli.ShowIgnoredFlag)

	// configure SQL engine
	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return handleStatusVErr(err)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	// get status information from the database
	pd, err := createPrintData(err, queryist, sqlCtx, showIgnoredTables)
	if err != nil {
		return handleStatusVErr(err)
	}

	err = printEverything(pd)
	if err != nil {
		return handleStatusVErr(err)
	}

	return 0
}

func createPrintData(err error, queryist cli.Queryist, sqlCtx *sql.Context, showIgnoredTables bool) (*printData, error) {
	branchName, err := getActiveBranchName(sqlCtx, queryist)
	if err != nil {
		return nil, err
	}

	ignorePatterns, err := getIgnoredTablePatternsFromSql(queryist, sqlCtx)
	if err != nil {
		return nil, err
	}

	stagedTableNames, workingTableNames, err := getWorkingStagedTables(queryist, sqlCtx)
	if err != nil {
		return nil, err
	}

	constraintViolationTables, err := getConstraintViolationTables(queryist, sqlCtx)
	if err != nil {
		return nil, err
	}

	dataConflictTables, err := getDataConflictsTables(queryist, sqlCtx)
	if err != nil {
		return nil, err
	}

	mergeActive, err := getMergeStatus(queryist, sqlCtx)
	if err != nil {
		return nil, err
	}

	remoteName, remoteBranchName, currentBranchCommit, err := getLocalBranchInfo(queryist, sqlCtx, branchName)
	if err != nil {
		return nil, err
	}

	ahead, behind, err := getRemoteInfo(queryist, sqlCtx, branchName, remoteName, remoteBranchName, currentBranchCommit)
	if err != nil {
		return nil, err
	}

	statusRows, err := GetRowsForSql(queryist, sqlCtx, "select table_name,staged,status from dolt_status;")
	if err != nil {
		return nil, err
	}
	statusPresent := len(statusRows) > 0

	conflictedTables := getConflictedTables(statusRows)

	// sort tables into categories
	conflictsPresent := false
	stagedTables := map[string]string{}
	unstagedTables := map[string]string{}
	untrackedTables := map[string]string{}
	unmergedTables := map[string]string{}
	schemaConflictTables := map[string]bool{}
	ignoredTables := doltdb.IgnoredTables{}
	if statusPresent {
		for _, row := range statusRows {
			tableName := row[0].(string)
			staged := row[1]
			status := row[2].(string)

			isStaged, err := GetTinyIntColAsBool(staged)
			if err != nil {
				return nil, err
			}

			shouldIgnoreTable := false
			if !isStaged {
				// determine if the table should be ignored
				ignored, err := ignorePatterns.IsTableNameIgnored(doltdb.TableName{Name: tableName})
				if conflict := doltdb.AsDoltIgnoreInConflict(err); conflict != nil {
					ignoredTables.Conflicts = append(ignoredTables.Conflicts, *conflict)
				} else if err != nil {
					return nil, err
				} else if ignored == doltdb.DontIgnore {
					ignoredTables.DontIgnore = append(ignoredTables.DontIgnore, doltdb.TableName{Name: tableName})
				} else if ignored == doltdb.Ignore {
					ignoredTables.Ignore = append(ignoredTables.Ignore, doltdb.TableName{Name: tableName})
				} else {
					return nil, fmt.Errorf("unrecognized ignore result value: %v", ignored)
				}
				shouldIgnoreTable = ignored == doltdb.Ignore
			}
			shouldIgnoreTable = shouldIgnoreTable || doltdb.IsFullTextTable(tableName)

			switch status {
			case "renamed":
				// for renamed tables, add both source and dest changes
				parts := strings.Split(tableName, " -> ")
				srcTableName := parts[0]
				dstTableName := parts[1]

				if workingTableNames[dstTableName] {
					unstagedTables[srcTableName] = "deleted"
					untrackedTables[dstTableName] = "new table"
				} else if stagedTableNames[dstTableName] {
					stagedTables[tableName] = status
				}
			case "conflict":
				conflictsPresent = true
				if isStaged {
					stagedTables[tableName] = status
				} else {
					unmergedTables[tableName] = "both modified"
				}
			case "deleted", "modified", "added", "new table":
				if shouldIgnoreTable {
					continue
				}

				if isStaged {
					stagedTables[tableName] = status
				} else {
					isTableConflicted := conflictedTables[tableName]
					if !isTableConflicted {
						if status == "new table" {
							untrackedTables[tableName] = status
						} else {
							unstagedTables[tableName] = status
						}
					}
				}
			case "schema conflict":
				conflictsPresent = true
				schemaConflictTables[tableName] = true

			case "constraint violation":
				constraintViolationTables[tableName] = true

			default:
				panic(fmt.Sprintf("table %s, unexpected merge status: %s", tableName, status))
			}
		}
	}

	// filter out ignored tables from untracked tables
	filteredUntrackedTables := map[string]string{}
	for tableName, status := range untrackedTables {
		ignored, err := ignorePatterns.IsTableNameIgnored(doltdb.TableName{Name: tableName})

		if conflict := doltdb.AsDoltIgnoreInConflict(err); conflict != nil {
			continue
		} else if err != nil {
			return nil, err
		} else if ignored == doltdb.DontIgnore {
			// no-op
		} else if ignored == doltdb.Ignore {
			continue
		} else {
			return nil, fmt.Errorf("unrecognized ignore result value: %v", ignored)
		}
		filteredUntrackedTables[tableName] = status
	}

	pd := printData{
		branchName:                branchName,
		remoteName:                remoteName,
		remoteBranchName:          remoteBranchName,
		ahead:                     ahead,
		behind:                    behind,
		conflictsPresent:          conflictsPresent,
		showIgnoredTables:         showIgnoredTables,
		statusPresent:             statusPresent,
		mergeActive:               mergeActive,
		stagedTables:              stagedTables,
		unstagedTables:            unstagedTables,
		untrackedTables:           untrackedTables,
		filteredUntrackedTables:   filteredUntrackedTables,
		unmergedTables:            unmergedTables,
		ignoredTables:             ignoredTables,
		constraintViolationTables: constraintViolationTables,
		schemaConflictTables:      schemaConflictTables,
		ignorePatterns:            ignorePatterns,
		dataConflictTables:        dataConflictTables,
	}
	return &pd, nil
}

func getConflictedTables(statusRows []sql.Row) map[string]bool {
	conflictedTables := make(map[string]bool)
	for _, row := range statusRows {
		tableName := row[0].(string)
		status := row[2].(string)
		if status == "conflict" {
			conflictedTables[tableName] = true
		}
	}
	return conflictedTables
}

func getRemoteInfo(queryist cli.Queryist, sqlCtx *sql.Context, branchName string, remoteName string, remoteBranchName string, currentBranchCommit string) (ahead int64, behind int64, err error) {
	ahead = 0
	behind = 0
	if len(remoteName) > 0 {
		// get remote branch
		remoteBranchRef := fmt.Sprintf("remotes/%s/%s", remoteName, remoteBranchName)
		q := fmt.Sprintf("select name, hash from dolt_remote_branches where name = '%s';", remoteBranchRef)
		remoteBranches, err := GetRowsForSql(queryist, sqlCtx, q)
		if err != nil {
			return ahead, behind, err
		}
		if len(remoteBranches) != 1 {
			return ahead, behind, fmt.Errorf("could not find remote branch %s", remoteBranchRef)
		}
		remoteBranchCommit := remoteBranches[0][1].(string)

		q = fmt.Sprintf("call dolt_count_commits('--from', '%s', '--to', '%s')", currentBranchCommit, remoteBranchCommit)
		rows, err := GetRowsForSql(queryist, sqlCtx, q)
		if err != nil {
			return ahead, behind, err
		}
		if len(rows) != 1 {
			return ahead, behind, fmt.Errorf("could not count commits between %s and %s", currentBranchCommit, remoteBranchCommit)
		}
		aheadDb := rows[0][0]
		behindDb := rows[0][1]

		ahead, err = getInt64ColAsInt64(aheadDb)
		if err != nil {
			return ahead, behind, err
		}
		behind, err = getInt64ColAsInt64(behindDb)
		if err != nil {
			return ahead, behind, err
		}
	}
	return ahead, behind, nil
}

func getLocalBranchInfo(queryist cli.Queryist, sqlCtx *sql.Context, branchName string) (remoteName, remoteBranchName, currentBranchCommit string, err error) {
	remoteName = ""
	currentBranchCommit = ""
	remoteBranchName = ""

	localBranches, err := GetRowsForSql(queryist, sqlCtx, "select name, hash, remote, branch from dolt_branches;")
	if err != nil {
		return remoteName, remoteBranchName, currentBranchCommit, err
	}
	for _, row := range localBranches {
		branch := row[0].(string)
		if branch == branchName {
			currentBranchCommit = row[1].(string)
			remoteName = row[2].(string)
			remoteBranchName = row[3].(string)
		}
	}
	if currentBranchCommit == "" {
		return remoteName, remoteBranchName, currentBranchCommit, fmt.Errorf("could not find current branch commit")
	}
	return remoteName, remoteBranchName, currentBranchCommit, nil
}

func getMergeStatus(queryist cli.Queryist, sqlCtx *sql.Context) (bool, error) {
	mergeRows, err := GetRowsForSql(queryist, sqlCtx, "select is_merging from dolt_merge_status;")
	if err != nil {
		return false, err
	}
	// determine if a merge is active
	mergeActive := false
	if len(mergeRows) == 1 {
		isMerging := mergeRows[0][0]
		mergeActive, err = GetTinyIntColAsBool(isMerging)
		if err != nil {
			return false, err
		}
	} else {
		mergeActive = true
	}
	return mergeActive, nil
}

func getDataConflictsTables(queryist cli.Queryist, sqlCtx *sql.Context) (map[string]bool, error) {
	dataConflictTables := make(map[string]bool)
	dataConflicts, err := GetRowsForSql(queryist, sqlCtx, "select * from dolt_conflicts;")
	if err != nil {
		return nil, err
	}
	for _, row := range dataConflicts {
		tableName := row[0].(string)
		dataConflictTables[tableName] = true
	}
	return dataConflictTables, nil
}

func getConstraintViolationTables(queryist cli.Queryist, sqlCtx *sql.Context) (map[string]bool, error) {
	constraintViolationTables := make(map[string]bool)
	constraintViolations, err := GetRowsForSql(queryist, sqlCtx, "select * from dolt_constraint_violations;")
	if err != nil {
		return nil, err
	}
	for _, row := range constraintViolations {
		tableName := row[0].(string)
		constraintViolationTables[tableName] = true
	}
	return constraintViolationTables, nil
}

func getWorkingStagedTables(queryist cli.Queryist, sqlCtx *sql.Context) (map[string]bool, map[string]bool, error) {
	stagedTableNames := make(map[string]bool)
	workingTableNames := make(map[string]bool)
	diffs, err := GetRowsForSql(queryist, sqlCtx, "select * from dolt_diff where commit_hash='WORKING' OR commit_hash='STAGED';")
	if err != nil {
		return nil, nil, err
	}
	for _, row := range diffs {
		commitHash := row[0].(string)
		tableName := row[1].(string)
		if commitHash == "STAGED" {
			stagedTableNames[tableName] = true
		} else {
			workingTableNames[tableName] = true
		}
	}
	return stagedTableNames, workingTableNames, nil
}

func getIgnoredTablePatternsFromSql(queryist cli.Queryist, sqlCtx *sql.Context) (doltdb.IgnorePatterns, error) {
	var ignorePatterns []doltdb.IgnorePattern
	ignoreRows, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("select * from %s", doltdb.IgnoreTableName))
	if err != nil {
		return nil, err
	}
	for _, row := range ignoreRows {
		pattern := row[0].(string)
		ignoreVal := row[1]

		var ignore bool
		if ignoreString, ok := ignoreVal.(string); ok {
			ignore = ignoreString == "1"
		} else if ignoreInt, ok := ignoreVal.(int8); ok {
			ignore = ignoreInt == 1
		} else {
			return nil, fmt.Errorf("unexpected type for ignore column, value = %s", ignoreVal)
		}

		ip := doltdb.NewIgnorePattern(pattern, ignore)
		ignorePatterns = append(ignorePatterns, ip)
	}
	return ignorePatterns, nil
}

func printEverything(data *printData) error {
	statusFmt := "\t%-18s%s"
	constraintViolationsExist := len(data.constraintViolationTables) > 0
	changesPresent := false

	// branch name
	cli.Printf(branchHeader, data.branchName)

	// remote info
	if data.remoteName != "" {
		ahead := data.ahead
		behind := data.behind
		remoteBranchRef := fmt.Sprintf("%s/%s", data.remoteName, data.remoteBranchName)

		if ahead > 0 && behind > 0 {
			cli.Printf(`Your branch and '%s' have diverged,
and have %v and %v different commits each, respectively.
  (use "dolt pull" to update your local branch)
	`, remoteBranchRef, ahead, behind)
			changesPresent = true
		} else if ahead > 0 {
			s := ""
			if ahead > 1 {
				s = "s"
			}
			cli.Printf(`Your branch is ahead of '%s' by %v commit%s.
  (use "dolt push" to publish your local commits)
	`, remoteBranchRef, ahead, s)
			changesPresent = true
		} else if behind > 0 {
			s := ""
			if behind > 1 {
				s = "s"
			}
			cli.Printf(`Your branch is behind '%s' by %v commit%s, and can be fast-forwarded.
  (use "dolt pull" to update your local branch)
	`, remoteBranchRef, behind, s)
			changesPresent = true
		} else {
			cli.Printf("Your branch is up to date with '%s'.\n", remoteBranchRef)
		}
	}

	// merge info
	if data.mergeActive {
		if constraintViolationsExist && data.conflictsPresent {
			cli.Println(fmt.Sprintf(unmergedTablesHeader, "conflicts and constraint violations"))
		} else if data.conflictsPresent {
			cli.Println(fmt.Sprintf(unmergedTablesHeader, "conflicts"))
		} else if constraintViolationsExist {
			cli.Println(fmt.Sprintf(unmergedTablesHeader, "constraint violations"))
		} else {
			cli.Println(allMergedHeader)
		}
	}

	// staged tables
	if len(data.stagedTables) > 0 {
		cli.Println(stagedHeader)
		cli.Println(stagedHeaderHelp)
		for tableName, status := range data.stagedTables {
			if !doltdb.IsReadOnlySystemTable(tableName) {
				text := fmt.Sprintf(statusFmt, status+":", tableName)
				greenText := color.GreenString(text)
				cli.Println(greenText)
				changesPresent = true
			}
		}
	}

	// conflicts and violations
	if data.conflictsPresent || constraintViolationsExist {
		cli.Println(unmergedPathsHeader)
		cli.Println(mergedTableHelp)

		// conflicted tables
		if data.conflictsPresent {
			// show schema conflicts
			for tableName := range data.schemaConflictTables {
				text := fmt.Sprintf(statusFmt, schemaConflictLabel, tableName)
				redText := color.RedString(text)
				cli.Println(redText)
				changesPresent = true
			}
			// show data conflicts
			for tableName := range data.dataConflictTables {
				text := fmt.Sprintf(statusFmt, bothModifiedLabel, tableName)
				redText := color.RedString(text)
				cli.Println(redText)
				changesPresent = true
			}
		}

		// constraint violations
		if constraintViolationsExist {
			for tableName := range data.constraintViolationTables {
				hasConflicts := data.dataConflictTables[tableName] || data.schemaConflictTables[tableName]
				if hasConflicts {
					continue
				}
				text := fmt.Sprintf(statusFmt, "modified", tableName)
				redText := color.RedString(text)
				cli.Println(redText)
				changesPresent = true
			}
		}
	}

	// unstaged tables
	if len(data.unstagedTables) > 0 {
		filteredUnstagedTables := make(map[string]string)
		for tableName, status := range data.unstagedTables {
			hasConflicts := data.dataConflictTables[tableName] || data.schemaConflictTables[tableName]
			hasViolations := data.constraintViolationTables[tableName]
			if hasConflicts || hasViolations {
				continue
			}
			filteredUnstagedTables[tableName] = status
		}

		if len(filteredUnstagedTables) > 0 {
			cli.Println()
			cli.Println(workingHeader)
			cli.Println(workingHeaderHelp)
			for tableName, status := range filteredUnstagedTables {
				text := fmt.Sprintf(statusFmt, status+":", tableName)
				redText := color.RedString(text)
				cli.Println(redText)
			}
			changesPresent = true
		}
	}

	// untracked tables
	if len(data.untrackedTables) > 0 {
		if changesPresent {
			cli.Println()
		}
		cli.Println(untrackedHeader)
		cli.Println(untrackedHeaderHelp)
		for tableName, status := range data.filteredUntrackedTables {
			text := fmt.Sprintf(statusFmt, status+":", tableName)
			redText := color.RedString(text)
			cli.Println(redText)
		}
		changesPresent = true
	}

	// ignored tables
	if data.showIgnoredTables && len(data.ignoredTables.Ignore) > 0 {
		if changesPresent {
			cli.Println()
		}
		cli.Println(ignoredHeader)
		cli.Println(ignoredHeaderHelp)
		for _, tableName := range data.ignoredTables.Ignore {
			text := fmt.Sprintf(statusFmt, "new table:", tableName)
			redText := color.RedString(text)
			cli.Println(redText)
			changesPresent = true
		}
	}

	if len(data.ignoredTables.Conflicts) > 0 {
		if changesPresent {
			cli.Println()
		}
		cli.Println(conflictedIgnoredHeader)
		cli.Println(conflictedIgnoredHeaderHelp)
		for _, conflict := range data.ignoredTables.Conflicts {
			text := fmt.Sprintf(statusFmt, "new table:", conflict.Table)
			redText := color.RedString(text)
			cli.Println(redText)
			changesPresent = true
		}
	}

	// nothing to commit
	if !changesPresent {
		cli.Println("nothing to commit, working tree clean")
	}

	return nil
}

func handleStatusVErr(err error) int {
	if err != argparser.ErrHelp {
		cli.PrintErrln(errhand.VerboseErrorFromError(err).Verbose())
	}
	return 1
}

// getJsonDocumentColAsString returns the value of a JSONDocument column as a string
// This is necessary because Queryist may return a tinyint column as a bool (when using SQLEngine)
// or as a string (when using ConnectionQueryist).
func getJsonDocumentColAsString(sqlCtx *sql.Context, col interface{}) (string, error) {
	switch v := col.(type) {
	case string:
		return v, nil
	case types.JSONDocument:
		text, err := v.JSONString()
		if err != nil {
			return "", err
		}
		return text, nil
	default:
		return "", fmt.Errorf("unexpected type %T, was expecting JSONDocument or string", v)
	}
}
