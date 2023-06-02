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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

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

// Exec executes the command
func (cmd StatusCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	// parse arguments
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, statusDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
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
	pd, err := cmd.createPrintData(err, queryist, sqlCtx, showIgnoredTables)
	if err != nil {
		return handleStatusVErr(err)
	}

	// print everything
	err = printEverything(pd)
	if err != nil {
		return handleStatusVErr(err)
	}

	return 0
}

func (cmd StatusCmd) createPrintData(err error, queryist cli.Queryist, sqlCtx *sql.Context, showIgnoredTables bool) (*printData, error) {
	// get current branch name
	branchName, err := getBranchName(queryist, sqlCtx)
	if err != nil {
		return nil, err
	}

	// get ignored tables
	ignorePatterns, err := getIgnoredTablePatternsFromSql(queryist, sqlCtx)
	if err != nil {
		return nil, err
	}

	// get staged/working tables
	stagedTableNames := make(map[string]bool)
	workingTableNames := make(map[string]bool)
	diffs, err := getRowsForSql(queryist, sqlCtx, "select * from dolt_diff where commit_hash='WORKING' OR commit_hash='STAGED';")
	if err != nil {
		return nil, err
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

	// get constraint violations
	constraintViolationTables := make(map[string]bool)
	constraintViolations, err := getRowsForSql(queryist, sqlCtx, "select * from dolt_constraint_violations;")
	if err != nil {
		return nil, err
	}
	for _, row := range constraintViolations {
		tableName := row[0].(string)
		constraintViolationTables[tableName] = true
	}

	// get data conflicts
	dataConflictTables := make(map[string]bool)
	dataConflicts, err := getRowsForSql(queryist, sqlCtx, "select * from dolt_conflicts;")
	if err != nil {
		return nil, err
	}
	for _, row := range dataConflicts {
		tableName := row[0].(string)
		dataConflictTables[tableName] = true
	}

	// get merge status
	mergeRows, err := getRowsForSql(queryist, sqlCtx, "select is_merging from dolt_merge_status;")
	if err != nil {
		return nil, err
	}
	// determine if a merge is active
	mergeActive := false
	if len(mergeRows) == 1 {
		isMerging := mergeRows[0][0]
		mergeActive, err = getTinyIntColAsBool(isMerging)
		if err != nil {
			return nil, err
		}
	} else {
		mergeActive = true
	}

	// get local branches
	localBranches, err := getRowsForSql(queryist, sqlCtx, "select name, hash, remote from dolt_branches;")
	if err != nil {
		return nil, err
	}
	var ahead int64 = 0
	var behind int64 = 0
	remoteName := ""
	currentBranchCommit := ""
	remoteBranchCommit := ""
	remoteBranchName := ""
	for _, row := range localBranches {
		branch := row[0].(string)
		if branch == branchName {
			currentBranchCommit = row[1].(string)
			remoteName = row[2].(string)
		}
	}
	if currentBranchCommit == "" {
		return nil, fmt.Errorf("could not find current branch commit")
	}

	if len(remoteName) > 0 {
		// get dolt remotes
		q := fmt.Sprintf("select name, url, fetch_specs, params from dolt_remotes where name = '%s';", remoteName)
		remotes, err := getRowsForSql(queryist, sqlCtx, q)
		if err != nil {
			return nil, err
		}
		if len(remotes) != 1 {
			return nil, fmt.Errorf("could not find remote %s", remoteName)
		}

		var fetchSpecs []string
		err = json.Unmarshal([]byte(remotes[0][2].(string)), &fetchSpecs)
		if err != nil {
			return nil, err
		}

		var params map[string]string
		err = json.Unmarshal([]byte(remotes[0][3].(string)), &params)
		if err != nil {
			return nil, err
		}

		remote := env.Remote{
			Name:       remotes[0][0].(string),
			Url:        remotes[0][1].(string),
			FetchSpecs: fetchSpecs,
			Params:     params,
		}

		branchRef := ref.NewBranchRef(branchName)
		remoteRef, err := env.GetTrackingRef(branchRef, remote)
		if err != nil {
			return nil, err
		}
		remoteBranchName = remoteRef.GetPath()
		remoteBranchRef := fmt.Sprintf("remotes/%s", remoteBranchName)

		// get remote branches
		q = fmt.Sprintf("select * from dolt_remote_branches where name = '%s';", remoteBranchRef)
		remoteBranches, err := getRowsForSql(queryist, sqlCtx, q)
		if err != nil {
			return nil, err
		}
		if len(remoteBranches) != 1 {
			return nil, fmt.Errorf("could not find remote branch %s", remoteBranchRef)
		}
		remoteBranchCommit = remoteBranches[0][1].(string)

		q = fmt.Sprintf("call dolt_count_commits('--from', '%s', '--to', '%s')", currentBranchCommit, remoteBranchCommit)
		rows, err := getRowsForSql(queryist, sqlCtx, q)
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 {
			return nil, fmt.Errorf("could not count commits between %s and %s", currentBranchCommit, remoteBranchCommit)
		}
		aheadDb := rows[0][0].(string)
		behindDb := rows[0][1].(string)

		ahead, err = strconv.ParseInt(aheadDb, 10, 64)
		if err != nil {
			return nil, err
		}
		behind, err = strconv.ParseInt(behindDb, 10, 64)
		if err != nil {
			return nil, err
		}
	}

	// get statuses
	statusRows, err := getRowsForSql(queryist, sqlCtx, "select * from dolt_status;")
	if err != nil {
		return nil, err
	}
	statusPresent := len(statusRows) > 0

	// find conflicts in statuses
	conflictedTables := make(map[string]bool)
	for _, row := range statusRows {
		tableName := row[0].(string)
		status := row[2].(string)
		if status == "conflict" {
			conflictedTables[tableName] = true
		}
	}

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

			isStaged, err := getTinyIntColAsBool(staged)
			if err != nil {
				return nil, err
			}

			// determine if the table should be ignored
			ignored, err := ignorePatterns.IsTableNameIgnored(tableName)
			if conflict := doltdb.AsDoltIgnoreInConflict(err); conflict != nil {
				ignoredTables.Conflicts = append(ignoredTables.Conflicts, *conflict)
			} else if err != nil {
				return nil, err
			} else if ignored == doltdb.DontIgnore {
				ignoredTables.DontIgnore = append(ignoredTables.DontIgnore, tableName)
			} else if ignored == doltdb.Ignore {
				ignoredTables.Ignore = append(ignoredTables.Ignore, tableName)
			} else {
				return nil, fmt.Errorf("unrecognized ignore result value: %v", ignored)
			}
			if err != nil {
				return nil, err
			}
			shouldIgnoreTable := ignored == doltdb.Ignore

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
			default:
				panic(fmt.Sprintf("table %s, unexpected merge status: %s", tableName, status))
			}
		}
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
		unmergedTables:            unmergedTables,
		ignoredTables:             ignoredTables,
		constraintViolationTables: constraintViolationTables,
		schemaConflictTables:      schemaConflictTables,
		ignorePatterns:            ignorePatterns,
		dataConflictTables:        dataConflictTables,
	}
	return &pd, nil
}

func getBranchName(queryist cli.Queryist, sqlCtx *sql.Context) (string, error) {
	rows, err := getRowsForSql(queryist, sqlCtx, "select active_branch()")
	if err != nil {
		return "", err
	}
	if len(rows) != 1 {
		return "", errors.New("expected one row in dolt_branches")
	}
	branchName := rows[0][0].(string)
	return branchName, nil
}

func getRowsForSql(queryist cli.Queryist, sqlCtx *sql.Context, q string) ([]sql.Row, error) {
	schema, ri, err := queryist.Query(sqlCtx, q)
	if err != nil {
		return nil, err
	}
	rows, err := sql.RowIterToRows(sqlCtx, schema, ri)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func getIgnoredTablePatternsFromSql(queryist cli.Queryist, sqlCtx *sql.Context) (doltdb.IgnorePatterns, error) {
	var ignorePatterns []doltdb.IgnorePattern
	ignoreRows, err := getRowsForSql(queryist, sqlCtx, fmt.Sprintf("select * from %s", doltdb.IgnoreTableName))
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
			return nil, errors.New(fmt.Sprintf("unexpected type for ignore column, value = %s", ignoreVal))
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
		remoteBranchName := data.remoteBranchName
		ahead := data.ahead
		behind := data.behind

		if ahead > 0 && behind > 0 {
			cli.Printf(`Your branch and '%s' have diverged,
and have %v and %v different commits each, respectively.
  (use "dolt pull" to update your local branch)`, remoteBranchName, ahead, behind)
		} else if ahead > 0 {
			s := ""
			if ahead > 1 {
				s = "s"
			}
			cli.Printf(`Your branch is ahead of '%s' by %v commit%s.
  (use "dolt push" to publish your local commits)`, remoteBranchName, ahead, s)
		} else if behind > 0 {
			s := ""
			if behind > 1 {
				s = "s"
			}
			cli.Printf(`Your branch is behind '%s' by %v commit%s, and can be fast-forwarded.
  (use "dolt pull" to update your local branch)`, remoteBranchName, behind, s)
		} else {
			cli.Printf("Your branch is up to date with '%s'.", remoteBranchName)
		}
		changesPresent = true
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
		for tableName, status := range data.untrackedTables {
			ignoreResult, err := data.ignorePatterns.IsTableNameIgnored(tableName)
			if err != nil {
				return err
			}
			isIgnored := ignoreResult == doltdb.Ignore
			if isIgnored {
				continue
			}
			text := fmt.Sprintf(statusFmt, status+":", tableName)
			redText := color.RedString(text)
			cli.Println(redText)
			changesPresent = true
		}
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
	cli.PrintErrln(errhand.VerboseErrorFromError(err).Verbose())
	return 1
}

// getTinyIntColAsBool returns the value of a tinyint column as a bool
// This is necessary because Queryist may return a tinyint column as a bool (when using SQLEngine)
// or as a string (when using ConnectionQueryist).
func getTinyIntColAsBool(col interface{}) (bool, error) {
	switch v := col.(type) {
	case bool:
		return v, nil
	case int:
		return v == 1, nil
	case string:
		return v == "1", nil
	default:
		return false, fmt.Errorf("unexpected type %T", v)
	}
}
