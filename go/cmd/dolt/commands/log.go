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
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
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
	
{{.EmphasisLeft}}dolt log [<revisions>...] <table>{{.EmphasisRight}}
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
	return cli.CreateLogArgParser()
}

// Exec executes the command
func (cmd LogCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	return cmd.logWithLoggerFunc(ctx, commandStr, args, dEnv, cliCtx)
}

func (cmd LogCmd) logWithLoggerFunc(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, logDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		handleErrAndExit(err)
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

	return logCommits(apr, logRows, queryist, sqlCtx)
}

// constructInterpolatedDoltLogQuery generates the sql query necessary to call the DOLT_LOG() function.
// Also interpolates this query to prevent sql injection.
func constructInterpolatedDoltLogQuery(apr *argparser.ArgParseResults, queryist cli.Queryist, sqlCtx *sql.Context) (string, error) {
	var params []interface{}

	var buffer bytes.Buffer
	var first bool
	first = true

	// todo: clean up

	branchName := ""
	altBranchName := ""
	tableName := ""
	tagGiven := false
	var tagHash string
	var writeToDoltLog []string

	if apr.NArg() > 2 {
		return "", fmt.Errorf("error: too many arguments: %d", apr.NArg())
	}
	if apr.NArg() == 1 {
		if row, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("select name from dolt_branches where name = '%s'", apr.Arg(0))); len(row) > 0 && err == nil {
			branchName = apr.Arg(0)
			writeToDoltLog = append(writeToDoltLog, apr.Arg(0))
		} else if _, _, err = queryist.Query(sqlCtx, fmt.Sprintf("describe %s", apr.Arg(0))); err == nil {
			tableName = apr.Arg(0)
		} else if row, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("select tag_hash from dolt_tags where tag_name = '%s'", apr.Arg(0))); len(row) > 0 && err == nil {
			tagGiven = true
			tagHash = row[0][0].(string)
		} else if strings.Contains(apr.Arg(0), "..") || strings.Contains(apr.Arg(0), "^") {
			writeToDoltLog = append(writeToDoltLog, apr.Arg(0))
		} else {
			return "", fmt.Errorf("error: table %s does not exist", apr.Arg(0))
		}
	}
	if apr.NArg() == 2 {
		for i, arg := range apr.Args {
			if i == 0 {
				if row, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("select name from dolt_branches where name = '%s'", arg)); len(row) > 0 && err == nil {
					branchName = arg
					writeToDoltLog = append(writeToDoltLog, arg)
				} else if row, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("select tag_hash from dolt_tags where tag_name = '%s'", arg)); len(row) > 0 && err == nil {
					tagGiven = true
					tagHash = row[0][0].(string)
				} else if strings.Contains(arg, "..") || strings.Contains(arg, "^") {
					writeToDoltLog = append(writeToDoltLog, arg)
				} else {
					return "", fmt.Errorf("error: table %s does not exist", arg)
				}
			} else {
				if _, _, err := queryist.Query(sqlCtx, fmt.Sprintf("describe %s", arg)); err == nil {
					tableName = arg
				} else if row, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("select name from dolt_branches where name = '%s'", arg)); len(row) > 0 && err == nil {
					altBranchName = arg
					writeToDoltLog = append(writeToDoltLog, arg)
				} else if row, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("select tag_hash from dolt_tags where tag_name = '%s'", arg)); len(row) > 0 && err == nil {
					tagGiven = true
					tagHash = row[0][0].(string)
				} else if strings.Contains(arg, "..") || strings.Contains(arg, "^") {
					writeToDoltLog = append(writeToDoltLog, arg)
				} else {
					return "", fmt.Errorf("error: table %s does not exist", arg)
				}
			}
		}
	}

	// todo: 3 branch refs, 4 branch refs
	/*if apr.NArg() == 3 {
		b1 := fmt.Sprintf("'%s'", apr.Arg(0))
		if notValue, hasNotFlag := apr.GetValue(cli.NotFlag); hasNotFlag {
			b1 = b1 + ", '--not', '" + notValue + "'"
		}
		b2 := fmt.Sprintf("'%s'", apr.Arg(1))
		if notValue, hasNotFlag := apr.GetValue(cli.NotFlag); hasNotFlag {
			b2 = b2 + ", '--not', '" + notValue + "'"
		}
		b3 := fmt.Sprintf("'%s'", apr.Arg(2))
		if notValue, hasNotFlag := apr.GetValue(cli.NotFlag); hasNotFlag {
			b3 = b3 + ", '--not', '" + notValue + "'"
		}

		query := fmt.Sprintf(`
			select b1.commit_hash from dolt_log(%s) as b1
			cross join (select commit_hash from dolt_log(%s)) as b2
			    on b1.commit_hash = b2.commit_hash
			cross join (select commit_hash from dolt_log(%s)) as b3
			on b2.commit_hash = b3.commit_hash`, b1, b2, b3)
		return query, nil
	}*/

	if branchName != "" && altBranchName != "" {
		leftBranch := fmt.Sprintf("'%s'", branchName)
		if notValue, hasNotFlag := apr.GetValue(cli.NotFlag); hasNotFlag {
			leftBranch = leftBranch + ", '--not', '" + notValue + "'"
		}
		rightBranch := fmt.Sprintf("'%s'", altBranchName)
		if notValue, hasNotFlag := apr.GetValue(cli.NotFlag); hasNotFlag {
			rightBranch = rightBranch + ", '--not', '" + notValue + "'"
		}
		query := fmt.Sprintf(`
			select l.commit_hash from dolt_log(%s) as l 
			left join (select * from dolt_log(%s)) as r 
    		on l.commit_hash = r.commit_hash
    		union
    		select r.commit_hash from dolt_log(%s) as l
    		right join (select * from dolt_log(%s)) as r
    		on l.commit_hash = r.commit_hash`, leftBranch, rightBranch, leftBranch, rightBranch)
		return query, nil
	}

	if tableName != "" {
		buffer.WriteString("select dl.commit_hash from dolt_log(")
	} else {
		buffer.WriteString("select commit_hash from dolt_log(")
	}

	writeToBuffer := func(s string, param bool) {
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
	}

	for _, args := range writeToDoltLog {
		writeToBuffer("?", true)
		params = append(params, args)
	}

	if minParents, hasMinParents := apr.GetValue(cli.MinParentsFlag); hasMinParents {
		writeToBuffer("?", true)
		params = append(params, "--min-parents="+minParents)
	}

	if hasMerges := apr.Contains(cli.MergesFlag); hasMerges {
		writeToBuffer("--merges", false)
	}

	if excludedCommits, hasExcludedCommits := apr.GetValueList(cli.NotFlag); hasExcludedCommits {
		writeToBuffer("--not", false)
		for _, commit := range excludedCommits {
			writeToBuffer("?", true)
			params = append(params, commit)
		}
	}

	if decoration, hasDecoration := apr.GetValue(cli.DecorateFlag); hasDecoration {
		writeToBuffer("? ", true)
		params = append(params, "--decorate="+decoration)
	}

	if tableName != "" {
		if branchName != "" {
			rows, err := GetRowsForSql(queryist, sqlCtx, "select database()")
			if err != nil {
				return "", fmt.Errorf("could not retrieve database name")
			}
			dbName := rows[0][0].(string)
			buffer.WriteString(fmt.Sprintf(") as dl join (select * from `%s/%s`.dolt_diff where table_name = ?) as dd on dl.commit_hash = dd.commit_hash", dbName, branchName))
		} else {
			buffer.WriteString(") as dl join (select * from dolt_diff where table_name = ?) as dd on dl.commit_hash = dd.commit_hash")
		}
		params = append(params, tableName)
	} else if tagGiven {
		buffer.WriteString(fmt.Sprintf(") where commit_hash = '%s'", tagHash))
	} else {
		buffer.WriteString(")")
	}

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

func logCommits(apr *argparser.ArgParseResults, sqlResult []sql.Row, queryist cli.Queryist, sqlCtx *sql.Context) int {
	var commitsInfo []CommitInfo
	for _, row := range sqlResult {
		cmHash := row[0].(string)
		commit, err := getCommitInfo(queryist, sqlCtx, cmHash)
		if err != nil {
			return handleErrAndExit(err)
		}
		commitsInfo = append(commitsInfo, *commit)
	}

	logToStdOut(apr, commitsInfo)

	return 0
}

func logCompact(pager *outputpager.Pager, apr *argparser.ArgParseResults, commits []CommitInfo) {
	for _, comm := range commits {
		if len(comm.parentHashes) < apr.GetIntOrDefault(cli.MinParentsFlag, 0) {
			return
		}

		chStr := comm.commitHash
		if apr.Contains(cli.ParentsFlag) {
			for _, h := range comm.parentHashes {
				chStr += " " + h
			}
		}

		// TODO: use short hash instead
		// Write commit hash
		pager.Writer.Write([]byte(fmt.Sprintf("\033[33m%s \033[0m", chStr)))

		if decoration := apr.GetValueOrDefault(cli.DecorateFlag, "auto"); decoration != "no" {
			printRefs(pager, &comm, decoration)
		}

		formattedDesc := strings.Replace(comm.commitMeta.Description, "\n", " ", -1) + "\n"
		pager.Writer.Write([]byte(formattedDesc))
	}
}

func logDefault(pager *outputpager.Pager, apr *argparser.ArgParseResults, commits []CommitInfo) {
	for _, comm := range commits {
		PrintCommitInfo(pager, apr.GetIntOrDefault(cli.MinParentsFlag, 0), apr.Contains(cli.ParentsFlag), apr.GetValueOrDefault(cli.DecorateFlag, "auto"), &comm)
	}
}

func logToStdOut(apr *argparser.ArgParseResults, commits []CommitInfo) {
	if cli.ExecuteWithStdioRestored == nil {
		return
	}
	cli.ExecuteWithStdioRestored(func() {
		pager := outputpager.Start()
		defer pager.Stop()
		if apr.Contains(cli.OneLineFlag) {
			logCompact(pager, apr, commits)
		} else {
			logDefault(pager, apr, commits)
		}
	})
}

func handleErrAndExit(err error) int {
	if err != nil {
		if strings.Contains(err.Error(), "both revisions cannot contain '^'") {
			// just returns nothing
			return 0
		}

		cli.PrintErrln(strings.ReplaceAll(err.Error(), "Invalid argument to dolt_log: ", ""))
		return 1
	}

	return 0
}
