// Copyright 2023 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const (
	PushCmdRef  = "push"
	PopCmdRef   = "pop"
	DropCmdRef  = "drop"
	ClearCmdRef = "clear"
	ListCmdRef  = "list"
)

var stashDocs = cli.CommandDocumentationContent{
	ShortDesc: "Stash the changes in a dirty workspace away.",
	LongDesc: `Use dolt stash when you want to record the current state of the workspace and the index, but want to go back to a clean workspace. 

The command saves your local modifications away and reverts the workspace to match the HEAD commit. The stash entries that are saved away can be listed with 'dolt stash list'.
`,
	Synopsis: []string{
		"", // this is for `dolt stash` itself.
		"list",
		"pop {{.LessThan}}stash{{.GreaterThan}}",
		"clear",
		"drop {{.LessThan}}stash{{.GreaterThan}}",
	},
}

type StashCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StashCmd) Name() string {
	return "stash"
}

// Description returns a description of the command
func (cmd StashCmd) Description() string {
	return "Stash the changes in a dirty workspace away."
}

func (cmd StashCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(stashDocs, ap)
}

func (cmd StashCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateStashArgParser()
}

// EventType returns the type of the event to log
func (cmd StashCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_STASH
}

// Exec executes the command
func (cmd StashCmd) Exec(ctx context.Context, commandStr string, args []string, _ *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateStashArgParser()

	apr, _, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, stashDocs)
	if terminate {
		return status
	}
	if len(apr.Args) > 2 {
		cli.PrintErrln(fmt.Errorf("dolt stash takes 2 arguments, received %d", len(apr.Args)))
		return 1
	}

	subcommand := "push"
	if len(apr.Args) > 0 {
		subcommand = strings.ToLower(apr.Arg(0))
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	idx := 0
	if apr.NArg() > 1 {
		idx, err = parseStashIndex(apr.Arg(1))
		if err != nil {
			cli.PrintErrln(errhand.VerboseErrorFromError(err))
			return 1
		}
	}

	switch subcommand {
	case PushCmdRef:
		err = stashPush(queryist, sqlCtx, apr, subcommand)
	case PopCmdRef, DropCmdRef:
		err = stashRemove(queryist, sqlCtx, cliCtx, apr, subcommand, idx)
	case ListCmdRef:
		err = stashList(ctx, cliCtx)
	case ClearCmdRef:
		err = stashClear(queryist, sqlCtx, apr, subcommand)
	default:
		err = fmt.Errorf("unknown stash subcommand %s", subcommand)
	}

	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		if strings.Contains(err.Error(), "No local changes to save") {
			return 0
		}
		return 1
	}
	return 0
}

func stashPush(queryist cli.Queryist, sqlCtx *sql.Context, apr *argparser.ArgParseResults, subcommand string) error {
	rowIter, err := stashQuery(queryist, sqlCtx, apr, subcommand)
	if err != nil {
		return err
	}

	stashes, err := getStashesSQL(sqlCtx, queryist, 1)
	if err != nil {
		return err
	}
	stash := stashes[0]
	cli.Println(fmt.Sprintf("Saved working directory and index state WIP on %s: %s %s", stash.BranchReference, stash.CommitHash, stash.Description))
	_, err = sql.RowIterToRows(sqlCtx, rowIter)
	return err
}

func stashRemove(queryist cli.Queryist, sqlCtx *sql.Context, cliCtx cli.CliContext, apr *argparser.ArgParseResults, subcommand string, idx int) error {
	stashes, err := getStashesSQL(sqlCtx, queryist, 0)
	if err != nil {
		return err
	}
	if len(stashes) == 0 {
		return fmt.Errorf("No stash entries found.")
	}
	if len(stashes)-1 < idx {
		return fmt.Errorf("stash index stash@{%d} does not exist", idx)
	}

	interpolatedQuery, err := generateStashSql(apr, subcommand)
	if err != nil {
		return err
	}
	_, rowIter, _, err := queryist.Query(sqlCtx, interpolatedQuery)
	if err != nil {
		return err
	}

	if subcommand == PopCmdRef {
		ret := StatusCmd{}.Exec(sqlCtx, StatusCmd{}.Name(), []string{}, nil, cliCtx)
		if ret != 0 {
			cli.Println("The stash entry is kept in case you need it again.")
			return err
		}
	}

	cli.Println(fmt.Sprintf("Dropped refs/stash@{%v} (%s)", idx, stashes[idx].CommitHash))
	_, err = sql.RowIterToRows(sqlCtx, rowIter)
	return err
}

func stashList(ctx context.Context, cliCtx cli.CliContext) error {
	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return err
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	stashes, err := getStashesSQL(sqlCtx, queryist, 0)
	if err != nil {
		return err
	}
	for _, stash := range stashes {
		cli.Println(fmt.Sprintf("%s: WIP on %s: %s %s", stash.Name, stash.BranchReference, stash.CommitHash, stash.Description))
	}

	return nil
}

func stashClear(queryist cli.Queryist, sqlCtx *sql.Context, apr *argparser.ArgParseResults, subcommand string) error {
	rowIter, err := stashQuery(queryist, sqlCtx, apr, subcommand)
	if err != nil {
		return err
	}
	_, err = sql.RowIterToRows(sqlCtx, rowIter)
	return err
}

// getStashesSQL queries the dolt_stashes system table to return the requested number of stashes. A limit of 0 will get all stashes
func getStashesSQL(sqlCtx *sql.Context, queryist cli.Queryist, limit int) ([]*doltdb.Stash, error) {
	limitStr := fmt.Sprintf("limit %d", limit)
	if limit == 0 {
		limitStr = ""
	}

	qry := fmt.Sprintf("select stash_id, branch, hash, commit_message from dolt_stashes where name = '%s' order by stash_id ASC %s;", doltdb.DoltCliRef, limitStr)
	rows, err := GetRowsForSql(queryist, sqlCtx, qry)
	if err != nil {
		return nil, err
	}

	var stashes []*doltdb.Stash
	for _, s := range rows {
		id, ok := s[0].(string)
		if !ok {
			return nil, fmt.Errorf("Invalid stash id")
		}

		branch, ok := s[1].(string)
		if !ok {
			return nil, fmt.Errorf("invalid stash branch")
		}
		fullBranch := ref.NewBranchRef(branch).String()

		stashHash, ok := s[2].(string)
		if !ok {
			return nil, fmt.Errorf("invalid stash hash")
		}

		msg, ok := s[3].(string)
		if !ok {
			return nil, fmt.Errorf("invalid stash message")
		}

		stashes = append(stashes, &doltdb.Stash{
			Name:            id,
			BranchReference: fullBranch,
			Description:     msg,
			CommitHash:      stashHash,
		})
	}

	return stashes, nil
}

// generateStashSql returns the query that will call the `DOLT_STASH` stored procedure.
func generateStashSql(apr *argparser.ArgParseResults, subcommand string) (string, error) {
	var buffer bytes.Buffer
	var params []interface{}
	buffer.WriteString("CALL DOLT_STASH(?, ?")
	params = append(params, subcommand)
	params = append(params, doltdb.DoltCliRef)

	if len(apr.Args) == 2 {
		params = append(params, apr.Arg(1))
		buffer.WriteString(", ?")
	}

	if apr.Contains(cli.AllFlag) {
		buffer.WriteString(", '--all'")
	}
	if apr.Contains(cli.IncludeUntrackedFlag) {
		buffer.WriteString(", '--include-untracked'")
	}

	buffer.WriteString(")")
	interpolatedQuery, err := dbr.InterpolateForDialect(buffer.String(), params, dialect.MySQL)
	return interpolatedQuery, err
}

func stashQuery(queryist cli.Queryist, sqlCtx *sql.Context, apr *argparser.ArgParseResults, subcommand string) (sql.RowIter, error) {
	interpolatedQuery, err := generateStashSql(apr, subcommand)
	if err != nil {
		return nil, err
	}

	_, rowIter, _, err := queryist.Query(sqlCtx, interpolatedQuery)
	if err != nil {
		return nil, err
	}

	return rowIter, nil
}

func parseStashIndex(stashID string) (int, error) {
	var err error
	stashID = strings.TrimSuffix(strings.TrimPrefix(stashID, "stash@{"), "}")
	idx, err := strconv.Atoi(stashID)
	if err != nil {
		return 0, fmt.Errorf("error: %s is not a valid reference", stashID)
	}

	return idx, nil
}
