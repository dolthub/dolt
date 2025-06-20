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
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var stashDocs = cli.CommandDocumentationContent{
	ShortDesc: "Stash the changes in a dirty working directory away.",
	LongDesc: `Use dolt stash when you want to record the current state of the working directory and the index, but want to go back to a clean working directory. 

The command saves your local modifications away and reverts the working directory to match the HEAD commit. The stash entries that are saved away can be listed with 'dolt stash list'.
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
	return "Stash the changes in a dirty working directory away."
}

func (cmd StashCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(stashDocs, ap)
}

func (cmd StashCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsFlag(cli.IncludeUntrackedFlag, "u", "Untracked tables are also stashed.")
	ap.SupportsFlag(cli.AllFlag, "a", "All tables are stashed, including untracked and ignored tables.")
	return ap
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

	var err error
	switch subcommand {
	case "push":
		err = stashPush(ctx, cliCtx, apr, subcommand)
	case "pop", "drop":
		err = stashRemove(ctx, cliCtx, apr, subcommand)
	case "list":
		err = stashList(ctx, cliCtx)
	case "clear":
		err = stashClear(ctx, cliCtx, apr, subcommand)
	default:
		err = fmt.Errorf("unknown stash subcommand %s", subcommand)
	}

	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}
	return 0
}

func stashPush(ctx context.Context, cliCtx cli.CliContext, apr *argparser.ArgParseResults, subcommand string) error {
	rowIter, queryist, sqlCtx, closeFunc, err := stashQuery(ctx, cliCtx, apr, subcommand)
	if err != nil {
		return err
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	stashes, err := getStashesSQL(ctx, sqlCtx, queryist, 1)
	if err != nil {
		return err
	}
	stash := stashes[0]
	stashHash, err := stash.HeadCommit.HashOf()
	if err != nil {
		return err
	}
	hashStr := stashHash.String()

	cli.Println(fmt.Sprintf("Saved working directory and index state WIP on %s: %s %s", stash.BranchReference, hashStr, stash.Description))
	_, err = sql.RowIterToRows(sqlCtx, rowIter)
	return err
}

func stashRemove(ctx context.Context, cliCtx cli.CliContext, apr *argparser.ArgParseResults, subcommand string) error {
	idx, err := parseStashIndex(apr)
	if err != nil {
		return err
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return err
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	stashes, err := getStashesSQL(ctx, sqlCtx, queryist, 0)
	if err != nil {
		return err
	}
	if len(stashes) == 0 {
		return fmt.Errorf("No stash entries found.")
	}
	if len(stashes)-1 < idx {
		return fmt.Errorf("stash index stash@{%d} does not exist", idx)
	}
	commit := stashes[idx].HeadCommit
	stashHash, err := commit.HashOf()
	if err != nil {
		return err
	}

	qry, params := generateStashSql(apr, subcommand)
	interpolatedQuery, err := dbr.InterpolateForDialect(qry, params, dialect.MySQL)
	if err != nil {
		return err
	}
	_, rowIter, _, err := queryist.Query(sqlCtx, interpolatedQuery)
	if err != nil {
		return err
	}

	if subcommand == "pop" {
		var dEnv *env.DoltEnv
		ret := StatusCmd{}.Exec(sqlCtx, "status", []string{}, dEnv, cliCtx)
		if ret != 0 {
			cli.Println("The stash entry is kept in case you need it again.")
			return err
		}
	}

	cli.Println(fmt.Sprintf("Dropped refs/stash@{%v} (%s)", idx, stashHash.String()))
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

	stashes, err := getStashesSQL(ctx, sqlCtx, queryist, 0)
	for _, stash := range stashes {
		commitHash, err := stash.HeadCommit.HashOf()
		if err != nil {
			return err
		}
		cli.Println(fmt.Sprintf("%s: WIP on %s: %s %s", stash.Name, stash.BranchReference, commitHash.String(), stash.Description))
	}

	return nil
}

func stashClear(ctx context.Context, cliCtx cli.CliContext, apr *argparser.ArgParseResults, subcommand string) error {
	rowIter, _, sqlCtx, closeFunc, err := stashQuery(ctx, cliCtx, apr, subcommand)
	if err != nil {
		return err
	}
	if closeFunc != nil {
		defer closeFunc()
	}
	_, err = sql.RowIterToRows(sqlCtx, rowIter)
	return err
}

// getStashesSQL queries the dolt_stashes system table to return the requested number of stashes. A limit of 0 will get all stashes
func getStashesSQL(ctx context.Context, sqlCtx *sql.Context, queryist cli.Queryist, limit int) ([]*doltdb.Stash, error) {
	sess := dsess.DSessFromSess(sqlCtx.Session)
	dbName := sqlCtx.GetCurrentDatabase()
	doltDb, ok := sess.GetDbData(sqlCtx, dbName)
	if !ok {
		return nil, fmt.Errorf("No doltdb found for %s", dbName)
	}
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
		fullHash, ok := hash.MaybeParse(stashHash)
		if !ok {
			return nil, fmt.Errorf("invalid stash hash")
		}

		var commit *doltdb.Commit
		optCommit, err := doltDb.Ddb.ReadCommit(ctx, fullHash)
		if err == nil {
			commit, ok = optCommit.ToCommit()
		}

		msg, ok := s[3].(string)
		if !ok {
			return nil, fmt.Errorf("invalid stash message")
		}

		stashes = append(stashes, &doltdb.Stash{
			Name:            id,
			BranchReference: fullBranch,
			Description:     msg,
			HeadCommit:      commit,
		})
	}

	return stashes, nil
}

// generateStashSql returns the query that will call the `DOLT_STASH` stored proceudre.
func generateStashSql(apr *argparser.ArgParseResults, subcommand string) (string, []interface{}) {
	var buffer bytes.Buffer
	var params []interface{}
	var param bool
	first := true
	buffer.WriteString("CALL DOLT_STASH(")

	write := func(s string) {
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

	param = true
	write("?")
	param = true
	write("?")
	params = append(params, subcommand)
	params = append(params, doltdb.DoltCliRef)

	if len(apr.Args) == 2 {
		param = true
		params = append(params, apr.Arg(1))
		write("?")
	}

	if apr.Contains(cli.AllFlag) {
		write("-a")
	}
	if apr.Contains(cli.IncludeUntrackedFlag) {
		write("-u")
	}

	buffer.WriteString(")")
	return buffer.String(), params
}

func stashQuery(ctx context.Context, cliCtx cli.CliContext, apr *argparser.ArgParseResults, subcommand string) (sql.RowIter, cli.Queryist, *sql.Context, func(), error) {
	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	qry, params := generateStashSql(apr, subcommand)
	interpolatedQuery, err := dbr.InterpolateForDialect(qry, params, dialect.MySQL)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	_, rowIter, _, err := queryist.Query(sqlCtx, interpolatedQuery)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return rowIter, queryist, sqlCtx, closeFunc, nil
}

func parseStashIndex(apr *argparser.ArgParseResults) (int, error) {
	idx := 0

	if apr.NArg() > 1 {
		stashID := apr.Arg(1)
		var err error

		stashID = strings.TrimSuffix(strings.TrimPrefix(stashID, "stash@{"), "}")
		idx, err = strconv.Atoi(stashID)
		if err != nil {
			return 0, fmt.Errorf("error: %s is not a valid reference", stashID)
		}
	}

	return idx, nil
}
