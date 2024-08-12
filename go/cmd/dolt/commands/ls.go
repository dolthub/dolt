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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var lsDocs = cli.CommandDocumentationContent{
	ShortDesc: "List tables",
	LongDesc: `With no arguments lists the tables in the current working set but if a commit is specified it will list the tables in that commit.  If the {{.EmphasisLeft}}--verbose{{.EmphasisRight}} flag is provided a row count of the table will also be displayed.

If the {{.EmphasisLeft}}--system{{.EmphasisRight}} flag is supplied this will show the dolt system tables which are queryable with SQL.

If the {{.EmphasisLeft}}--all{{.EmphasisRight}} flag is supplied both user and system tables will be printed.
`,

	Synopsis: []string{
		"[--options] [{{.LessThan}}commit{{.GreaterThan}}]",
	},
}

type LsCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd LsCmd) Name() string {
	return "ls"
}

// Description returns a description of the command
func (cmd LsCmd) Description() string {
	return "List tables in the working set."
}

func (cmd LsCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(lsDocs, ap)
}

func (cmd LsCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	ap.SupportsFlag(cli.VerboseFlag, "v", "show the hash of the table and row count")
	ap.SupportsFlag(cli.SystemFlag, "s", "show system tables")
	ap.SupportsFlag(cli.AllFlag, "a", "show user and system tables")
	return ap
}

// EventType returns the type of the event to log
func (cmd LsCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_LS
}

// Exec executes the command
func (cmd LsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, lsDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.Contains(cli.SystemFlag) && apr.Contains(cli.AllFlag) {
		verr := errhand.BuildDError("--%s and --%s are mutually exclusive", cli.SystemFlag, cli.AllFlag).SetPrintUsage().Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	query, err := constructInterpolatedDoltLsQuery(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	_, _, _, err = queryist.Query(sqlCtx, "set @@dolt_show_system_tables = 1")
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	rows, err := GetRowsForSql(queryist, sqlCtx, query)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	_, _, _, err = queryist.Query(sqlCtx, "set @@dolt_show_system_tables = 0")
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	var userTables []string
	var systemTables []string
	for _, row := range rows {
		tableName := row[0].(string)
		if doltdb.HasDoltPrefix(tableName) {
			systemTables = append(systemTables, tableName)
		} else {
			userTables = append(userTables, tableName)
		}
	}

	if apr.Contains(cli.AllFlag) {
		err = printUserTables(userTables, apr, queryist, sqlCtx)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		err = printSystemTables(systemTables)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
	} else if apr.Contains(cli.SystemFlag) {
		err = printSystemTables(systemTables)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
	} else {
		err = printUserTables(userTables, apr, queryist, sqlCtx)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
	}

	return HandleVErrAndExitCode(nil, usage)
}

// constructInterpolatedDoltLsQuery generates the sql query necessary to list all tables and interpolates this query
// to prevent sql injection
func constructInterpolatedDoltLsQuery(apr *argparser.ArgParseResults) (string, error) {
	query := "show tables"
	if apr.NArg() == 1 {
		query += " as of ?"
		interpolatedQuery, err := dbr.InterpolateForDialect(query, []interface{}{apr.Arg(0)}, dialect.MySQL)
		if err != nil {
			return "", err
		}
		return interpolatedQuery, nil
	} else {
		return query, nil
	}
}

func printUserTables(tableNames []string, apr *argparser.ArgParseResults, queryist cli.Queryist, sqlCtx *sql.Context) error {
	var label string
	if apr.NArg() == 0 {
		label = "working set"
	} else {
		query := fmt.Sprintf("select hashof('%s')", apr.Arg(0))
		row, err := GetRowsForSql(queryist, sqlCtx, query)
		if err != nil {
			return err
		}
		label = row[0][0].(string)
	}

	if len(tableNames) == 0 {
		cli.Printf("No tables in %s\n", label)
		return nil
	}

	cli.Printf("Tables in %s:\n", label)
	for _, tbl := range tableNames {
		if apr.Contains(cli.VerboseFlag) {
			err := printTableVerbose(tbl, queryist, sqlCtx)
			if err != nil {
				return err
			}
		} else {
			cli.Println("\t", tbl)
		}
	}

	return nil
}

func printTableVerbose(table string, queryist cli.Queryist, sqlCtx *sql.Context) error {
	query := fmt.Sprintf("select count(*) from `%s`", table)
	row, err := GetRowsForSql(queryist, sqlCtx, query)
	if err != nil {
		return err
	}

	if cnt, ok := row[0][0].(int64); ok {
		cli.Println(fmt.Sprintf("\t%-20s     %d rows", table, cnt))
	} else if cnt, ok := row[0][0].(string); ok {
		// remote execution returns result as a string
		cli.Println(fmt.Sprintf("\t%-20s     %s rows", table, cnt))
	} else {
		return fmt.Errorf("unexpected type for count: %T", row[0][0])
	}

	return nil
}

func printSystemTables(tableNames []string) error {
	cli.Println("System tables:")
	for _, tbl := range tableNames {
		cli.Println("\t", tbl)
	}

	return nil
}
