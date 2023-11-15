// Copyright 2020 Dolthub, Inc.
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

package sqlserver

import (
	"context"
	mysql "database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/abiosoft/readline"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/ishell"
	"github.com/fatih/color"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/svcs"
)

const (
	sqlClientDualFlag     = "dual"
	SqlClientQueryFlag    = "query"
	sqlClientResultFormat = "result-format"
)

var sqlClientDocs = cli.CommandDocumentationContent{
	ShortDesc: "Starts a built-in MySQL client.",
	LongDesc: `Starts a MySQL client that is built into dolt. May connect to any database that supports MySQL connections, including dolt servers.

You may also start a dolt server and automatically connect to it using this client. Both the server and client will be a part of the same process. This is useful for testing behavior of the dolt server without the need for an external client, and is not recommended for general usage.

Similar to {{.EmphasisLeft}}dolt sql-server{{.EmphasisRight}}, this command may use a YAML configuration file or command line arguments. For more information on the YAML file, refer to the documentation on {{.EmphasisLeft}}dolt sql-server{{.EmphasisRight}}.`,
	Synopsis: []string{
		"[-d] --config {{.LessThan}}file{{.GreaterThan}}",
		"[-d] [-H {{.LessThan}}host{{.GreaterThan}}] [-P {{.LessThan}}port{{.GreaterThan}}] [-u {{.LessThan}}user{{.GreaterThan}}] [-p {{.LessThan}}password{{.GreaterThan}}] [-t {{.LessThan}}timeout{{.GreaterThan}}] [-l {{.LessThan}}loglevel{{.GreaterThan}}] [--data-dir {{.LessThan}}directory{{.GreaterThan}}] [--query-parallelism {{.LessThan}}num-go-routines{{.GreaterThan}}] [-r]",
		"-q {{.LessThan}}string{{.GreaterThan}} [--use-db {{.LessThan}}db_name{{.GreaterThan}}] [--result-format {{.LessThan}}format{{.GreaterThan}}] [-H {{.LessThan}}host{{.GreaterThan}}] [-P {{.LessThan}}port{{.GreaterThan}}] [-u {{.LessThan}}user{{.GreaterThan}}] [-p {{.LessThan}}password{{.GreaterThan}}]",
	},
}

type SqlClientCmd struct {
	VersionStr string
}

var _ cli.Command = SqlClientCmd{}

func (cmd SqlClientCmd) Name() string {
	return "sql-client"
}

func (cmd SqlClientCmd) Description() string {
	return "Starts a built-in MySQL client."
}

func (cmd SqlClientCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(sqlClientDocs, ap)
}

func (cmd SqlClientCmd) ArgParser() *argparser.ArgParser {
	ap := SqlServerCmd{}.ArgParserWithName(cmd.Name())
	ap.SupportsFlag(sqlClientDualFlag, "d", "Causes this command to spawn a dolt server that is automatically connected to.")
	ap.SupportsString(SqlClientQueryFlag, "q", "string", "Sends the given query to the server and immediately exits.")
	ap.SupportsString(commands.UseDbFlag, "", "db_name", fmt.Sprintf("Selects the given database before executing a query. "+
		"By default, uses the current folder's name. Must be used with the --%s flag.", SqlClientQueryFlag))
	ap.SupportsString(sqlClientResultFormat, "", "format", fmt.Sprintf("Returns the results in the given format. Must be used with the --%s flag.", SqlClientQueryFlag))
	return ap
}

func (cmd SqlClientCmd) RequiresRepo() bool {
	return false
}

func (cmd SqlClientCmd) Hidden() bool {
	return true
}

func (cmd SqlClientCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, sqlClientDocs, ap))

	apr := cli.ParseArgsOrDie(ap, args, help)
	var serverConfig ServerConfig
	var svcsController *svcs.Controller
	var err error

	cli.Println(color.YellowString("WARNING: This command is being deprecated and is not recommended for general use.\n" +
		"\t Use dolt sql or any compatible MySQL client instead."))

	if apr.Contains(sqlClientDualFlag) {
		if !dEnv.Valid() {
			if !cli.CheckEnvIsValid(dEnv) {
				return 2
			}

			cli.PrintErrln(color.RedString("--dual flag requires running within a dolt database directory"))
			cli.PrintErrln(err.Error())
			return 1
		}
		if apr.Contains(SqlClientQueryFlag) {
			cli.PrintErrln(color.RedString(fmt.Sprintf("--%s flag may not be used with --%s", sqlClientDualFlag, SqlClientQueryFlag)))
			return 1
		}
		if apr.Contains(commands.UseDbFlag) {
			cli.PrintErrln(color.RedString(fmt.Sprintf("--%s flag may not be used with --%s", sqlClientDualFlag, commands.UseDbFlag)))
			return 1
		}
		if apr.Contains(sqlClientResultFormat) {
			cli.PrintErrln(color.RedString(fmt.Sprintf("--%s flag may not be used with --%s", sqlClientDualFlag, sqlClientResultFormat)))
			return 1
		}

		serverConfig, err = GetServerConfig(dEnv.FS, apr)
		if err != nil {
			cli.PrintErrln(color.RedString("Bad Configuration"))
			cli.PrintErrln(err.Error())
			return 1
		}
		if err = SetupDoltConfig(dEnv, apr, serverConfig); err != nil {
			cli.PrintErrln(color.RedString("Bad Configuration"))
			cli.PrintErrln(err.Error())
			return 1
		}
		cli.PrintErrf("Starting server with Config %v\n", ConfigInfo(serverConfig))

		svcsController = svcs.NewController()
		go func() {
			_, _ = Serve(ctx, cmd.VersionStr, serverConfig, svcsController, dEnv)
		}()
		err = svcsController.WaitForStart()
		if err != nil {
			cli.PrintErrln(err.Error())
			return 1
		}
	} else {
		serverConfig, err = GetServerConfig(dEnv.FS, apr)
		if err != nil {
			cli.PrintErrln(color.RedString("Bad Configuration"))
			cli.PrintErrln(err.Error())
			return 1
		}
	}

	query, hasQuery := apr.GetValue(SqlClientQueryFlag)
	dbToUse, hasUseDb := apr.GetValue(commands.UseDbFlag)
	resultFormat, hasResultFormat := apr.GetValue(sqlClientResultFormat)
	if !hasQuery && hasUseDb {
		cli.PrintErrln(color.RedString(fmt.Sprintf("--%s may only be used with --%s", commands.UseDbFlag, SqlClientQueryFlag)))
		return 1
	} else if !hasQuery && hasResultFormat {
		cli.PrintErrln(color.RedString(fmt.Sprintf("--%s may only be used with --%s", commands.UseDbFlag, sqlClientResultFormat)))
		return 1
	}
	if !hasUseDb && hasQuery {
		directory, err := os.Getwd()
		if err != nil {
			cli.PrintErrln(color.RedString(err.Error()))
			return 1
		}
		dbToUse = strings.Replace(filepath.Base(directory), "-", "_", -1)
	}
	format := engine.FormatTabular
	if hasResultFormat {
		switch strings.ToLower(resultFormat) {
		case "tabular":
			format = engine.FormatTabular
		case "csv":
			format = engine.FormatCsv
		case "json":
			format = engine.FormatJson
		case "null":
			format = engine.FormatNull
		case "vertical":
			format = engine.FormatVertical
		default:
			cli.PrintErrln(color.RedString(fmt.Sprintf("unknown --%s value: %s", sqlClientResultFormat, resultFormat)))
			return 1
		}
	}

	// The standard DSN parser cannot handle a forward slash in the database name, so we have to workaround it.
	// See the original issue: https://github.com/dolthub/dolt/issues/4623
	parsedMySQLConfig, err := mysqlDriver.ParseDSN(ConnectionString(serverConfig, "no_database"))
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}

	if parsedMySQLConfig.User == "" {
		cli.PrintErrln(color.RedString("--user or -u argument is required"))
		return 1
	}

	parsedMySQLConfig.DBName = dbToUse
	mysqlConnector, err := mysqlDriver.NewConnector(parsedMySQLConfig)
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}
	conn := &dbr.Connection{DB: mysql.OpenDB(mysqlConnector), EventReceiver: nil, Dialect: dialect.MySQL}
	_ = conn.Ping()

	if hasQuery {
		defer conn.Close()

		if apr.Contains(noAutoCommitFlag) {
			_, err = conn.Exec("set @@autocommit = off;")
			if err != nil {
				cli.PrintErrln(err.Error())
				return 1
			}
		}

		scanner := commands.NewSqlStatementScanner(strings.NewReader(query))
		query = ""
		for scanner.Scan() {
			query += scanner.Text()
			if len(query) == 0 || query == "\n" {
				continue
			}

			rows, err := conn.Query(query)
			if err != nil {
				cli.PrintErrln(err.Error())
				return 1
			}
			if rows != nil {
				sqlCtx := sql.NewContext(ctx)
				wrapper, err := NewMysqlRowWrapper(rows)
				if err != nil {
					cli.PrintErrln(err.Error())
					return 1
				}
				defer wrapper.Close(sqlCtx)
				if wrapper.HasMoreRows() {
					err = engine.PrettyPrintResults(sqlCtx, format, wrapper.Schema(), wrapper)
					if err != nil {
						cli.PrintErrln(err.Error())
						return 1
					}
				}
			}
			query = ""
		}

		if err = scanner.Err(); err != nil {
			cli.PrintErrln(err.Error())
			return 1
		}
		return 0
	}

	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for range ticker.C {
			_ = conn.Ping()
		}
	}()

	_ = iohelp.WriteLine(cli.CliOut, `# Welcome to the Dolt MySQL client.
# Statements must be terminated with ';'.
# "exit" or "quit" (or Ctrl-D) to exit.`)
	historyFile := filepath.Join(".sqlhistory") // history file written to working dir
	prompt := "mysql> "
	multilinePrompt := fmt.Sprintf(fmt.Sprintf("%%%ds", len(prompt)), "-> ")

	rlConf := readline.Config{
		Prompt:                 prompt,
		Stdout:                 cli.CliOut,
		Stderr:                 cli.CliOut,
		HistoryFile:            historyFile,
		HistoryLimit:           500,
		HistorySearchFold:      true,
		DisableAutoSaveHistory: true,
	}
	shellConf := ishell.UninterpretedConfig{
		ReadlineConfig: &rlConf,
		QuitKeywords: []string{
			"quit", "exit", "quit()", "exit()",
		},
		LineTerminator: ";",
		MysqlShellCmds: []string{},
	}

	shell := ishell.NewUninterpreted(&shellConf)
	shell.SetMultiPrompt(multilinePrompt)

	shell.EOF(func(c *ishell.Context) {
		c.Stop()
	})

	shell.Interrupt(func(c *ishell.Context, count int, input string) {
		if count > 1 {
			c.Stop()
		} else {
			c.Println("Received SIGINT. Interrupt again to exit, or use ^D, quit, or exit.")
		}
	})

	shell.Uninterpreted(func(c *ishell.Context) {
		query := c.Args[0]
		if len(strings.TrimSpace(query)) == 0 {
			return
		}

		// grab time for query timing
		startTime := time.Now()

		rows, err := conn.Query(query)
		if err != nil {
			shell.Println(color.RedString(err.Error()))
			return
		}
		if rows != nil {
			wrapper, err := NewMysqlRowWrapper(rows)
			if err != nil {
				shell.Println(color.RedString(err.Error()))
				return
			}
			if wrapper.HasMoreRows() {
				sqlCtx := sql.NewContext(ctx)
				sqlCtx.SetQueryTime(startTime)
				err = engine.PrettyPrintResultsExtended(sqlCtx, engine.FormatTabular, wrapper.Schema(), wrapper)
				if err != nil {
					shell.Println(color.RedString(err.Error()))
					return
				}
			} else {
				err := iohelp.WriteLine(cli.CliOut, fmt.Sprintf("Query OK (%.2f sec)", secondsSince(startTime, time.Now())))
				if err != nil {
					shell.Println(color.RedString(err.Error()))
					return
				}
			}
		}

		// TODO: there's a bug in the readline library when editing multi-line history entries.
		singleLine := strings.ReplaceAll(query, "\n", " ")
		if err := shell.AddHistory(singleLine); err != nil {
			shell.Println(color.RedString(err.Error()))
		}
	})

	shell.Run()
	ticker.Stop()

	// everything beyond this point may error and the overall process is still a success, thus we return 0 regardless
	err = conn.Close()
	if err != nil {
		cli.PrintErrln(err.Error())
	}
	if apr.Contains(sqlClientDualFlag) {
		svcsController.Stop()
		err = svcsController.WaitForStop()
		if err != nil {
			cli.PrintErrln(err.Error())
		}
	}

	return 0
}

type MysqlRowWrapper struct {
	rows     *mysql.Rows
	schema   sql.Schema
	finished bool
	vRow     []*string
	iRow     []interface{}
}

var _ sql.RowIter = (*MysqlRowWrapper)(nil)

func NewMysqlRowWrapper(rows *mysql.Rows) (*MysqlRowWrapper, error) {
	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	schema := make(sql.Schema, len(colNames))
	vRow := make([]*string, len(colNames))
	iRow := make([]interface{}, len(colNames))
	for i, colName := range colNames {
		schema[i] = &sql.Column{
			Name:     colName,
			Type:     types.LongText,
			Nullable: true,
		}
		iRow[i] = &vRow[i]
	}
	return &MysqlRowWrapper{
		rows:     rows,
		schema:   schema,
		finished: !rows.Next(),
		vRow:     vRow,
		iRow:     iRow,
	}, nil
}

func (s *MysqlRowWrapper) Schema() sql.Schema {
	return s.schema
}

func (s *MysqlRowWrapper) Next(*sql.Context) (sql.Row, error) {
	if s.finished {
		return nil, io.EOF
	}
	err := s.rows.Scan(s.iRow...)
	if err != nil {
		return nil, err
	}
	sqlRow := make(sql.Row, len(s.vRow))
	for i, val := range s.vRow {
		if val != nil {
			sqlRow[i] = *val
		}
	}
	s.finished = !s.rows.Next()
	return sqlRow, nil
}

func (s *MysqlRowWrapper) HasMoreRows() bool {
	return !s.finished
}

func (s *MysqlRowWrapper) Close(*sql.Context) error {
	return s.rows.Close()
}

// secondsSince returns the number of full and partial seconds since the time given
func secondsSince(start time.Time, end time.Time) float64 {
	runTime := end.Sub(start)
	seconds := runTime / time.Second
	milliRemainder := (runTime - seconds*time.Second) / time.Millisecond
	timeDisplay := float64(seconds) + float64(milliRemainder)*.001
	return timeDisplay
}

// ConnectionQueryist executes queries by connecting to a running mySql server.
type ConnectionQueryist struct {
	connection *dbr.Connection
}

var _ cli.Queryist = ConnectionQueryist{}

func (c ConnectionQueryist) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	rows, err := c.connection.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	rowIter, err := NewMysqlRowWrapper(rows)
	if err != nil {
		return nil, nil, err
	}
	return rowIter.Schema(), rowIter, nil
}

// BuildConnectionStringQueryist returns a Queryist that connects to the server specified by the given server config. Presence in this
// module isn't ideal, but it's the only way to get the server config into the queryist.
func BuildConnectionStringQueryist(ctx context.Context, cwdFS filesys.Filesys, creds *cli.UserPassword, apr *argparser.ArgParseResults, host string, port int, useTLS bool, dbRev string) (cli.LateBindQueryist, error) {
	clientConfig, err := GetClientConfig(cwdFS, creds, apr)
	if err != nil {
		return nil, err
	}

	// ParseDSN currently doesn't support `/` in the db name
	dbName, _ := dsess.SplitRevisionDbName(dbRev)
	parsedMySQLConfig, err := mysqlDriver.ParseDSN(ConnectionString(clientConfig, dbName))
	if err != nil {
		return nil, err
	}

	parsedMySQLConfig.DBName = dbRev
	parsedMySQLConfig.Addr = fmt.Sprintf("%s:%d", host, port)

	if useTLS {
		parsedMySQLConfig.TLSConfig = "true"
	}

	mysqlConnector, err := mysqlDriver.NewConnector(parsedMySQLConfig)
	if err != nil {
		return nil, err
	}

	conn := &dbr.Connection{DB: mysql.OpenDB(mysqlConnector), EventReceiver: nil, Dialect: dialect.MySQL}

	queryist := ConnectionQueryist{connection: conn}

	var lateBind cli.LateBindQueryist = func(ctx context.Context) (cli.Queryist, *sql.Context, func(), error) {
		sqlCtx := sql.NewContext(ctx)
		sqlCtx.SetCurrentDatabase(dbRev)
		return queryist, sqlCtx, func() { conn.Conn(ctx) }, nil
	}

	return lateBind, nil
}
