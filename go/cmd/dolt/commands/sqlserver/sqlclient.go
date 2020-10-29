// Copyright 2020 Liquidata, Inc.
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
	"path/filepath"
	"strings"
	"time"

	"github.com/abiosoft/readline"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/liquidata-inc/ishell"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

const (
	sqlClientDualFlag = "dual"
)

var sqlClientDocs = cli.CommandDocumentationContent{
	ShortDesc: "Starts a built-in MySQL client.",
	LongDesc: `Starts a MySQL client that is built into dolt. May connect to any database that supports MySQL connections, including dolt servers.

You may also start a dolt server and automatically connect to it using this client. Both the server and client will be a part of the same process. This is useful for testing behavior of the dolt server without the need for an external client, and is not recommended for general usage.

Similar to {{.EmphasisLeft}}dolt sql-server{{.EmphasisRight}}, this command may use a YAML configuration file or command line arguments. For more information on the YAML file, refer to the documentation on {{.EmphasisLeft}}dolt sql-server{{.EmphasisRight}}.`,
	Synopsis: []string{
		"[-d] --config {{.LessThan}}file{{.GreaterThan}}",
		"[-d] [-H {{.LessThan}}host{{.GreaterThan}}] [-P {{.LessThan}}port{{.GreaterThan}}] [-u {{.LessThan}}user{{.GreaterThan}}] [-p {{.LessThan}}password{{.GreaterThan}}] [-t {{.LessThan}}timeout{{.GreaterThan}}] [-l {{.LessThan}}loglevel{{.GreaterThan}}] [--multi-db-dir {{.LessThan}}directory{{.GreaterThan}}] [--query-parallelism {{.LessThan}}num-go-routines{{.GreaterThan}}] [-r]",
	},
}

type SqlClientCmd struct{}

var _ cli.Command = SqlClientCmd{}

func (cmd SqlClientCmd) Name() string {
	return "sql-client"
}

func (cmd SqlClientCmd) Description() string {
	return "Starts a built-in MySQL client."
}

func (cmd SqlClientCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, sqlClientDocs, ap))
}

func (cmd SqlClientCmd) createArgParser() *argparser.ArgParser {
	ap := SqlServerCmd{}.CreateArgParser()
	ap.SupportsFlag(sqlClientDualFlag, "d", "Causes this command to spawn a dolt server that is automatically connected to.")
	return ap
}

func (cmd SqlClientCmd) RequiresRepo() bool {
	return false
}

func (cmd SqlClientCmd) Hidden() bool {
	return true
}

func (cmd SqlClientCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, sqlClientDocs, ap))

	apr := cli.ParseArgs(ap, args, help)
	serverConfig, err := GetServerConfig(dEnv, apr)
	if err != nil {
		cli.PrintErrln(color.RedString("Bad Configuration"))
		cli.PrintErrln(err.Error())
		return 1
	}

	var serverController *ServerController

	if apr.Contains(sqlClientDualFlag) {
		cli.PrintErrf("Starting server with Config %v\n", ConfigInfo(serverConfig))

		serverController = CreateServerController()
		go func() {
			_, _ = Serve(ctx, SqlServerCmd{}.VersionStr, serverConfig, serverController, dEnv)
		}()
		err := serverController.WaitForStart()
		if err != nil {
			cli.PrintErrln(err.Error())
			return 1
		}
	}

	conn, err := dbr.Open("mysql", ConnectionString(serverConfig), nil)
	if err != nil {
		cli.PrintErrln(err.Error())
		serverController.StopServer()
		err = serverController.WaitForClose()
		if err != nil {
			cli.PrintErrln(err.Error())
		}
		return 1
	}
	_ = conn.Ping()
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
				err = commands.PrettyPrintResults(ctx, 0, wrapper.Schema(), wrapper)
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
	serverController.StopServer()
	err = serverController.WaitForClose()
	if err != nil {
		cli.PrintErrln(err.Error())
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
			Type:     sql.LongText,
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

func (s *MysqlRowWrapper) Next() (sql.Row, error) {
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

func (s *MysqlRowWrapper) Close() error {
	return s.rows.Close()
}
