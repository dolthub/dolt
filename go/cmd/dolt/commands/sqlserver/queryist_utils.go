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

package sqlserver

import (
	"context"
	"crypto/tls"
	sql2 "database/sql"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

type QueryistTLSMode int

const (
	QueryistTLSMode_Disabled QueryistTLSMode = iota
	// Require TLS, verify the server certificate using the system
	// trust store, do not allow fallback to plaintext.
	//
	// Used for `dolt --host ... sql ...` when `--no-tls-` is not
	// specified. Often used for connecting to Hosted DoltDB
	// instances using the CLI commands posted on
	// hosted.doltdb.com.
	QueryistTLSMode_Enabled
	// Used for local Dolt CLI queryist connecting to the running
	// local server. In this mode, TLS is allowed but not required
	// and the client does not verify the remote TLS
	// certificate. It is assumed connecting to the port locally
	// is secure and lands the client in the correct place, given
	// the contents of sql-server.info, for example.
	//
	// This mode still does not allow the Dolt CLI to connect to a
	// server which requires a client certificate.
	QueryistTLSMode_NoVerify_FallbackToPlaintext
)

// BuildConnectionStringQueryist returns a Queryist that connects to the server specified by the given server config. Presence in this
// module isn't ideal, but it's the only way to get the server config into the queryist.
func BuildConnectionStringQueryist(ctx context.Context, cwdFS filesys.Filesys, creds *cli.UserPassword, apr *argparser.ArgParseResults, host string, port int, tlsMode QueryistTLSMode, dbRev string) (cli.LateBindQueryist, error) {
	clientConfig, err := GetClientConfig(cwdFS, creds, apr)
	if err != nil {
		return nil, err
	}

	// ParseDSN currently doesn't support `/` in the db name
	dbName, _ := doltdb.SplitRevisionDbName(dbRev)
	parsedMySQLConfig, err := mysql.ParseDSN(servercfg.ConnectionString(clientConfig, dbName))
	if err != nil {
		return nil, err
	}

	parsedMySQLConfig.DBName = dbRev
	parsedMySQLConfig.Addr = fmt.Sprintf("%s:%d", host, port)

	switch tlsMode {
	case QueryistTLSMode_Disabled:
	case QueryistTLSMode_Enabled:
		parsedMySQLConfig.TLS = &tls.Config{}
	case QueryistTLSMode_NoVerify_FallbackToPlaintext:
		parsedMySQLConfig.TLS = &tls.Config{InsecureSkipVerify: true}
		parsedMySQLConfig.AllowFallbackToPlaintext = true
	}

	mysqlConnector, err := mysql.NewConnector(parsedMySQLConfig)
	if err != nil {
		return nil, err
	}

	conn := &dbr.Connection{DB: sql2.OpenDB(mysqlConnector), EventReceiver: nil, Dialect: dialect.MySQL}

	gatherWarnings := false
	queryist := ConnectionQueryist{connection: conn, gatherWarnings: &gatherWarnings}

	var lateBind cli.LateBindQueryist = func(ctx context.Context, opts ...cli.LateBindQueryistOption) (res cli.LateBindQueryistResult, err error) {
		sqlCtx := sql.NewContext(ctx)
		sqlCtx.SetCurrentDatabase(dbRev)

		if err := engine.InitCommitIdentitySessionVars(queryist, sqlCtx); err != nil {
			cli.PrintErr(err.Error())
		}

		res.Queryist = queryist
		res.Context = sqlCtx
		res.Closer = func() {
			conn.Close()
		}
		res.IsRemote = true
		return res, nil
	}

	return lateBind, nil
}

// ConnectionQueryist executes queries by connecting to a running mySql server.
type ConnectionQueryist struct {
	connection     *dbr.Connection
	gatherWarnings *bool
}

var _ cli.Queryist = &ConnectionQueryist{}

func (c ConnectionQueryist) EnableGatherWarnings() {
	*c.gatherWarnings = true
}

func (c ConnectionQueryist) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	rows, err := c.connection.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, nil, err
	}

	rowIter, err := NewMysqlRowWrapper(rows)
	if err != nil {
		return nil, nil, nil, err
	}

	if c.gatherWarnings != nil && *c.gatherWarnings == true {
		ctx.ClearWarnings()

		re := regexp.MustCompile(`\s+`)
		noSpace := strings.TrimSpace(re.ReplaceAllString(query, " "))
		isShowWarnings := strings.EqualFold(noSpace, "show warnings")

		if !isShowWarnings {
			warnRows, err := c.connection.QueryContext(ctx, "show warnings")
			if err != nil {
				return nil, nil, nil, err
			}

			for warnRows.Next() {
				var code int
				var msg string
				var level string

				err = warnRows.Scan(&level, &code, &msg)
				if err != nil {
					return nil, nil, nil, err
				}

				ctx.Warn(code, "%s", msg)
			}
		}
	}

	return rowIter.Schema(), rowIter, nil, nil
}

func (c ConnectionQueryist) QueryWithBindings(ctx *sql.Context, query string, _ sqlparser.Statement, _ map[string]sqlparser.Expr, _ *sql.QueryFlags) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	return c.Query(ctx, query)
}

type MysqlRowWrapper struct {
	rows    []sql.Row
	schema  sql.Schema
	numRows int
	curRow  int
}

var _ sql.RowIter = (*MysqlRowWrapper)(nil)

func NewMysqlRowWrapper(sqlRows *sql2.Rows) (*MysqlRowWrapper, error) {
	colTypes, err := sqlRows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	schema := make(sql.Schema, len(colTypes))
	vRow := make([]*string, len(colTypes))
	iRow := make([]interface{}, len(colTypes))
	rows := make([]sql.Row, 0)
	for i, colType := range colTypes {
		schema[i] = &sql.Column{
			Name:     colType.Name(),
			Type:     sqlutil.DatabaseTypeNameToSqlType(colType.DatabaseTypeName()),
			Nullable: true,
		}
		iRow[i] = &vRow[i]
	}

	for sqlRows.Next() {
		err := sqlRows.Scan(iRow...)
		if err != nil {
			return nil, err
		}
		sqlRow := make(sql.Row, len(vRow))
		for i, val := range vRow {
			if val != nil {
				sqlRow[i] = *val
			}
		}

		rows = append(rows, sqlRow)
	}

	closeErr := sqlRows.Close()
	if closeErr != nil {
		return nil, err
	}

	return &MysqlRowWrapper{
		rows:    rows,
		schema:  schema,
		numRows: len(rows),
		curRow:  0,
	}, nil
}

func (s *MysqlRowWrapper) Schema() sql.Schema {
	return s.schema
}

func (s *MysqlRowWrapper) Next(*sql.Context) (sql.Row, error) {
	if s.NoMoreRows() {
		return nil, io.EOF
	}

	s.curRow++
	return s.rows[s.curRow-1], nil
}

func (s *MysqlRowWrapper) NoMoreRows() bool {
	return s.curRow >= s.numRows
}

func (s *MysqlRowWrapper) Close(*sql.Context) error {
	s.curRow = s.numRows
	return nil
}
