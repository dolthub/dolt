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
	sql2 "database/sql"
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// BuildConnectionStringQueryist returns a Queryist that connects to the server specified by the given server config. Presence in this
// module isn't ideal, but it's the only way to get the server config into the queryist.
func BuildConnectionStringQueryist(ctx context.Context, cwdFS filesys.Filesys, creds *cli.UserPassword, apr *argparser.ArgParseResults, host string, port int, useTLS bool, dbRev string) (cli.LateBindQueryist, error) {
	clientConfig, err := GetClientConfig(cwdFS, creds, apr)
	if err != nil {
		return nil, err
	}

	// ParseDSN currently doesn't support `/` in the db name
	dbName, _ := dsess.SplitRevisionDbName(dbRev)
	parsedMySQLConfig, err := mysql.ParseDSN(servercfg.ConnectionString(clientConfig, dbName))
	if err != nil {
		return nil, err
	}

	parsedMySQLConfig.DBName = dbRev
	parsedMySQLConfig.Addr = fmt.Sprintf("%s:%d", host, port)

	if useTLS {
		parsedMySQLConfig.TLSConfig = "true"
	}

	mysqlConnector, err := mysql.NewConnector(parsedMySQLConfig)
	if err != nil {
		return nil, err
	}

	conn := &dbr.Connection{DB: sql2.OpenDB(mysqlConnector), EventReceiver: nil, Dialect: dialect.MySQL}

	queryist := ConnectionQueryist{connection: conn}

	var lateBind cli.LateBindQueryist = func(ctx context.Context) (cli.Queryist, *sql.Context, func(), error) {
		sqlCtx := sql.NewContext(ctx)
		sqlCtx.SetCurrentDatabase(dbRev)
		return queryist, sqlCtx, func() { conn.Conn(ctx) }, nil
	}

	return lateBind, nil
}

// ConnectionQueryist executes queries by connecting to a running mySql server.
type ConnectionQueryist struct {
	connection *dbr.Connection
}

var _ cli.Queryist = ConnectionQueryist{}

func (c ConnectionQueryist) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	rows, err := c.connection.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, nil, err
	}
	rowIter, err := NewMysqlRowWrapper(rows)
	if err != nil {
		return nil, nil, nil, err
	}
	return rowIter.Schema(), rowIter, nil, nil
}

func (c ConnectionQueryist) QueryWithBindings(ctx *sql.Context, query string, _ sqlparser.Statement, _ map[string]*querypb.BindVariable, _ *sql.QueryFlags) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	return c.Query(ctx, query)
}

type MysqlRowWrapper struct {
	rows     *sql2.Rows
	schema   sql.Schema
	finished bool
	vRow     []*string
	iRow     []interface{}
}

var _ sql.RowIter = (*MysqlRowWrapper)(nil)

func NewMysqlRowWrapper(rows *sql2.Rows) (*MysqlRowWrapper, error) {
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
