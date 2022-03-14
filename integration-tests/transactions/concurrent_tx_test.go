// Copyright 2022 Dolthub, Inc.
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

package transactions

import (
	"fmt"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var defaultConfig = ServerConfig{
	database: "mysql",
	host:     "127.0.0.1",
	port:     3316,
	user:     "root",
	password: "toor",
}

func TestConcurrentTransactions(t *testing.T) {
	sequential := &sync.Mutex{}
	for _, test := range txTests {
		t.Run(test.name, func(t *testing.T) {
			sequential.Lock()
			defer sequential.Unlock()
			testConcurrentTx(t, test)
		})
	}
}

type ConcurrentTxTest struct {
	name    string
	queries []concurrentQuery
}

type concurrentQuery struct {
	conn      string
	query     string
	assertion selector
	expected  []testRow
}

type selector func(s *dbr.Session) *dbr.SelectStmt

type testRow struct {
	Pk, C0 int
}

const (
	one = "one"
	two = "two"
)

var txTests = []ConcurrentTxTest{
	{
		name: "smoke test",
		queries: []concurrentQuery{
			{
				conn: one,
				assertion: func(s *dbr.Session) *dbr.SelectStmt {
					return s.Select("*").From("tx.data")
				},
				expected: []testRow{
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
		},
	},
	{
		name: "concurrent transactions",
		queries: []concurrentQuery{
			{
				conn:  one,
				query: "BEGIN;",
			},
			{
				conn:  two,
				query: "BEGIN;",
			},
			{
				conn: two,
				assertion: func(s *dbr.Session) *dbr.SelectStmt {
					return s.Select("*").From("tx.data")
				},
				expected: []testRow{
					{1, 1}, {2, 2}, {3, 3},
				},
			},
			{
				conn:  one,
				query: "INSERT INTO tx.data VALUES (4,4)",
			},
			{
				conn: two,
				assertion: func(s *dbr.Session) *dbr.SelectStmt {
					return s.Select("*").From("tx.data")
				},
				expected: []testRow{
					{1, 1}, {2, 2}, {3, 3},
				},
			},
			{
				conn:  one,
				query: "COMMIT",
			},
			{
				conn: two,
				assertion: func(s *dbr.Session) *dbr.SelectStmt {
					return s.Select("*").From("tx.data")
				},
				expected: []testRow{
					{1, 1}, {2, 2}, {3, 3},
				},
			},
			{
				conn:  two,
				query: "COMMIT",
			},
			{
				conn: two,
				assertion: func(s *dbr.Session) *dbr.SelectStmt {
					return s.Select("*").From("tx.data")
				},
				expected: []testRow{
					{1, 1}, {2, 2}, {3, 3}, {4, 4},
				},
			},
		},
	},
	{
		name: "concurrent updates",
		queries: []concurrentQuery{
			{
				conn:  one,
				query: "BEGIN;",
			},
			{
				conn:  two,
				query: "BEGIN;",
			},
			{
				conn: two,
				assertion: func(s *dbr.Session) *dbr.SelectStmt {
					return s.Select("*").From("tx.data").Where("pk = 1")
				},
				expected: []testRow{
					{1, 1},
				},
			},
			{
				conn:  one,
				query: "UPDATE tx.data SET c0 = c0 + 10 WHERE pk = 1;",
			},
			{
				conn: two,
				assertion: func(s *dbr.Session) *dbr.SelectStmt {
					return s.Select("*").From("tx.data").Where("pk = 1")
				},
				expected: []testRow{
					{1, 1},
				},
			},
			{
				conn:  one,
				query: "COMMIT",
			},
			{
				conn: two,
				assertion: func(s *dbr.Session) *dbr.SelectStmt {
					return s.Select("*").From("tx.data").Where("pk = 1")
				},
				expected: []testRow{
					{1, 1},
				},
			},
			{
				conn:  two,
				query: "UPDATE tx.data SET c0 = c0 + 10 WHERE pk = 1;",
			},
			{
				conn: two,
				assertion: func(s *dbr.Session) *dbr.SelectStmt {
					return s.Select("*").From("tx.data").Where("pk = 1")
				},
				expected: []testRow{
					{1, 21},
				},
			},
			{
				conn:  two,
				query: "COMMIT",
			},
		},
	},
}

func setupCommon(sess *dbr.Session) (err error) {
	queries := []string{
		"DROP DATABASE IF EXISTS tx;",
		"CREATE DATABASE IF NOT EXISTS tx;",
		"USE tx;",
		"CREATE TABLE data (pk int primary key, c0 int);",
		"INSERT INTO data VALUES (1,1),(2,2),(3,3);",
	}

	for _, q := range queries {
		if _, err = sess.Exec(q); err != nil {
			return
		}
	}
	return
}

func testConcurrentTx(t *testing.T, test ConcurrentTxTest) {
	conns, err := createNamedConnections(defaultConfig, one, two)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeNamedConnections(conns)) }()

	err = setupCommon(conns[one])
	defer func() { require.NoError(t, teardownCommon(conns[one])) }()

	for _, q := range test.queries {
		conn := conns[q.conn]
		if q.query != "" {
			_, err = conn.Exec(q.query)
			require.NoError(t, err)
		}

		if q.assertion == nil {
			continue
		}

		var actual []testRow
		_, err = q.assertion(conn).Load(&actual)
		require.NoError(t, err)
		assert.Equal(t, q.expected, actual)
	}
}

func teardownCommon(sess *dbr.Session) (err error) {
	_, err = sess.Exec("DROP DATABASE tx;")
	return
}

type ServerConfig struct {
	database string
	host     string
	port     int
	user     string
	password string
}

type namedConnections map[string]*dbr.Session

// ConnectionString returns a Data Source Name (DSN) to be used by go clients for connecting to a running server.
func ConnectionString(config ServerConfig) string {
	return fmt.Sprintf("%v:%v@tcp(%v:%v)/%s",
		config.user,
		config.password,
		config.host,
		config.port,
		config.database,
	)
}

func createNamedConnections(config ServerConfig, names ...string) (nc namedConnections, err error) {
	nc = make(namedConnections, len(names))
	for _, name := range names {
		var c *dbr.Connection
		if c, err = dbr.Open("mysql", ConnectionString(config), nil); err != nil {
			return nil, err
		}
		nc[name] = c.NewSession(nil)
	}
	return
}

func closeNamedConnections(nc namedConnections) (err error) {
	for _, conn := range nc {
		if err = conn.Close(); err != nil {
			return
		}
	}
	return
}
