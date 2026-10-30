// Copyright 2026 Dolthub, Inc.
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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	// Necessary for the empty context used below to be initialized with system vars.
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

// recordingQueryist records the statements it is asked to run.
type recordingQueryist struct {
	queries []string
}

func (q *recordingQueryist) Query(_ *sql.Context, query string) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	q.queries = append(q.queries, query)
	return nil, sql.RowsToRowIter(), nil, nil
}

func (q *recordingQueryist) QueryWithBindings(ctx *sql.Context, query string, _ sqlparser.Statement, _ map[string]sqlparser.Expr, _ *sql.QueryFlags) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	return q.Query(ctx, query)
}

// serverQueryist is a recording client that is connected to a SQL server.
type serverQueryist struct {
	*recordingQueryist
}

func (serverQueryist) SupportsLoadDataLocal() bool { return true }

func TestProcessParsedQueryLoadDataLocal(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/10636
	const query = "load data local infile 'in.csv' into table t"
	ctx := sql.NewEmptyContext()
	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)
	load, ok := stmt.(*sqlparser.Load)
	require.True(t, ok)
	require.True(t, bool(load.Local))
	require.Equal(t, "in.csv", load.Infile)

	embedded := &recordingQueryist{}
	_, _, _, err = processParsedQuery(ctx, query, embedded, stmt)
	require.EqualError(t, err, "LOCAL supported only in sql-server mode")
	require.Empty(t, embedded.queries)

	remote := &recordingQueryist{}
	_, _, _, err = processParsedQuery(ctx, query, serverQueryist{remote}, stmt)
	require.NoError(t, err)
	require.Equal(t, []string{query}, remote.queries)

	const serverQuery = "load data infile 'in.csv' into table t"
	serverStmt, err := sqlparser.Parse(serverQuery)
	require.NoError(t, err)
	embedded.queries = nil
	_, _, _, err = processParsedQuery(ctx, serverQuery, embedded, serverStmt)
	require.NoError(t, err)
	require.Equal(t, []string{serverQuery}, embedded.queries)
}
