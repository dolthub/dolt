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

package sqle

import (
	"context"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// Not an exhaustive test of views -- we rely on bats tests for end-to-end verification.
func TestViews(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()

	var err error
	_, err = ExecuteSql(ctx, dEnv, "create table test (a int primary key)")
	require.NoError(t, err)

	_, err = ExecuteSql(ctx, dEnv, "insert into test values (1), (2), (3)")
	require.NoError(t, err)

	_, err = ExecuteSql(ctx, dEnv, "create view plus1 as select a + 1 from test")
	require.NoError(t, err)

	expectedRows := []sql.Row{
		{int64(2)},
		{int64(3)},
		{int64(4)},
	}
	rows, _, err := executeSelect(t, ctx, dEnv, "select * from plus1")
	require.NoError(t, err)
	assert.Equal(t, expectedRows, rows)

	_, err = ExecuteSql(ctx, dEnv, "drop view plus1")
	require.NoError(t, err)
}

// Runs the query given and returns the result. The schema result of the query's execution is currently ignored, and
// the targetSchema given is used to prepare all rows.
func executeSelect(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, query string) ([]sql.Row, sql.Schema, error) {
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)

	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	if err != nil {
		return nil, nil, err
	}

	sch, iter, _, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, nil, err
	}

	sqlRows := make([]sql.Row, 0)
	var r sql.Row
	for r, err = iter.Next(sqlCtx); err == nil; r, err = iter.Next(sqlCtx) {
		sqlRows = append(sqlRows, r)
	}

	if err != io.EOF {
		return nil, nil, err
	}

	return sqlRows, sch, nil
}
