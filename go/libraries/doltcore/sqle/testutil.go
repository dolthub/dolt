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

package sqle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

// Executes all the SQL non-select statements given in the string against the root value given and returns the updated
// root, or an error. Statements in the input string are split by `;\n`
func ExecuteSql(dEnv *env.DoltEnv, root *doltdb.RootValue, statements string) (*doltdb.RootValue, error) {
	db := NewBatchedDatabase("dolt", dEnv.DoltDB, dEnv.RepoStateReader(), dEnv.RepoStateWriter())
	engine, ctx, err := NewTestEngine(context.Background(), db, root)

	if err != nil {
		return nil, err
	}

	for _, query := range strings.Split(statements, ";\n") {
		if len(strings.Trim(query, " ")) == 0 {
			continue
		}

		sqlStatement, err := sqlparser.Parse(query)
		if err != nil {
			return nil, err
		}

		var execErr error
		switch sqlStatement.(type) {
		case *sqlparser.Show:
			return nil, errors.New("Show statements aren't handled")
		case *sqlparser.Select, *sqlparser.OtherRead:
			return nil, errors.New("Select statements aren't handled")
		case *sqlparser.Insert:
			var rowIter sql.RowIter
			_, rowIter, execErr = engine.Query(ctx, query)
			if execErr == nil {
				execErr = drainIter(rowIter)
			}
		case *sqlparser.DDL:
			var rowIter sql.RowIter
			_, rowIter, execErr = engine.Query(ctx, query)
			if execErr == nil {
				execErr = drainIter(rowIter)
			}
			if err = db.Flush(ctx); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("Unsupported SQL statement: '%v'.", query)
		}

		if execErr != nil {
			return nil, execErr
		}
	}

	if err := db.Flush(ctx); err == nil {
		return db.GetRoot(ctx)
	} else {
		return nil, err
	}
}

// NewTestSQLCtx returns a new *sql.Context with a default DoltSession, a new IndexRegistry, and a new ViewRegistry
func NewTestSQLCtx(ctx context.Context) *sql.Context {
	sqlCtx := sql.NewContext(
		ctx,
		sql.WithSession(DefaultDoltSession()),
		sql.WithIndexRegistry(sql.NewIndexRegistry()),
		sql.WithViewRegistry(sql.NewViewRegistry()),
	).WithCurrentDB("dolt")

	return sqlCtx
}

// NewTestEngine creates a new default engine, and a *sql.Context and initializes indexes and schema fragments.
func NewTestEngine(ctx context.Context, db Database, root *doltdb.RootValue) (*sqle.Engine, *sql.Context, error) {
	engine := sqle.NewDefault()
	engine.AddDatabase(db)

	sqlCtx := NewTestSQLCtx(ctx)
	DSessFromSess(sqlCtx.Session).AddDB(ctx, db)
	sqlCtx.SetCurrentDatabase(db.Name())
	err := db.SetRoot(sqlCtx, root)

	if err != nil {
		return nil, nil, err
	}

	err = RegisterSchemaFragments(sqlCtx, db, root)

	if err != nil {
		return nil, nil, err
	}

	return engine, sqlCtx, nil
}

// Executes the select statement given and returns the resulting rows, or an error if one is encountered.
// This uses the index functionality, which is not ready for prime time. Use with caution.
func ExecuteSelect(dEnv *env.DoltEnv, ddb *doltdb.DoltDB, root *doltdb.RootValue, query string) ([]sql.Row, error) {
	db := NewDatabase("dolt", ddb, dEnv.RepoStateReader(), dEnv.RepoStateWriter())
	engine, ctx, err := NewTestEngine(context.Background(), db, root)
	if err != nil {
		return nil, err
	}

	_, rowIter, err := engine.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	var (
		rows   []sql.Row
		rowErr error
		row    sql.Row
	)
	for row, rowErr = rowIter.Next(); rowErr == nil; row, rowErr = rowIter.Next() {
		rows = append(rows, row)
	}

	if rowErr != io.EOF {
		return nil, rowErr
	}

	return rows, nil
}

func drainIter(iter sql.RowIter) error {
	for {
		_, err := iter.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	return iter.Close()
}
