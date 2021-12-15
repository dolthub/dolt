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
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	config2 "github.com/dolthub/dolt/go/libraries/utils/config"
)

// ExecuteSql executes all the SQL non-select statements given in the string against the root value given and returns
// the updated root, or an error. Statements in the input string are split by `;\n`
func ExecuteSql(t *testing.T, dEnv *env.DoltEnv, root *doltdb.RootValue, statements string) (*doltdb.RootValue, error) {
	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	db := NewDatabase("dolt", dEnv.DbData(), opts)

	engine, ctx, err := NewTestEngine(t, dEnv, context.Background(), db, root)
	dsess.DSessFromSess(ctx.Session).EnableBatchedMode()
	err = ctx.Session.SetSessionVariable(ctx, sql.AutoCommitSessionVar, false)
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
				execErr = drainIter(ctx, rowIter)
			}
		case *sqlparser.DDL, *sqlparser.MultiAlterDDL:
			var rowIter sql.RowIter
			_, rowIter, execErr = engine.Query(ctx, query)
			if execErr == nil {
				execErr = drainIter(ctx, rowIter)
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

	err = db.CommitTransaction(ctx, ctx.GetTransaction())
	if err != nil {
		return nil, err
	}

	return db.GetRoot(ctx)
}

// NewTestSQLCtx returns a new *sql.Context with a default DoltSession, a new IndexRegistry, and a new ViewRegistry
func NewTestSQLCtx(ctx context.Context) *sql.Context {
	session := dsess.DefaultSession()
	dsess := session.NewDoltSession(config2.NewMapConfig(make(map[string]string)))
	sqlCtx := sql.NewContext(
		ctx,
		sql.WithSession(dsess),
	).WithCurrentDB("dolt")

	return sqlCtx
}

// NewTestEngine creates a new default engine, and a *sql.Context and initializes indexes and schema fragments.
func NewTestEngine(t *testing.T, dEnv *env.DoltEnv, ctx context.Context, db Database, root *doltdb.RootValue) (*sqle.Engine, *sql.Context, error) {
	engine := sqle.NewDefault(NewDoltDatabaseProvider(dEnv.Config, dEnv.FS, db))

	sqlCtx := NewTestSQLCtx(ctx)

	err := dsess.DSessFromSess(sqlCtx.Session).AddDB(sqlCtx, getDbState(t, db, dEnv))
	if err != nil {
		return nil, nil, err
	}

	sqlCtx.SetCurrentDatabase(db.Name())
	err = db.SetRoot(sqlCtx, root)
	if err != nil {
		return nil, nil, err
	}

	return engine, sqlCtx, nil
}

func getDbState(t *testing.T, db sql.Database, dEnv *env.DoltEnv) dsess.InitialDbState {
	ctx := context.Background()

	head := dEnv.RepoStateReader().CWBHeadSpec()
	headCommit, err := dEnv.DoltDB.Resolve(ctx, head, dEnv.RepoStateReader().CWBHeadRef())
	require.NoError(t, err)

	ws, err := dEnv.WorkingSet(ctx)
	require.NoError(t, err)

	return dsess.InitialDbState{
		Db:         db,
		HeadCommit: headCommit,
		WorkingSet: ws,
		DbData:     dEnv.DbData(),
		Remotes:    dEnv.RepoState.Remotes,
	}
}

// ExecuteSelect executes the select statement given and returns the resulting rows, or an error if one is encountered.
func ExecuteSelect(t *testing.T, dEnv *env.DoltEnv, ddb *doltdb.DoltDB, root *doltdb.RootValue, query string) ([]sql.Row, error) {

	dbData := env.DbData{
		Ddb: ddb,
		Rsw: dEnv.RepoStateWriter(),
		Rsr: dEnv.RepoStateReader(),
		Drw: dEnv.DocsReadWriter(),
	}

	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	db := NewDatabase("dolt", dbData, opts)
	engine, ctx, err := NewTestEngine(t, dEnv, context.Background(), db, root)
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

// DoltSchemaFromAlterableTable is a utility for integration tests
func DoltSchemaFromAlterableTable(t *AlterableDoltTable) schema.Schema {
	return t.sch
}

// DoltTableFromAlterableTable is a utility for integration tests
func DoltTableFromAlterableTable(ctx *sql.Context, t *AlterableDoltTable) *doltdb.Table {
	dt, err := t.doltTable(ctx)
	if err != nil {
		panic(err)
	}
	return dt
}

func drainIter(ctx *sql.Context, iter sql.RowIter) error {
	for {
		_, err := iter.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			closeErr := iter.Close(ctx)
			if closeErr != nil {
				panic(fmt.Errorf("%v\n%v", err, closeErr))
			}
			return err
		}
	}
	return iter.Close(ctx)
}
