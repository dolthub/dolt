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

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/gcctx"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// ExecuteSql executes all the SQL non-select statements given in the string against the root value given and returns
// the updated root, or an error. Statements in the input string are split by `;\n`
func ExecuteSql(ctx context.Context, dEnv *env.DoltEnv, statements string) (doltdb.RootValue, error) {
	db, err := NewDatabase(context.Background(), "dolt", dEnv.DbData(ctx), editor.Options{})
	if err != nil {
		return nil, err
	}

	engine, sqlCtx, err := NewTestEngine(dEnv, context.Background(), db)
	if err != nil {
		return nil, err
	}

	err = ExecuteSqlOnEngine(sqlCtx, engine, statements)
	if err != nil {
		return nil, err
	}
	return db.GetRoot(sqlCtx)
}

func ExecuteSqlOnEngine(ctx *sql.Context, engine *sqle.Engine, statements string) error {
	err := ctx.Session.SetSessionVariable(ctx, sql.AutoCommitSessionVar, false)
	if err != nil {
		return err
	}

	for _, query := range strings.Split(statements, ";\n") {
		if len(strings.Trim(query, " ")) == 0 {
			continue
		}

		sqlStatement, err := sqlparser.Parse(query)
		if err != nil {
			return err
		}

		var execErr error
		switch sqlStatement.(type) {
		case *sqlparser.Select, *sqlparser.OtherRead:
			return errors.New("Select statements aren't handled")
		case *sqlparser.Insert:
			var rowIter sql.RowIter
			_, rowIter, _, execErr = engine.Query(ctx, query)
			if execErr == nil {
				execErr = drainIter(ctx, rowIter)
			}
		case *sqlparser.DDL, *sqlparser.AlterTable, *sqlparser.DBDDL:
			var rowIter sql.RowIter
			_, rowIter, _, execErr = engine.Query(ctx, query)
			if execErr == nil {
				execErr = drainIter(ctx, rowIter)
			}
		default:
			return fmt.Errorf("Unsupported SQL statement: '%v'.", query)
		}

		if execErr != nil {
			return execErr
		}
	}

	// commit leftover transaction
	trx := ctx.GetTransaction()
	if trx != nil {
		err = dsess.DSessFromSess(ctx.Session).CommitTransaction(ctx, trx)
		if err != nil {
			return err
		}
	}

	return nil
}

func NewTestSQLCtxWithProvider(ctx context.Context, pro dsess.DoltDatabaseProvider, config config.ReadWriteConfig, statsPro sql.StatsProvider, gcSafepointController *gcctx.GCSafepointController) *sql.Context {
	s, err := dsess.NewDoltSession(sql.NewBaseSession(), pro, config, branch_control.CreateDefaultController(ctx), statsPro, writer.NewWriteSession, gcSafepointController, nil)
	if err != nil {
		panic(err)
	}

	s.SetCurrentDatabase("dolt")
	return sql.NewContext(
		ctx,
		sql.WithSession(s),
	)
}

// NewTestEngine creates a new default engine, and a *sql.Context and initializes indexes and schema fragments.
func NewTestEngine(dEnv *env.DoltEnv, ctx context.Context, db dsess.SqlDatabase) (*sqle.Engine, *sql.Context, error) {
	b := env.GetDefaultInitBranch(dEnv.Config)
	pro, err := NewDoltDatabaseProviderWithDatabase(b, dEnv.FS, db, dEnv.FS, sql.EngineOverrides{})
	if err != nil {
		return nil, nil, err
	}
	gcSafepointController := gcctx.NewGCSafepointController()

	engine := sqle.NewDefault(pro)

	config, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	sqlCtx := NewTestSQLCtxWithProvider(ctx, pro, config, nil, gcSafepointController)
	sqlCtx.SetCurrentDatabase(db.Name())
	return engine, sqlCtx, nil
}

// ExecuteSelect executes the select statement given and returns the resulting rows, or an error if one is encountered.
func ExecuteSelect(ctx context.Context, dEnv *env.DoltEnv, root doltdb.RootValue, query string) ([]sql.Row, error) {
	dbData := env.DbData[context.Context]{
		Ddb: dEnv.DoltDB(ctx),
		Rsw: dEnv.RepoStateWriter(),
		Rsr: dEnv.RepoStateReader(),
	}

	db, err := NewDatabase(context.Background(), "dolt", dbData, editor.Options{})
	if err != nil {
		return nil, err
	}

	engine, sqlCtx, err := NewTestEngine(dEnv, context.Background(), db)
	if err != nil {
		return nil, err
	}

	_, rowIter, _, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, err
	}

	var (
		rows   []sql.Row
		rowErr error
		row    sql.Row
	)
	for row, rowErr = rowIter.Next(sqlCtx); rowErr == nil; row, rowErr = rowIter.Next(sqlCtx) {
		rows = append(rows, row)
	}

	if rowErr != io.EOF {
		return nil, rowErr
	}

	return rows, nil
}

func drainIter(ctx *sql.Context, iter sql.RowIter) error {
	for {
		_, err := iter.Next(ctx)
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

func CreateEnvWithSeedData() (*env.DoltEnv, error) {
	const seedData = `
	CREATE TABLE people (
	    id varchar(36) primary key,
	    name varchar(40) not null,
	    age int unsigned,
	    is_married int,
	    title varchar(40),
	    INDEX idx_name (name)
	);
	INSERT INTO people VALUES
		('00000000-0000-0000-0000-000000000000', 'Bill Billerson', 32, 1, 'Senior Dufus'),
		('00000000-0000-0000-0000-000000000001', 'John Johnson', 25, 0, 'Dufus'),
		('00000000-0000-0000-0000-000000000002', 'Rob Robertson', 21, 0, '');`

	ctx := context.Background()
	dEnv := CreateTestEnv()
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, err
	}

	root, err = ExecuteSql(ctx, dEnv, seedData)
	if err != nil {
		return nil, err
	}

	err = dEnv.UpdateWorkingRoot(ctx, root)
	if err != nil {
		return nil, err
	}

	return dEnv, nil
}

const (
	TestHomeDirPrefix = "/user/dolt/"
	WorkingDirPrefix  = "/user/dolt/datasets/"
)

// CreateTestEnv creates a new DoltEnv suitable for testing. The CreateTestEnvWithName
// function should generally be preferred over this method, especially when working
// with tests using multiple databases within a MultiRepoEnv.
func CreateTestEnv() *env.DoltEnv {
	return CreateTestEnvWithName("test")
}

// CreateTestEnvWithName creates a new DoltEnv suitable for testing and uses
// the specified name to distinguish it from other test envs. This function
// should generally be preferred over CreateTestEnv, especially when working with
// tests using multiple databases within a MultiRepoEnv.
func CreateTestEnvWithName(envName string) *env.DoltEnv {
	const name = "billy bob"
	const email = "bigbillieb@fake.horse"
	initialDirs := []string{TestHomeDirPrefix + envName, WorkingDirPrefix + envName}
	homeDirFunc := func() (string, error) { return TestHomeDirPrefix + envName, nil }
	fs := filesys.NewInMemFS(initialDirs, nil, WorkingDirPrefix+envName)
	dEnv := env.Load(context.Background(), homeDirFunc, fs, doltdb.InMemDoltDB+envName, "test")
	cfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	cfg.SetStrings(map[string]string{
		config.UserNameKey:  name,
		config.UserEmailKey: email,
	})

	err := dEnv.InitRepo(context.Background(), types.Format_Default, name, email, env.DefaultInitBranch)

	if err != nil {
		panic("Failed to initialize environment:" + err.Error())
	}

	return dEnv
}

func SqlRowsFromDurableIndex(idx durable.Index, sch schema.Schema) ([]sql.Row, error) {
	ctx := context.Background()
	var sqlRows []sql.Row
	rowData, err := durable.ProllyMapFromIndex(idx)
	if err != nil {
		return nil, err
	}
	kd, vd := rowData.Descriptors()
	iter, err := rowData.IterAll(ctx)
	if err != nil {
		return nil, err
	}
	for {
		var k, v val.Tuple
		k, v, err = iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		sqlRow, err := sqlRowFromTuples(sch, kd, vd, k, v)
		if err != nil {
			return nil, err
		}
		sqlRows = append(sqlRows, sqlRow)
	}

	return sqlRows, nil
}

func sqlRowFromTuples(sch schema.Schema, kd, vd *val.TupleDesc, k, v val.Tuple) (sql.Row, error) {
	var err error
	ctx := context.Background()
	r := make(sql.Row, sch.GetAllCols().Size())
	keyless := schema.IsKeyless(sch)

	for i, col := range sch.GetAllCols().GetColumns() {
		pos, ok := sch.GetPKCols().TagToIdx[col.Tag]
		if ok {
			r[i], err = tree.GetField(ctx, kd, pos, k, nil)
			if err != nil {
				return nil, err
			}
		}

		pos, ok = sch.GetNonPKCols().TagToIdx[col.Tag]
		if keyless {
			pos += 1 // compensate for cardinality field
		}
		if ok {
			r[i], err = tree.GetField(ctx, vd, pos, v, nil)
			if err != nil {
				return nil, err
			}
		}
	}
	return r, nil
}
