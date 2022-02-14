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

package dolt

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"time"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/sqllogictest/go/logictest"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	dsql "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

var _ logictest.Harness = &DoltHarness{}

const (
	name  = "sqllogictest runner"
	email = "sqllogictestrunner@dolthub.com"
)

type DoltHarness struct {
	Version string
	engine  *sqle.Engine
	sess    *dsess.DoltSession
}

func (h *DoltHarness) EngineStr() string {
	return "mysql"
}

func (h *DoltHarness) Init() error {
	dEnv := env.Load(context.Background(), env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, "test")
	return innerInit(h, dEnv)
}

func (h *DoltHarness) ExecuteStatement(statement string) error {
	ctx := sql.NewContext(
		context.Background(),
		sql.WithPid(rand.Uint64()),
		sql.WithSession(h.sess))

	_, rowIter, err := h.engine.Query(ctx, statement)
	if err != nil {
		return err
	}

	return drainIterator(ctx, rowIter)
}

func (h *DoltHarness) ExecuteQuery(statement string) (schema string, results []string, err error) {
	pid := rand.Uint32()
	ctx := sql.NewContext(
		context.Background(),
		sql.WithPid(uint64(pid)),
		sql.WithSession(h.sess))

	var sch sql.Schema
	var rowIter sql.RowIter
	defer func() {
		if r := recover(); r != nil {
			// Panics leave the engine in a bad state that we have to clean up
			h.engine.ProcessList.Kill(pid)
			panic(r)
		}
	}()

	sch, rowIter, err = h.engine.Query(ctx, statement)
	if err != nil {
		return "", nil, err
	}

	schemaString, err := schemaToSchemaString(sch)
	if err != nil {
		return "", nil, err
	}

	results, err = rowsToResultStrings(ctx, rowIter)
	if err != nil {
		return "", nil, err
	}

	return schemaString, results, nil
}

func innerInit(h *DoltHarness, dEnv *env.DoltEnv) error {
	if !dEnv.HasDoltDir() {
		err := dEnv.InitRepoWithTime(context.Background(), types.Format_Default, name, email, env.DefaultInitBranch, time.Now())
		if err != nil {
			return err
		}
	} else {
		err := dEnv.InitDBAndRepoState(context.Background(), types.Format_Default, name, email, env.DefaultInitBranch, time.Now())
		if err != nil {
			return err
		}
	}

	var err error
	h.engine, err = sqlNewEngine(dEnv)

	if err != nil {
		return err
	}

	ctx := dsql.NewTestSQLCtx(context.Background())
	h.sess = ctx.Session.(*dsess.DoltSession)

	dbs := h.engine.Analyzer.Catalog.AllDatabases(ctx)
	dsqlDBs := make([]dsql.Database, len(dbs))
	for i, db := range dbs {
		dsqlDB := db.(dsql.Database)
		dsqlDBs[i] = dsqlDB

		sess := dsess.DSessFromSess(ctx.Session)
		err := sess.AddDB(ctx, getDbState(db, dEnv))
		if err != nil {
			return err
		}

		root, verr := commands.GetWorkingWithVErr(dEnv)
		if verr != nil {
			return verr
		}

		err = dsqlDB.SetRoot(ctx, root)
		if err != nil {
			return err
		}
	}

	if len(dbs) == 1 {
		h.sess.SetCurrentDatabase(dbs[0].Name())
	}

	return nil
}

func getDbState(db sql.Database, dEnv *env.DoltEnv) dsess.InitialDbState {
	ctx := context.Background()

	head := dEnv.RepoStateReader().CWBHeadSpec()
	headCommit, err := dEnv.DoltDB.Resolve(ctx, head, dEnv.RepoStateReader().CWBHeadRef())
	if err != nil {
		panic(err)
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		panic(err)
	}

	return dsess.InitialDbState{
		Db:         db,
		HeadCommit: headCommit,
		WorkingSet: ws,
		DbData:     dEnv.DbData(),
		Remotes:    dEnv.RepoState.Remotes,
	}
}

func drainIterator(ctx *sql.Context, iter sql.RowIter) error {
	if iter == nil {
		return nil
	}

	for {
		_, err := iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	return iter.Close(ctx)
}

// This shouldn't be necessary -- the fact that an iterator can return an error but not clean up after itself in all
// cases is a bug.
func drainIteratorIgnoreErrors(ctx *sql.Context, iter sql.RowIter) {
	if iter == nil {
		return
	}

	for {
		_, err := iter.Next(ctx)
		if err == io.EOF {
			return
		}
	}
}

// Returns the rows in the iterator given as an array of their string representations, as expected by the test files
func rowsToResultStrings(ctx *sql.Context, iter sql.RowIter) ([]string, error) {
	var results []string
	if iter == nil {
		return results, nil
	}

	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			return results, nil
		} else if err != nil {
			drainIteratorIgnoreErrors(ctx, iter)
			return nil, err
		} else {
			for _, col := range row {
				results = append(results, toSqlString(col))
			}
		}
	}
}

func toSqlString(val interface{}) string {
	if val == nil {
		return "NULL"
	}

	switch v := val.(type) {
	case float32, float64:
		// exactly 3 decimal points for floats
		return fmt.Sprintf("%.3f", v)
	case decimal.Decimal:
		// exactly 3 decimal points for floats
		return v.StringFixed(3)
	case int:
		return strconv.Itoa(v)
	case uint:
		return strconv.Itoa(int(v))
	case int8:
		return strconv.Itoa(int(v))
	case uint8:
		return strconv.Itoa(int(v))
	case int16:
		return strconv.Itoa(int(v))
	case uint16:
		return strconv.Itoa(int(v))
	case int32:
		return strconv.Itoa(int(v))
	case uint32:
		return strconv.Itoa(int(v))
	case int64:
		return strconv.Itoa(int(v))
	case uint64:
		return strconv.Itoa(int(v))
	case string:
		return v
	// Mysql returns 1 and 0 for boolean values, mimic that
	case bool:
		if v {
			return "1"
		} else {
			return "0"
		}
	default:
		panic(fmt.Sprintf("No conversion for value %v of type %T", val, val))
	}
}

func schemaToSchemaString(sch sql.Schema) (string, error) {
	b := strings.Builder{}
	for _, col := range sch {
		switch col.Type.Type() {
		case query.Type_INT8, query.Type_INT16, query.Type_INT24, query.Type_INT32, query.Type_INT64,
			query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64,
			query.Type_BIT:
			b.WriteString("I")
		case query.Type_TEXT, query.Type_VARCHAR:
			b.WriteString("T")
		case query.Type_FLOAT32, query.Type_FLOAT64, query.Type_DECIMAL:
			b.WriteString("R")
		default:
			return "", fmt.Errorf("Unhandled type: %v", col.Type)
		}
	}
	return b.String(), nil
}

func sqlNewEngine(dEnv *env.DoltEnv) (*sqle.Engine, error) {
	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	db := dsql.NewDatabase("dolt", dEnv.DbData(), opts)
	mrEnv, err := env.DoltEnvAsMultiEnv(context.Background(), dEnv)
	if err != nil {
		return nil, err
	}

	pro := dsql.NewDoltDatabaseProvider(dEnv.Config, mrEnv.FileSystem(), db)
	pro = pro.WithDbFactoryUrl(doltdb.InMemDoltDB)

	engine := sqle.NewDefault(pro)

	return engine, nil
}
