// Copyright 2019 Liquidata, Inc.
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

	sqle "github.com/liquidata-inc/go-mysql-server"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/sqllogictest/go/logictest"
	"github.com/liquidata-inc/vitess/go/vt/proto/query"
	"github.com/liquidata-inc/vitess/go/vt/sqlparser"
	"github.com/shopspring/decimal"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	dsql "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

var _ logictest.Harness = &DoltHarness{}

type DoltHarness struct {
	Version string
	engine  *sqle.Engine

	sess    *dsql.DoltSession
	idxReg  *sql.IndexRegistry
	viewReg *sql.ViewRegistry
}

func (h *DoltHarness) EngineStr() string {
	return "mysql"
}

func (h *DoltHarness) Init() error {
	dEnv := env.Load(context.Background(), env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, "test")
	if !dEnv.HasDoltDir() {
		panic("Current directory must be a valid dolt repository")
	}

	return innerInit(h, dEnv)
}

func (h *DoltHarness) ExecuteStatement(statement string) error {
	ctx := sql.NewContext(
		context.Background(),
		sql.WithPid(rand.Uint64()),
		sql.WithIndexRegistry(h.idxReg),
		sql.WithViewRegistry(h.viewReg),
		sql.WithSession(h.sess))

	statement = normalizeStatement(statement)

	_, rowIter, err := h.engine.Query(ctx, statement)
	if err != nil {
		return err
	}

	return drainIterator(rowIter)
}

func (h *DoltHarness) ExecuteQuery(statement string) (schema string, results []string, err error) {
	pid := rand.Uint32()
	ctx := sql.NewContext(
		context.Background(),
		sql.WithPid(uint64(pid)),
		sql.WithIndexRegistry(h.idxReg),
		sql.WithViewRegistry(h.viewReg),
		sql.WithSession(h.sess))

	var sch sql.Schema
	var rowIter sql.RowIter
	defer func() {
		if r := recover(); r != nil {
			// Panics leave the engine in a bad state that we have to clean up
			h.engine.Catalog.ProcessList.Kill(pid)
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

	results, err = rowsToResultStrings(rowIter)
	if err != nil {
		return "", nil, err
	}

	return schemaString, results, nil
}

func innerInit(h *DoltHarness, dEnv *env.DoltEnv) error {
	var err error
	h.engine, err = sqlNewEngine(dEnv)

	if err != nil {
		return err
	}

	h.sess = dsql.DefaultDoltSession()
	h.idxReg = sql.NewIndexRegistry()
	h.viewReg = sql.NewViewRegistry()

	ctx := sql.NewContext(
		context.Background(),
		sql.WithIndexRegistry(h.idxReg),
		sql.WithViewRegistry(h.viewReg),
		sql.WithSession(h.sess))

	dbs := h.engine.Catalog.AllDatabases()
	dsqlDBs := make([]dsql.Database, len(dbs))
	for i, db := range dbs {
		dsqlDB := db.(dsql.Database)
		dsqlDBs[i] = dsqlDB

		sess := dsql.DSessFromSess(ctx.Session)
		err := sess.AddDB(ctx, dsqlDB)

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

		err = dsql.RegisterSchemaFragments(ctx, dsqlDB, root)

		if err != nil {
			return err
		}
	}

	if len(dbs) == 1 {
		h.sess.SetCurrentDatabase(dbs[0].Name())
	}

	return nil
}

// We cheat a little at these tests. A great many of them use tables without primary keys, which we don't currently
// support. Until we do, we just make every column in such tables part of the primary key.
func normalizeStatement(statement string) string {
	if !strings.Contains(statement, "CREATE TABLE") {
		return statement
	}
	if strings.Contains(statement, "PRIMARY KEY") {
		return statement
	}

	stmt, err := sqlparser.Parse(statement)
	if err != nil {
		panic(err)
	}
	create, ok := stmt.(*sqlparser.DDL)
	if !ok {
		panic("Expected CREATE TABLE statement")
	}

	lastParen := strings.LastIndex(statement, ")")
	normalized := statement[:lastParen] + ", PRIMARY KEY ("
	for i, column := range create.TableSpec.Columns {
		normalized += column.Name.String()
		if i != len(create.TableSpec.Columns)-1 {
			normalized += ", "
		}
	}
	normalized += "))"
	return normalized
}

func drainIterator(iter sql.RowIter) error {
	if iter == nil {
		return nil
	}

	for {
		_, err := iter.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}
}

// This shouldn't be necessary -- the fact that an iterator can return an error but not clean up after itself in all
// cases is a bug.
func drainIteratorIgnoreErrors(iter sql.RowIter) {
	if iter == nil {
		return
	}

	for {
		_, err := iter.Next()
		if err == io.EOF {
			return
		}
	}
}

// Returns the rows in the iterator given as an array of their string representations, as expected by the test files
func rowsToResultStrings(iter sql.RowIter) ([]string, error) {
	var results []string
	if iter == nil {
		return results, nil
	}

	for {
		row, err := iter.Next()
		if err == io.EOF {
			return results, nil
		} else if err != nil {
			drainIteratorIgnoreErrors(iter)
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

func resetEnv(root *doltdb.RootValue) *doltdb.RootValue {
	tableNames, err := root.GetTableNames(context.Background())
	if err != nil {
		panic(err)
	}
	newRoot, err := root.RemoveTables(context.Background(), tableNames...)
	if err != nil {
		panic(err)
	}
	return newRoot
}

func sqlNewEngine(dEnv *env.DoltEnv) (*sqle.Engine, error) {
	db := dsql.NewDatabase("dolt", dEnv.DoltDB, dEnv.RepoState, dEnv.RepoStateWriter())
	engine := sqle.NewDefault()
	engine.AddDatabase(db)

	return engine, nil
}
