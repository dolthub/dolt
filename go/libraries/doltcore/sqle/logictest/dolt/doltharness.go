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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	dsql "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statsnoms"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro"
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

func (h *DoltHarness) Close() {
	dbs := h.sess.Provider().AllDatabases(sql.NewEmptyContext())
	for _, db := range dbs {
		db.(dsess.SqlDatabase).DbData().Ddb.Close()
	}
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

	_, rowIter, _, err := h.engine.Query(ctx, statement)
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

	sch, rowIter, _, err = h.engine.Query(ctx, statement)
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

func (h *DoltHarness) GetTimeout() int64 {
	return 0
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
	var pro dsess.DoltDatabaseProvider
	h.engine, pro, err = sqlNewEngine(dEnv)

	if err != nil {
		return err
	}

	ctx := dsql.NewTestSQLCtxWithProvider(context.Background(), pro, statspro.NewProvider(pro.(*dsql.DoltDatabaseProvider), statsnoms.NewNomsStatsFactory(env.NewGRPCDialProviderFromDoltEnv(dEnv))), dsess.NewGCSafepointController())
	h.sess = ctx.Session.(*dsess.DoltSession)

	dbs := h.engine.Analyzer.Catalog.AllDatabases(ctx)
	var dbName string
	for _, db := range dbs {
		dsqlDB, ok := db.(dsql.Database)
		if !ok {
			continue
		}
		dbName = dsqlDB.Name()
		break
	}

	h.sess.SetCurrentDatabase(dbName)

	return nil
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
		res, _ := v.Float64()
		return fmt.Sprintf("%.3f", res)
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

func sqlNewEngine(dEnv *env.DoltEnv) (*sqle.Engine, dsess.DoltDatabaseProvider, error) {
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, nil, err
	}
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
	db, err := dsql.NewDatabase(context.Background(), "dolt", dEnv.DbData(), opts)
	if err != nil {
		return nil, nil, err
	}

	mrEnv, err := env.MultiEnvForDirectory(context.Background(), dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	if err != nil {
		return nil, nil, err
	}

	b := env.GetDefaultInitBranch(dEnv.Config)
	pro, err := dsql.NewDoltDatabaseProviderWithDatabase(b, mrEnv.FileSystem(), db, dEnv.FS)
	if err != nil {
		return nil, nil, err
	}

	pro = pro.WithDbFactoryUrl(doltdb.InMemDoltDB)

	engine := sqle.NewDefault(pro)

	return engine, pro, nil
}
