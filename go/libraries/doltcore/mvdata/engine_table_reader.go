// Copyright 2021 Dolthub, Inc.
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

package mvdata

import (
	"context"
	"fmt"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

type sqlEngineTableReader struct {
	se     *engine.SqlEngine
	sqlCtx *sql.Context

	sch  schema.Schema
	iter sql.RowIter
}

func NewSqlEngineReader(ctx context.Context, dEnv *env.DoltEnv, tableName string) (*sqlEngineTableReader, error) {
	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv, false)
	if err != nil {
		return nil, err
	}

	config := &engine.SqlEngineConfig{
		ServerUser: "root",
		Autocommit: true,
	}
	se, err := engine.NewSqlEngine(
		ctx,
		mrEnv,
		config,
	)
	if err != nil {
		return nil, err
	}

	sqlCtx, err := se.NewLocalContext(ctx)
	if err != nil {
		return nil, err
	}
	sqlCtx.SetCurrentDatabase(mrEnv.GetFirstDatabase())

	sqlEngine := se.GetUnderlyingEngine()
	binder := planbuilder.New(sqlCtx, sqlEngine.Analyzer.Catalog, sqlEngine.Parser)
	ret, _, _, _, err := binder.Parse(fmt.Sprintf("show create table `%s`", tableName), false)
	if err != nil {
		return nil, err
	}

	create, ok := ret.(*plan.ShowCreateTable)
	if !ok {
		return nil, fmt.Errorf("expected *plan.ShowCreate table, found %T", ret)
	}

	_, iter, _, err := se.Query(sqlCtx, fmt.Sprintf("SELECT * FROM `%s`", tableName))
	if err != nil {
		return nil, err
	}

	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, err
	}

	doltSchema, err := sqlutil.ToDoltSchema(ctx, root, tableName, create.PrimaryKeySchema, nil, sql.Collation_Default)
	if err != nil {
		return nil, err
	}

	return &sqlEngineTableReader{
		se:     se,
		sqlCtx: sqlCtx,

		sch:  doltSchema,
		iter: iter,
	}, nil
}

// Used by Dolthub API
func NewSqlEngineTableReaderWithEngine(sqlCtx *sql.Context, se *sqle.Engine, db dsqle.Database, root doltdb.RootValue, tableName string) (*sqlEngineTableReader, error) {
	sch, iter, _, err := se.Query(sqlCtx, fmt.Sprintf("SELECT * FROM `%s`", tableName))
	if err != nil {
		return nil, err
	}

	doltSchema, err := sqlutil.ToDoltSchema(sqlCtx, root, tableName, sql.NewPrimaryKeySchema(sch), nil, sql.Collation_Default)
	if err != nil {
		return nil, err
	}

	return &sqlEngineTableReader{
		se:     engine.NewRebasedSqlEngine(se, map[string]dsess.SqlDatabase{db.Name(): db}),
		sqlCtx: sqlCtx,
		sch:    doltSchema,
		iter:   iter,
	}, nil
}

func (s *sqlEngineTableReader) GetSchema() schema.Schema {
	return s.sch
}

func (s *sqlEngineTableReader) ReadRow(ctx context.Context) (row.Row, error) {
	panic("deprecated")
}

func (s *sqlEngineTableReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	next, err := s.iter.Next(s.sqlCtx)
	if err != nil {
		return nil, err
	}

	return next, nil
}

func (s *sqlEngineTableReader) Close(ctx context.Context) error {
	return s.iter.Close(s.sqlCtx)
}
