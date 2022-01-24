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

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

type sqlEngineTableReader struct {
	se     *engine.SqlEngine
	sqlCtx *sql.Context

	sch  schema.Schema
	iter sql.RowIter
}

func NewSqlEngineReader(ctx context.Context, dEnv *env.DoltEnv, tableName string) (*sqlEngineTableReader, error) {
	mrEnv, err := env.DoltEnvAsMultiEnv(ctx, dEnv)
	if err != nil {
		return nil, err
	}

	// Choose the first DB as the current one. This will be the DB in the working dir if there was one there
	var dbName string
	mrEnv.Iter(func(name string, _ *env.DoltEnv) (stop bool, err error) {
		dbName = name
		return true, nil
	})

	se, err := engine.NewSqlEngine(ctx, mrEnv, engine.FormatCsv, dbName, false, nil, false)
	if err != nil {
		return nil, err
	}

	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return nil, err
	}

	sch, iter, err := se.Query(sqlCtx, fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		return nil, err
	}

	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, err
	}

	doltSchema, err := sqlutil.ToDoltSchema(ctx, root, tableName, sql.NewPrimaryKeySchema(sch), nil)
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
func NewSqlEngineTableReaderWithEngine(sqlCtx *sql.Context, se *sqle.Engine, db dsqle.Database, root *doltdb.RootValue, tableName string) (*sqlEngineTableReader, error) {
	sch, iter, err := se.Query(sqlCtx, fmt.Sprintf("SELECT * FROM `%s`", tableName))
	if err != nil {
		return nil, err
	}

	doltSchema, err := sqlutil.ToDoltSchema(sqlCtx, root, tableName, sql.NewPrimaryKeySchema(sch), nil)
	if err != nil {
		return nil, err
	}

	return &sqlEngineTableReader{
		se:     engine.NewRebasedSqlEngine(se, map[string]dsqle.SqlDatabase{db.Name(): db}),
		sqlCtx: sqlCtx,

		sch:  doltSchema,
		iter: iter,
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
