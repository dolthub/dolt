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
	"fmt"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/tracing"
)

// These functions cannot be in the sqlfmt package as the reliance on the sqle package creates a circular reference.

func PrepareCreateTableStmt(ctx context.Context, sqlDb sql.Database) (*sql.Context, *sqle.Engine, *dsess.Session) {
	sess := dsess.DefaultSession()
	sqlCtx := sql.NewContext(ctx,
		sql.WithSession(sess),
		sql.WithTracer(tracing.Tracer(ctx)))

	var cfg config.ReadableConfig = nil
	pro := NewDoltDatabaseProvider(cfg, nil, sqlDb)
	engine := sqle.NewDefault(pro)
	sqlCtx.SetCurrentDatabase(sqlDb.Name())
	return sqlCtx, engine, sess
}

func GetCreateTableStmt(ctx *sql.Context, engine *sqle.Engine, tableName string) (string, error) {
	_, rowIter, err := engine.Query(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`;", tableName))
	if err != nil {
		return "", err
	}
	rows, err := sql.RowIterToRows(ctx, nil, rowIter)
	if err != nil {
		return "", err
	}
	if len(rows) != 1 || len(rows[0]) != 2 {
		return "", fmt.Errorf("unexpected result from SHOW CREATE TABLE")
	}
	stmt, ok := rows[0][1].(string)
	if !ok {
		return "", fmt.Errorf("expected string statement from SHOW CREATE TABLE")
	}
	return stmt + ";", nil
}
