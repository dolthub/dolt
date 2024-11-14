// Copyright 2024 Dolthub, Inc.
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

package expranalysis

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
)

// ResolveDefaultExpression returns a sql.Expression for the column default or generated expression for the
// column provided
func ResolveDefaultExpression(ctx *sql.Context, tableName string, sch schema.Schema, col schema.Column) (sql.Expression, error) {
	ct, err := parseCreateTable(ctx, tableName, sch)
	if err != nil {
		return nil, err
	}

	colIdx := ct.PkSchema().Schema.IndexOfColName(col.Name)
	if colIdx < 0 {
		return nil, fmt.Errorf("unable to find column %s in analyzed query", col.Name)
	}

	sqlCol := ct.PkSchema().Schema[colIdx]
	expr := sqlCol.Default
	if expr == nil || expr.Expr == nil {
		expr = sqlCol.Generated
	}

	if expr == nil || expr.Expr == nil {
		return nil, fmt.Errorf("unable to find default or generated expression")
	}

	return expr, nil
}

// ResolveCheckExpression returns a sql.Expression for the check provided
func ResolveCheckExpression(ctx *sql.Context, tableName string, sch schema.Schema, checkExpr string) (sql.Expression, error) {
	ct, err := parseCreateTable(ctx, tableName, sch)
	if err != nil {
		return nil, err
	}

	for _, check := range ct.Checks() {
		if stripTableNamesFromExpression(check.Expr).String() == checkExpr {
			return check.Expr, nil
		}
	}

	return nil, fmt.Errorf("unable to find check expression")
}

func stripTableNamesFromExpression(expr sql.Expression) sql.Expression {
	e, _, _ := transform.Expr(expr, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		if col, ok := e.(*expression.GetField); ok {
			return col.WithTable(""), transform.NewTree, nil
		}
		return e, transform.SameTree, nil
	})
	return e
}

func parseCreateTable(_ *sql.Context, tableName string, sch schema.Schema) (*plan.CreateTable, error) {
	createTable, err := sqlfmt.GenerateCreateTableStatement(tableName, sch, nil, nil)
	if err != nil {
		return nil, err
	}

	query := createTable

	mockDatabase := memory.NewDatabase("mydb")
	mockProvider := memory.NewDBProvider(mockDatabase)
	catalog := analyzer.NewCatalog(mockProvider)
	parseCtx := sql.NewEmptyContext()
	parseCtx.SetCurrentDatabase("mydb")

	b := planbuilder.New(parseCtx, catalog, nil, nil)
	pseudoAnalyzedQuery, _, _, _, err := b.Parse(query, nil, false)
	if err != nil {
		return nil, err
	}

	ct, ok := pseudoAnalyzedQuery.(*plan.CreateTable)
	if !ok {
		return nil, fmt.Errorf("expected a *plan.CreateTable node, but got %T", pseudoAnalyzedQuery)
	}
	return ct, nil
}
