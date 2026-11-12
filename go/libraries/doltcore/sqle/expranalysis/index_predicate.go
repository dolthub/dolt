// Copyright 2026 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/overrides"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

// ResolveIndexPredicate returns a sql.Expression for the predicate of the partial index |idx|, or nil if |idx| is not
// a partial index. The predicate is resolved against |sch| rather than the session's view of the table, so it can be
// evaluated against rows built from |sch|, e.g. by index.BuildRow, even when |sch| is not the schema of any table the
// session can see, as during a merge or while ALTER TABLE rewrites a table.
func ResolveIndexPredicate(ctx *sql.Context, tableName string, sch schema.Schema, idx schema.Index) (sql.Expression, error) {
	if idx.Predicate() == "" {
		return nil, nil
	}

	// The predicate is planned against a stand-in table with the name and columns of the table of |idx|. Its columns
	// take their types from |sch| directly, rather than by parsing type names, so that types defined in the database,
	// like Doltgres enums and domains, resolve as they do for the real table.
	pkSch, err := sqlutil.FromDoltSchema(ctx, "", tableName, sch)
	if err != nil {
		return nil, err
	}

	// Every database of the stand-in provider holds the stand-in table, but planning still requires a current database.
	// Without one, plan in a session of our own rather than select a database in the caller's.
	planCtx := ctx
	if ctx.GetCurrentDatabase() == "" {
		planCtx = sql.NewContext(ctx)
		if vars := ctx.EngineVariables(); vars != nil {
			planCtx = sql.NewContextWithVariables(ctx, vars)
		}
		planCtx.SetCurrentDatabase("predicate_db")
	}

	catalog := analyzer.NewCatalog(standInProvider{table: standInTable{name: tableName, sch: pkSch.Schema}}, overrides.EngineOverridesFromContext(ctx))
	catalog.AuthHandler = sql.NoopAuthorizationHandler{}
	b := planbuilder.New(planCtx, catalog, nil)
	query := fmt.Sprintf("SELECT %s FROM %s", idx.Predicate(), overrides.SchemaFormatterFromContext(ctx).QuoteIdentifier(tableName))
	parsed, _, _, _, err := b.Parse(query, nil, false)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve predicate of partial index %s: %w", idx.Name(), err)
	}

	proj, ok := parsed.(*plan.Project)
	if !ok || len(proj.Projections) != 1 {
		return nil, fmt.Errorf("unable to resolve predicate of partial index %s", idx.Name())
	}
	expr := proj.Projections[0]
	if alias, ok := expr.(*expression.Alias); ok {
		expr = alias.Child
	}

	// The planner identifies columns by their ids in the query, so point them at their positions in |sch| instead.
	expr, _, err = transform.Expr(ctx, expr, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		gf, ok := e.(*expression.GetField)
		if !ok {
			return e, transform.SameTree, nil
		}
		colIdx := pkSch.Schema.IndexOfColName(gf.Name())
		if colIdx < 0 {
			return nil, transform.SameTree, fmt.Errorf("unable to find column %s of the predicate of partial index %s", gf.Name(), idx.Name())
		}
		return gf.WithIndex(colIdx), transform.NewTree, nil
	})
	if err != nil {
		return nil, err
	}
	return expr, nil
}

// standInProvider is a sql.DatabaseProvider whose every database holds just |table|, so that a query planned against it
// resolves |table| whatever the session's current database.
type standInProvider struct {
	table standInTable
}

var _ sql.DatabaseProvider = standInProvider{}

func (p standInProvider) Database(_ *sql.Context, name string) (sql.Database, error) {
	return standInDatabase{name: name, table: p.table}, nil
}

func (p standInProvider) HasDatabase(*sql.Context, string) bool {
	return true
}

func (p standInProvider) AllDatabases(*sql.Context) []sql.Database {
	return nil
}

// standInDatabase is a sql.Database holding just |table|.
type standInDatabase struct {
	name  string
	table standInTable
}

var _ sql.Database = standInDatabase{}

func (d standInDatabase) Name() string {
	return d.name
}

func (d standInDatabase) GetTableInsensitive(_ *sql.Context, tblName string) (sql.Table, bool, error) {
	if strings.EqualFold(tblName, d.table.name) {
		return d.table, true, nil
	}
	return nil, false, nil
}

func (d standInDatabase) GetTableNames(*sql.Context) ([]string, error) {
	return []string{d.table.name}, nil
}

// standInTable is a sql.Table with no rows, used only to plan expressions over its columns.
type standInTable struct {
	name string
	sch  sql.Schema
}

var _ sql.Table = standInTable{}

func (t standInTable) Name() string {
	return t.name
}

func (t standInTable) String() string {
	return t.name
}

func (t standInTable) Schema(*sql.Context) sql.Schema {
	return t.sch
}

func (t standInTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (t standInTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return nil, fmt.Errorf("table %s cannot be read", t.name)
}

func (t standInTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return nil, fmt.Errorf("table %s cannot be read", t.name)
}
