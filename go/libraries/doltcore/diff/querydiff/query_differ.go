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

package querydiff

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/atomicerr"
)

const (
	Lesser  rowOrder = -1
	Equal   rowOrder = 0
	Greater rowOrder = 1
	Unknown rowOrder = math.MaxInt8
)

type rowOrder int8

type QueryDiffer struct {
	ctx   *sql.Context
	sch   sql.Schema
	from  *iterQueue
	to    *iterQueue
	order []plan.SortField
	ae    *atomicerr.AtomicError
}

func MakeQueryDiffer(ctx context.Context, dEnv *env.DoltEnv, fromRoot, toRoot *doltdb.RootValue, query string) (*QueryDiffer, error) {
	fromCtx, fromEng, err := makeSqlEngine(ctx, dEnv, fromRoot)
	if err != nil {
		return nil, err
	}
	toCtx, toEng, err := makeSqlEngine(ctx, dEnv, toRoot)
	if err != nil {
		return nil, err
	}

	from, to, err := getQueryPlans(fromCtx, toCtx, fromEng, toEng, query)
	if err != nil {
		return nil, err
	}

	lazyFrom, fromProjections, fromOrder, err := lazyQueryPlan(from)
	if err != nil {
		return nil, err
	}
	fromIter, err := lazyFrom.RowIter(fromCtx, nil)
	if err != nil {
		return nil, err
	}

	lazyTo, toProjections, _, err := lazyQueryPlan(to)
	if err != nil {
		return nil, err
	}
	toIter, err := lazyTo.RowIter(toCtx, nil)
	if err != nil {
		return nil, err
	}

	trueSch := from.Schema()
	rowOrder := fromOrder
	ae := atomicerr.New()

	qd := &QueryDiffer{
		ctx:   fromCtx,
		sch:   trueSch,
		from:  newIterQueue(fromCtx, fromIter, fromProjections, ae),
		to:    newIterQueue(toCtx, toIter, toProjections, ae),
		order: rowOrder,
		ae:    ae,
	}

	return qd, nil
}

func (qd *QueryDiffer) Start() {
	qd.from.start()
	qd.to.start()
}

func (qd *QueryDiffer) NextDiff() (fromRow sql.Row, toRow sql.Row, err error) {
	for {
		if qd.ae.IsSet() {
			return nil, nil, qd.ae.Get()
		}
		if qd.from.isDone() && qd.to.isDone() {
			return nil, nil, io.EOF
		}
		if qd.from.isDone() {
			toRow, err = qd.to.projectPop()
			if err != nil {
				return nil, nil, err
			}
			return nil, toRow, nil
		}
		if qd.to.isDone() {
			fromRow, err = qd.from.projectPop()
			if err != nil {
				return nil, nil, err
			}
			return fromRow, nil, nil
		}

		cmp, err := qd.rowCompare(qd.from.peek(), qd.to.peek())
		if err != nil {
			return nil, nil, err
		}

		switch cmp {
		case Lesser:
			fromRow, err = qd.from.projectPop()
			if err != nil {
				return nil, nil, err
			}
			return fromRow, nil, nil
		case Greater:
			toRow, err = qd.to.projectPop()
			if err != nil {
				return nil, nil, err
			}
			return nil, toRow, nil
		case Equal:
			fromRow, err = qd.from.projectPop()
			if err != nil {
				return nil, nil, err
			}
			toRow, err = qd.to.projectPop()
			if err != nil {
				return nil, nil, err
			}
			eq, err := nullSafeRowEquality(toRow, fromRow, qd.sch)
			if err != nil {
				return nil, nil, err
			}
			if eq {
				continue
			}
			return fromRow, toRow, nil
		default:
			panic("bad row cmp")
		}
	}
}

func (qd *QueryDiffer) rowCompare(left, right sql.Row) (rowOrder, error) {
	for _, sf := range qd.order {
		typ := sf.Column.Type()
		av, err := sf.Column.Eval(qd.ctx, left)
		if err != nil {
			return Unknown, err
		}

		bv, err := sf.Column.Eval(qd.ctx, right)
		if err != nil {
			return Unknown, err
		}

		if sf.Order == plan.Descending {
			av, bv = bv, av
		}

		if av == nil && bv == nil {
			continue
		} else if av == nil {
			if sf.NullOrdering == plan.NullsFirst {
				return Lesser, nil
			} else {
				return Greater, nil
			}
		} else if bv == nil {
			if sf.NullOrdering == plan.NullsFirst {
				return Greater, nil
			} else {
				return Lesser, nil
			}
		}

		cmp, err := typ.Compare(av, bv)
		if err != nil {
			return Unknown, err
		}

		switch cmp {
		case -1:
			return Lesser, nil
		case 1:
			return Greater, nil
		}
	}
	return Equal, nil
}

func (qd *QueryDiffer) Schema() sql.Schema {
	return qd.sch
}

func (qd *QueryDiffer) Order() []plan.SortField {
	return qd.order
}

func (qd *QueryDiffer) Close() error {
	qd.from.close()
	qd.to.close()
	return qd.ae.Get()
}

func nullSafeRowEquality(left, right sql.Row, sch sql.Schema) (bool, error) {
	if len(left) != len(right) {
		return false, nil
	}
	for i := range left {
		if left[i] == sql.Null {
			left[i] = nil // RIP Lisa Lopes
		}
		if right[i] == sql.Null {
			right[i] = nil
		}
	}
	return left.Equals(right, sch)
}

func makeSqlEngine(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue) (*sql.Context, *sqle.Engine, error) {
	doltSqlDB := dsqle.NewDatabase("db", dEnv.DoltDB, dEnv.RepoStateReader(), dEnv.RepoStateWriter())

	sqlCtx := sql.NewContext(ctx,
		sql.WithSession(dsqle.DefaultDoltSession()),
		sql.WithIndexRegistry(sql.NewIndexRegistry()),
		sql.WithViewRegistry(sql.NewViewRegistry()))
	sqlCtx.SetCurrentDatabase("db")

	engine := sqle.NewDefault()
	engine.AddDatabase(information_schema.NewInformationSchemaDatabase(engine.Catalog))

	dsess := dsqle.DSessFromSess(sqlCtx.Session)

	engine.AddDatabase(doltSqlDB)

	err := dsess.AddDB(sqlCtx, doltSqlDB)
	if err != nil {
		return nil, nil, err
	}

	err = doltSqlDB.SetRoot(sqlCtx, root)
	if err != nil {
		return nil, nil, err
	}

	err = dsqle.RegisterSchemaFragments(sqlCtx, doltSqlDB, root)
	if err != nil {
		return nil, nil, err
	}

	return sqlCtx, engine, nil
}

func getQueryPlans(fromCtx, toCtx *sql.Context, fromEng, toEng *sqle.Engine, query string) (fromPlan, toPlan sql.Node, err error) {
	err = validateQueryType(fromCtx, fromEng, query)
	if err != nil {
		return nil, nil, err
	}

	parsed, err := parse.Parse(fromCtx, query)
	if err != nil {
		return nil, nil, err
	}

	fromPlan, err = fromEng.Analyzer.Analyze(fromCtx, parsed, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("error executing query on from root: %s", err.Error())
	}
	err = validateQueryPlan(fromCtx, fromEng, fromPlan, query)
	if err != nil {
		return nil, nil, err
	}

	toPlan, err = toEng.Analyzer.Analyze(toCtx, parsed, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("error executing query on to root: %s", err.Error())
	}
	err = validateQueryPlan(toCtx, toEng, toPlan, query)
	if err != nil {
		return nil, nil, err
	}

	return fromPlan, toPlan, nil
}

func validateQueryType(ctx *sql.Context, eng *sqle.Engine, query string) error {
	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		return QueryDiffError{cause: "query is empty"}
	} else if err != nil {
		return QueryDiffError{cause: err.Error()}
	}

	if strings.Contains(strings.ToLower(query), "information_schema") {
		return QueryDiffError{cause: "querying information_schema is not supported"}
	}

	switch sqlStatement.(type) {
	case *sqlparser.Select:
		return nil
	case *sqlparser.Union:
		return errPrintQueryPlan(ctx, eng, query, "UNION queries not supported")
	case *sqlparser.Show:
		return errPrintQueryPlan(ctx, eng, query, "SHOW queries not supported")
	case *sqlparser.OtherRead, *sqlparser.Explain:
		return errPrintQueryPlan(ctx, eng, query, "EXPLAIN queries not supported")
	case *sqlparser.Use:
		return errPrintQueryPlan(ctx, eng, query, "USE queries not supported")
	case *sqlparser.Set:
		return errPrintQueryPlan(ctx, eng, query, "SET queries not supported")
	case *sqlparser.Insert, *sqlparser.Update, *sqlparser.Delete:
		return errPrintQueryPlan(ctx, eng, query, "write queries not supported")
	case *sqlparser.DDL:
		return errPrintQueryPlan(ctx, eng, query, "DDL queries not supported")
	default:
		return QueryDiffError{cause: fmt.Sprintf("cannot diff query, unsupported SQL statement: '%s'", query)}
	}
}

func validateQueryPlan(ctx *sql.Context, eng *sqle.Engine, node sql.Node, query string) (err error) {
	if node == plan.Nothing || node == plan.EmptyTable {
		return errPrintQueryPlan(ctx, eng, query, "queries returning no rows are not supported")
	}

	switch node.(type) {
	case *plan.Distinct:
		// todo: create DiffDistinct node that evaluates projections before hashing
		return errPrintQueryPlan(ctx, eng, query, "DISTINCT queries not supported")
	case *plan.Generate:
		return errPrintQueryPlan(ctx, eng, query, "EXPLODE queries not supported")
	case *plan.Commit:
		return errPrintQueryPlan(ctx, eng, query, "COMMIT queries not supported")
	case *plan.Rollback:
		return errPrintQueryPlan(ctx, eng, query, "ROLLBACK queries not supported")
	case *plan.UnresolvedTable:
		return errPrintQueryPlan(ctx, eng, query, "query references unresolved table")
	}

	unsupportedTables := []string{
		"dual",
	}
	if rt, ok := node.(*plan.ResolvedTable); ok {
		for _, tn := range unsupportedTables {
			if strings.ToLower(rt.Table.Name()) == tn {
				return errPrintQueryPlan(ctx, eng, query, fmt.Sprintf("queries on table '%s' not supported", tn))
			}
		}
	}

	_, err = plan.TransformExpressions(node, func(e sql.Expression) (sql.Expression, error) {
		switch e.(type) {
		case *function.JSONExtract,
			*function.JSONUnquote:
			return nil, errPrintQueryPlan(ctx, eng, query, "JSON functions not supported")
		case *function.Explode:
			return nil, errPrintQueryPlan(ctx, eng, query, "EXPLODE function not supported")
		}
		return e, nil
	})
	if err != nil {
		return err
	}

	for _, c := range node.Children() {
		err = validateQueryPlan(ctx, eng, c, query)
		if err != nil {
			return err
		}
	}

	return nil
}

type QueryDiffError struct {
	plan  string
	cause string
}

var _ error = QueryDiffError{}

func (qde QueryDiffError) Error() string {
	return fmt.Sprintf("cannot diff query: %s\n%s", qde.cause, qde.plan)
}

func errPrintQueryPlan(ctx *sql.Context, eng *sqle.Engine, query string, cause string) error {
	_, iter, err := eng.Query(ctx, fmt.Sprintf("describe %s", query))
	if err != nil {
		return QueryDiffError{cause: fmt.Sprintf("cannot diff query: %s\n", err.Error())}
	}

	var qp strings.Builder
	qp.WriteString("query plan:\n")
	for {
		r, err := iter.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return QueryDiffError{cause: fmt.Sprintf("cannot diff query: %s\n", err.Error())}
		}
		qp.WriteString(fmt.Sprintf("\t%s\n", r[0].(string)))
	}

	return QueryDiffError{plan: qp.String(), cause: cause}
}
