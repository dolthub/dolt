// Copyright 2020 Liquidata, Inc.
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

	sqle "github.com/liquidata-inc/go-mysql-server"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/parse"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/store/atomicerr"
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

	lazyFrom, projections, err := lazyQueryPlan(from)
	if err != nil {
		return nil, err
	}
	fromIter, err := lazyFrom.RowIter(fromCtx)
	if err != nil {
		return nil, err
	}

	lazyTo, _, err := lazyQueryPlan(to)
	if err != nil {
		return nil, err
	}
	toIter, err := lazyTo.RowIter(toCtx)
	if err != nil {
		return nil, err
	}

	trueSch := from.Schema()
	rowOrder := extractRowOrder(lazyFrom)
	ae := atomicerr.New()

	qd := &QueryDiffer{
		ctx:   fromCtx,
		sch:   trueSch,
		from:  newIterQueue(fromCtx, fromIter, projections, ae),
		to:    newIterQueue(toCtx, toIter, projections, ae),
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
			eq, err := fromRow.Equals(toRow, qd.sch)
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

func (qd *QueryDiffer) Close() error {
	qd.from.close()
	qd.to.close()
	return qd.ae.Get()
}

func makeSqlEngine(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue) (*sql.Context, *sqle.Engine, error) {
	doltSqlDB := dsqle.NewDatabase("db", dEnv.DoltDB, dEnv.RepoState, dEnv.RepoStateWriter())

	sqlCtx := sql.NewContext(ctx,
		sql.WithSession(dsqle.DefaultDoltSession()),
		sql.WithIndexRegistry(sql.NewIndexRegistry()),
		sql.WithViewRegistry(sql.NewViewRegistry()))
	sqlCtx.SetCurrentDatabase("db")

	engine := sqle.NewDefault()
	engine.AddDatabase(sql.NewInformationSchemaDatabase(engine.Catalog))

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
	parsed, err := parse.Parse(fromCtx, query)
	if err != nil {
		return nil, nil, err
	}

	fromPlan, err = fromEng.Analyzer.Analyze(fromCtx, parsed)
	if err != nil {
		return nil, nil, fmt.Errorf("error executing query on from root: %s", err.Error())
	}

	toPlan, err = toEng.Analyzer.Analyze(toCtx, parsed)
	if err != nil {
		return nil, nil, fmt.Errorf("error executing query on to root: %s", err.Error())
	}

	return fromPlan, toPlan, nil
}
