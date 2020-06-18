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
	"fmt"
	"io"
	"strings"

	"github.com/liquidata-inc/go-mysql-server/sql/expression"

	sqle "github.com/liquidata-inc/go-mysql-server"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

type LazyProject struct {
	*plan.Project
}

func (lp LazyProject) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	// skip row projection during iteration
	return lp.Project.Child.RowIter(ctx)
}

// lazyQueryPlan transforms a query plan by removing Project nodes and returning
// the composite projections of the plan tree. As the plan is transformed, Expressions that
// rely on these projections are transformed to lazily evaluate the projections.
func lazyQueryPlan(node sql.Node) (lazyNode sql.Node, projections []sql.Expression, err error) {
	// ignore opaque nodes

	if tbl, ok := node.(sql.Table); ok {
		return node, getFieldsForTable(tbl), nil
	}

	children := node.Children()
	if len(children) == 0 {
		return nil, nil, fmt.Errorf("reached bottom of query plan unexpectedly")
	}

	offset := 0
	newChildren := make([]sql.Node, len(children))
	for i, c := range children {
		c, pjs, err := lazyQueryPlan(c)
		if err != nil {
			return nil, nil, err
		}
		if len(c.Schema()) != len(pjs) {
			return nil, nil, fmt.Errorf("node schema mismatched with node projections")
		}
		newChildren[i] = c

		pjs = shiftFieldIndexes(pjs, offset)
		projections = append(projections, pjs...)
		offset += len(pjs)
	}

	lazyNode, err = node.WithChildren(newChildren...)
	if err != nil {
		return nil, nil, err
	}

	lazyNode, err = plan.TransformExpressions(lazyNode, func(e sql.Expression) (sql.Expression, error) {
		return makeExpressionLazy(e, projections)
	})
	if err != nil {
		return nil, nil, err
	}

	if p, ok := lazyNode.(*plan.Project); ok {
		lazyNode = &LazyProject{p}
		projections = p.Expressions()
	}

	return lazyNode, projections, err
}

func extractRowOrder(node sql.Node) (composite []plan.SortField) {
	if t, ok := node.(sql.Table); ok {
		for i, col := range t.Schema() {
			if !col.PrimaryKey {
				continue
			}
			pkOrder := plan.SortField{
				Column:       expression.NewGetField(i, col.Type, col.Name, col.PrimaryKey),
				Order:        plan.Ascending,
				NullOrdering: plan.NullsFirst,
			}
			composite = append(composite, pkOrder)
		}
		return composite
	}

	for _, c := range node.Children() {
		sf := extractRowOrder(c)
		composite = append(composite, sf...)
	}

	if s, ok := node.(*plan.Sort); ok {
		// todo: prune sort fields
		// having columns duplicated here won't lead to incorrect
		// results, but could lead to extra work during sorting
		composite = append(s.SortFields, composite...)
	}

	return composite
}

func makeExpressionLazy(e sql.Expression, composite []sql.Expression) (sql.Expression, error) {
	gf, ok := e.(*expression.GetField)
	if ok {
		if gf.Index() >= len(composite) {
			return nil, fmt.Errorf("index out of bounds in lazy expression substitution")
		}
		e = composite[gf.Index()]
	}
	return e, nil
}

func getFieldsForTable(tbl sql.Table) []sql.Expression {
	fields := make([]sql.Expression, len(tbl.Schema()))
	for i, col := range tbl.Schema() {
		fields[i] = expression.NewGetField(i, col.Type, col.Name, col.PrimaryKey)
	}
	return fields
}

func shiftFieldIndexes(composite []sql.Expression, offset int) []sql.Expression {
	shifted := make([]sql.Expression, len(composite))
	for i, e := range composite {
		shifted[i], _ = expression.TransformUp(e, func(e sql.Expression) (sql.Expression, error) {
			if gf, ok := e.(*expression.GetField); ok {
				return gf.WithIndex(gf.Index() + offset), nil
			}
			return e, nil
		})
	}
	return shifted
}

func errWithQueryPlan(ctx *sql.Context, eng *sqle.Engine, query string, cause error) error {
	_, iter, err := eng.Query(ctx, fmt.Sprintf("describe %s", query))
	if err != nil {
		return fmt.Errorf("cannot diff query. Error describing query plan: %s\n", err.Error())
	}

	var qp strings.Builder
	qp.WriteString("query plan:\n")
	for {
		r, err := iter.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("cannot diff query. Error describing query plan: %s\n", err.Error())
		}
		qp.WriteString(fmt.Sprintf("\t%s\n", r[0].(string)))
	}

	return fmt.Errorf("cannot diff query: %s\n%s", cause.Error(), qp.String())
}
