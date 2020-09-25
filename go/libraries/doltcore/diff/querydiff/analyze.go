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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// lazyQueryPlan transforms a query plan by removing Project nodes and returning
// the composite projections of the plan tree. As the plan is transformed, Expressions that
// rely on these projections are transformed to lazily evaluate the projections.
func lazyQueryPlan(node sql.Node) (lazyNode sql.Node, projections []sql.Expression, order []plan.SortField, err error) {
	if tbl, ok := node.(sql.Table); ok {
		return node, getFieldsForTable(tbl), getOrderForTable(tbl), nil
	}

	children := node.Children()
	if len(children) == 0 {
		return nil, nil, nil, fmt.Errorf("reached bottom of query plan unexpectedly")
	}

	offset := 0
	originalChildrenSchema := schema(node.Children())
	lazyChildren := make([]sql.Node, len(children))
	for i, c := range children {
		c, pjs, ord, err := lazyQueryPlan(c)
		if err != nil {
			return nil, nil, nil, err
		}
		lazyChildren[i] = c

		ord = shiftIndicesForSortFields(offset, ord...)
		order = append(order, ord...)
		pjs = shiftFieldIndices(offset, pjs...)
		projections = append(projections, pjs...)

		offset += len(c.Schema())
	}

	node, err = node.WithChildren(lazyChildren...)
	if err != nil {
		return nil, nil, nil, err
	}

	lazyNode, err = plan.TransformExpressionsWithNode(node, func(n sql.Node, e sql.Expression) (sql.Expression, error) {
		return makeExpressionLazy(node, originalChildrenSchema, e, projections)
	})
	if err != nil {
		return nil, nil, nil, err
	}

	if p, ok := lazyNode.(*plan.Project); ok {
		lazyNode = p.Child
		projections = p.Expressions()
	}

	if s, ok := lazyNode.(*plan.Sort); ok {
		// todo: prune sort fields
		// having columns duplicated here won't lead to incorrect
		// results, but could lead to extra work during sorting
		order = append(s.SortFields, order...)
	}

	if g, ok := lazyNode.(*plan.GroupBy); ok {
		lazyNode, projections, order = wrapGroupBy(g)
	}

	return lazyNode, projections, order, nil
}

func schema(nodes []sql.Node) sql.Schema {
	var schema sql.Schema
	for _, node := range nodes {
		schema = append(schema, node.Schema()...)
	}
	return schema
}

// wrapGroupBy wraps a GroupBy node in a Sort node so its output can be ordered in query diffs.
func wrapGroupBy(g *plan.GroupBy) (node sql.Node, projections []sql.Expression, order []plan.SortField) {
	projections = make([]sql.Expression, len(g.Schema()))
	for i, col := range g.Schema() {
		projections[i] = expression.NewGetField(i, col.Type, col.Name, col.Nullable)
	}
	g.SelectedExprs = append(g.SelectedExprs, g.GroupByExprs...)

	order = make([]plan.SortField, len(g.GroupByExprs))
	for i, exp := range g.GroupByExprs {
		idx := i + len(projections)
		order[i] = plan.SortField{
			Column: expression.NewGetField(idx, exp.Type(), exp.String(), exp.IsNullable()),
		}
	}

	return plan.NewSort(order, g), projections, order
}

func makeExpressionLazy(node sql.Node, originalChildSchema sql.Schema, e sql.Expression, exprs []sql.Expression) (sql.Expression, error) {
	if gf, ok := e.(*expression.GetField); ok {
		if gf.Index() >= len(exprs) {
			return nil, fmt.Errorf("index out of bounds in lazy expression substitution")
		}
		return exprs[gf.Index()], nil
	}

	// For subqueries, we need to apply the same lazy substitution to any expressions in the outer scope, and then shift
	// the indexes of the inner scope to handle any erased projections.
	if s, ok := e.(*plan.Subquery); ok {
		childSchema := schema(node.Children())
		newSubquery, err := plan.TransformExpressionsUp(s.Query, func(e sql.Expression) (sql.Expression, error) {
			if gf, ok := e.(*expression.GetField); ok {
				if gf.Index() < len(exprs) {
					return exprs[gf.Index()], nil
				} else {
					// Part of the inner scope, shift indexes
					offset := len(childSchema) - len(originalChildSchema)
					return shiftFieldIndices(offset, e)[0], nil
				}
			}
			return e, nil
		})
		if err != nil {
			return nil, err
		}
		return s.WithQuery(newSubquery), nil
	}

	return e, nil
}

func getFieldsForTable(tbl sql.Table) []sql.Expression {
	fields := make([]sql.Expression, len(tbl.Schema()))
	for i, col := range tbl.Schema() {
		fields[i] = expression.NewGetFieldWithTable(i, col.Type, tbl.Name(), col.Name, col.PrimaryKey)
	}
	return fields
}

func getOrderForTable(tbl sql.Table) (order []plan.SortField) {
	for i, col := range tbl.Schema() {
		if !col.PrimaryKey {
			continue
		}
		pkOrder := plan.SortField{
			Column:       expression.NewGetField(i, col.Type, col.Name, col.PrimaryKey),
			Order:        plan.Ascending,
			NullOrdering: plan.NullsFirst,
		}
		order = append(order, pkOrder)
	}
	return order
}

func shiftFieldIndices(offset int, exprs ...sql.Expression) []sql.Expression {
	shifted := make([]sql.Expression, len(exprs))
	for i, e := range exprs {
		shifted[i], _ = expression.TransformUp(e, func(e sql.Expression) (sql.Expression, error) {
			if gf, ok := e.(*expression.GetField); ok {
				return gf.WithIndex(gf.Index() + offset), nil
			}
			return e, nil
		})
	}
	return shifted
}

func shiftIndicesForSortFields(offset int, order ...plan.SortField) []plan.SortField {
	shifted := make([]plan.SortField, len(order))
	for i, sf := range order {
		sf.Column = shiftFieldIndices(offset, sf.Column)[0]
		shifted[i] = sf
	}
	return shifted
}
