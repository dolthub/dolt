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

package sqle

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
)

// ConfigureDoltAnalyzer installs Dolt-specific ordering optimizations after the
// standard analyzer has selected access paths and fixed expression indexes.
func ConfigureDoltAnalyzer(a *analyzer.Analyzer) {
	const batchName = "dolt-diff-order"
	// Keep Dolt's extension rule IDs outside the engine's built-in rule range.
	const diffOrderRuleID analyzer.RuleId = 100000
	for _, batch := range a.Batches {
		if batch.Desc == batchName {
			return
		}
	}
	a.Batches = append(a.Batches, &analyzer.Batch{
		Desc: batchName, Iterations: 1,
		Rules: []analyzer.Rule{{Id: diffOrderRuleID, Apply: removeDiffSort}},
	})
}

func removeDiffSort(ctx *sql.Context, _ *analyzer.Analyzer, node sql.Node, scope *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	if !scope.IsEmpty() {
		return node, transform.SameTree, nil
	}
	return transform.Node(ctx, node, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		var child sql.Node
		var order sql.SortConditions
		switch n := n.(type) {
		case *plan.Sort:
			child, order = n.Child, n.SortConditions
		case *plan.TopN:
			// FOUND_ROWS needs to exhaust the input and owns session row-count
			// accounting. Leave that execution path intact.
			if n.CalcFoundRows {
				return n, transform.SameTree, nil
			}
			child, order = n.Child, n.SortConditions
		default:
			return n, transform.SameTree, nil
		}
		source := child
		name := "dolt_diff"
		if alias, ok := source.(*plan.TableAlias); ok {
			source, name = alias.Child, alias.Name()
		}
		diff, ok := source.(*dtablefunctions.DiffTableFunction)
		if !ok || !diff.IsNaturallyOrdered(ctx, order, name) {
			return n, transform.SameTree, nil
		}
		if top, ok := n.(*plan.TopN); ok {
			return plan.NewLimit(top.Limit, child).WithCalcFoundRows(top.CalcFoundRows), transform.NewTree, nil
		}
		return child, transform.NewTree, nil
	})
}
