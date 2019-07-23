// Copyright 2019 Liquidata, Inc.
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

package sql

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Boolean predicate func type to filter rows in result sets
type RowFilterFn func(r row.Row) (matchesFilter bool)

// A RowFilter knows how to filter rows, and optionally can perform initialization logic. Init() must be called by
// clients before using filter().
type RowFilter struct {
	filter RowFilterFn
	initFn func(resolver TagResolver) error
	InitValue
}

func (rf *RowFilter) Init(resolver TagResolver) error {
	if rf.initFn != nil {
		return rf.initFn(resolver)
	}
	return nil
}

func newRowFilter(fn func(r row.Row) (matchesFilter bool)) *RowFilter {
	return &RowFilter{filter: fn}
}

// createFilterForWhere creates a filter function from the where clause given, or returns an error if it cannot
func createFilterForWhere(whereClause *sqlparser.Where, inputSchemas map[string]schema.Schema, aliases *Aliases) (*RowFilter, error) {
	if whereClause != nil && whereClause.Type != sqlparser.WhereStr {
		return nil, errFmt("Having clause not supported")
	}

	if whereClause == nil {
		return newRowFilter(
			func(r row.Row) bool {
				return true
			}), nil
	}

	return createFilterForWhereExpr(whereClause.Expr, inputSchemas, aliases.TableAliasesOnly())
}

// createFilterForWhere creates a filter function from the joins given
func createFilterForJoins(joins []*sqlparser.JoinTableExpr, inputSchemas map[string]schema.Schema, aliases *Aliases) (*RowFilter, error) {
	if len(joins) == 0 {
		return newRowFilter(
			func(r row.Row) bool {
				return true
			}), nil
	}

	rowFilters := make([]InitValue, 0)
	for _, je := range joins {
		if filterFn, err := createFilterForJoin(je, inputSchemas, aliases); err != nil {
			return nil, err
		} else if filterFn != nil {
			rowFilters = append(rowFilters, filterFn)
		}
	}

	rowFilter := newRowFilter(func(r row.Row) (matchesFilter bool) {
		for _, rf := range rowFilters {
			if !rf.(*RowFilter).filter(r) {
				return false
			}
		}
		return true
	})

	rowFilter.initFn = ComposeInits(rowFilters...)

	return rowFilter, nil
}

// createFilterForJoin creates a row filter function for the join expression given
func createFilterForJoin(expr *sqlparser.JoinTableExpr, schemas map[string]schema.Schema, aliases *Aliases) (*RowFilter, error) {
	if expr.Condition.Using != nil {
		return nil, errFmt("Using expression not supported: %v", nodeToString(expr.Condition.Using))
	}

	if expr.Condition.On == nil {
		return nil, nil
	}

	// This may not work in all cases -- not sure if there are expressions that are valid in where clauses but not in
	// join conditions or vice versa.
	return createFilterForWhereExpr(expr.Condition.On, schemas, aliases.TableAliasesOnly())
}

// createFilterForWhereExpr is the helper function for createFilterForWhere, which can be used recursively on sub
// expressions. Supported parser types here must be kept in sync with resolveColumnsInExpr
func createFilterForWhereExpr(whereExpr sqlparser.Expr, inputSchemas map[string]schema.Schema, aliases *Aliases) (*RowFilter, error) {

	getter, err := getterFor(whereExpr, inputSchemas, aliases)
	if err != nil {
		return nil, err
	}

	if getter.NomsKind != types.BoolKind {
		return nil, errFmt("Type mismatch: cannot use '%v' as boolean expression", nodeToString(whereExpr))
	}

	rowFilterFn := func(r row.Row) (matchesFilter bool) {
		boolVal := getter.Get(r)
		return bool(boolVal.(types.Bool))
	}

	rowFilter := newRowFilter(rowFilterFn)
	rowFilter.initFn = ComposeInits(getter)

	return rowFilter, nil
}
