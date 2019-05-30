package sql

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/xwb1989/sqlparser"
)

// Boolean predicate func type to filter rows in result sets
type RowFilterFn func(r row.Row) (matchesFilter bool)

// A RowFilter knows how to filter rows, and optionally can perform initialization logic. Init() must be called by
// clients before using filter().
type RowFilter struct {
	filter RowFilterFn
	InitFn func(resolver TagResolver) error
	InitValue
}

func (rf *RowFilter) Init(resolver TagResolver) error {
	if rf.InitFn != nil {
		return rf.InitFn(resolver)
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
	rowFilters := make([]*RowFilter, 0)
	for _, je := range joins {
		if filterFn, err := createFilterForJoin(je, inputSchemas, aliases); err != nil {
			return nil, err
		} else if filterFn != nil {
			rowFilters = append(rowFilters, filterFn)
		}
	}

	rowFilter := newRowFilter(func(r row.Row) (matchesFilter bool) {
		for _, rf := range rowFilters {
			if !rf.filter(r) {
				return false
			}
		}
		return true
	})
	rowFilter.InitFn = func(resolver TagResolver) error {
		for _, rf := range rowFilters {
			if err := rf.Init(resolver); err != nil {
				return err
			}
		}
		return nil
	}

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
	var rowFilterFn RowFilterFn
	var gettersToInit []InitValue
	switch expr := whereExpr.(type) {
	case *sqlparser.ComparisonExpr:

		leftGetter, err := getterFor(expr.Left, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		rightGetter, err := getterFor(expr.Right, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}

		// TODO: better type checking. This always converts the right type to the left. Probably not appropriate in all
		//  cases.
		if leftGetter.NomsKind != rightGetter.NomsKind {
			if rightGetter, err = ConversionValueGetter(rightGetter, leftGetter.NomsKind); err != nil {
				return nil, err
			}
		}

		gettersToInit = append(gettersToInit, leftGetter, rightGetter)

		// All the operations differ only in their filter logic
		switch expr.Operator {
		case sqlparser.EqualStr:
			rowFilterFn = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				return leftVal.Equals(rightVal)
			}
		case sqlparser.LessThanStr:
			rowFilterFn = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				return leftVal.Less(rightVal)
			}
		case sqlparser.GreaterThanStr:
			rowFilterFn = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				return rightVal.Less(leftVal)
			}
		case sqlparser.LessEqualStr:
			rowFilterFn = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				return leftVal.Less(rightVal) || leftVal.Equals(rightVal)
			}
		case sqlparser.GreaterEqualStr:
			rowFilterFn = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				return rightVal.Less(leftVal) || rightVal.Equals(leftVal)
			}
		case sqlparser.NotEqualStr:
			rowFilterFn = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				return !leftVal.Equals(rightVal)
			}
		case sqlparser.InStr:
			rowFilterFn = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				set := rightVal.(types.Set)
				return set.Has(context.Background(), leftVal)
			}
		case sqlparser.NotInStr:
			rowFilterFn = func(r row.Row) bool {
				leftVal := leftGetter.Get(r)
				rightVal := rightGetter.Get(r)
				if types.IsNull(leftVal) || types.IsNull(rightVal) {
					return false
				}
				set := rightVal.(types.Set)
				return !set.Has(context.Background(), leftVal)
			}
		case sqlparser.NullSafeEqualStr:
			return nil, errFmt("null safe equal operation not supported")
		case sqlparser.LikeStr:
			return nil, errFmt("like keyword not supported")
		case sqlparser.NotLikeStr:
			return nil, errFmt("like keyword not supported")
		case sqlparser.RegexpStr:
			return nil, errFmt("regular expressions not supported")
		case sqlparser.NotRegexpStr:
			return nil, errFmt("regular expressions not supported")
		case sqlparser.JSONExtractOp:
			return nil, errFmt("json not supported")
		case sqlparser.JSONUnquoteExtractOp:
			return nil, errFmt("json not supported")
		}
	case *sqlparser.ColName:
		getter, err := getterFor(expr, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}

		if getter.NomsKind != types.BoolKind {
			return nil, errFmt("Type mismatch: cannot use column %v as boolean expression", nodeToString(expr))
		}

		gettersToInit = append(gettersToInit, getter)

		rowFilterFn = func(r row.Row) bool {
			colVal := getter.Get(r)
			if types.IsNull(colVal) {
				return false
			}
			return colVal.Equals(types.Bool(true))
		}

	case *sqlparser.AndExpr:
		var leftFilter, rightFilter *RowFilter
		var err error
		if leftFilter, err = createFilterForWhereExpr(expr.Left, inputSchemas, aliases); err != nil {
			return nil, err
		}
		if rightFilter, err = createFilterForWhereExpr(expr.Right, inputSchemas, aliases); err != nil {
			return nil, err
		}
		rowFilterFn = func(r row.Row) (matchesFilter bool) {
			return leftFilter.filter(r) && rightFilter.filter(r)
		}

		gettersToInit = append(gettersToInit, leftFilter, rightFilter)

	case *sqlparser.OrExpr:
		var leftFilter, rightFilter *RowFilter
		var err error
		if leftFilter, err = createFilterForWhereExpr(expr.Left, inputSchemas, aliases); err != nil {
			return nil, err
		}
		if rightFilter, err = createFilterForWhereExpr(expr.Right, inputSchemas, aliases); err != nil {
			return nil, err
		}
		rowFilterFn = func(r row.Row) (matchesFilter bool) {
			return leftFilter.filter(r) || rightFilter.filter(r)
		}

		gettersToInit = append(gettersToInit, leftFilter, rightFilter)

	case *sqlparser.IsExpr:
		getter, err := getterFor(expr.Expr, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}

		op := expr.Operator
		switch op {
		case sqlparser.IsNullStr, sqlparser.IsNotNullStr:
			rowFilterFn = func(r row.Row) (matchesFilter bool) {
				colVal := getter.Get(r)
				if (types.IsNull(colVal) && op == sqlparser.IsNullStr) || (!types.IsNull(colVal) && op == sqlparser.IsNotNullStr) {
					return true
				}
				return false
			}
		case sqlparser.IsTrueStr, sqlparser.IsNotTrueStr, sqlparser.IsFalseStr, sqlparser.IsNotFalseStr:
			if getter.NomsKind != types.BoolKind {
				return nil, errFmt("Type mismatch: cannot use column %v as boolean expression", nodeToString(expr))
			}

			rowFilterFn = func(r row.Row) (matchesFilter bool) {
				colVal := getter.Get(r)
				if types.IsNull(colVal) {
					return false
				}
				// TODO: this may not be the correct nullness semantics for "is not" comparisons
				if colVal.Equals(types.Bool(true)) {
					return op == sqlparser.IsTrueStr || op == sqlparser.IsNotFalseStr
				} else {
					return op == sqlparser.IsFalseStr || op == sqlparser.IsNotTrueStr
				}
			}
		default:
			return nil, errFmt("Unrecognized is comparison: %v", expr.Operator)
		}

		gettersToInit = append(gettersToInit, getter)

	// Unary and Binary operators are supported in getterFor(), but not as top-level nodes here.
	case *sqlparser.BinaryExpr:
		return nil, errFmt("Binary expressions not supported: %v", nodeToString(expr))
	case *sqlparser.UnaryExpr:
		return nil, errFmt("Unary expressions not supported: %v", nodeToString(expr))

	// Full listing of the unsupported types for informative error messages
	case *sqlparser.NotExpr:
		return nil, errFmt("Not expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ParenExpr:
		return nil, errFmt("Parenthetical expressions not supported: %v", nodeToString(expr))
	case *sqlparser.RangeCond:
		return nil, errFmt("Range expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ExistsExpr:
		return nil, errFmt("Exists expressions not supported: %v", nodeToString(expr))
	case *sqlparser.SQLVal:
		return nil, errFmt("Literal expressions not supported: %v", nodeToString(expr))
	case *sqlparser.NullVal:
		return nil, errFmt("NULL expressions not supported: %v", nodeToString(expr))
	case *sqlparser.BoolVal:
		return nil, errFmt("Bool expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ValTuple:
		return nil, errFmt("Tuple expressions not supported: %v", nodeToString(expr))
	case *sqlparser.Subquery:
		return nil, errFmt("Subquery expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ListArg:
		return nil, errFmt("List expressions not supported: %v", nodeToString(expr))
	case *sqlparser.IntervalExpr:
		return nil, errFmt("Interval expressions not supported: %v", nodeToString(expr))
	case *sqlparser.CollateExpr:
		return nil, errFmt("Collate expressions not supported: %v", nodeToString(expr))
	case *sqlparser.FuncExpr:
		return nil, errFmt("Function expressions not supported: %v", nodeToString(expr))
	case *sqlparser.CaseExpr:
		return nil, errFmt("Case expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ValuesFuncExpr:
		return nil, errFmt("Values func expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ConvertExpr:
		return nil, errFmt("Conversion expressions not supported: %v", nodeToString(expr))
	case *sqlparser.SubstrExpr:
		return nil, errFmt("Substr expressions not supported: %v", nodeToString(expr))
	case *sqlparser.ConvertUsingExpr:
		return nil, errFmt("Convert expressions not supported: %v", nodeToString(expr))
	case *sqlparser.MatchExpr:
		return nil, errFmt("Match expressions not supported: %v", nodeToString(expr))
	case *sqlparser.GroupConcatExpr:
		return nil, errFmt("Group concat expressions not supported: %v", nodeToString(expr))
	case *sqlparser.Default:
		return nil, errFmt("Unrecognized expression: %v", nodeToString(expr))
	default:
		return nil, errFmt("Unrecognized expression: %v", nodeToString(expr))
	}

	rowFilter := newRowFilter(rowFilterFn)
	rowFilter.InitFn = func(resolver TagResolver) error {
		for _, getter := range gettersToInit {
			if err := getter.Init(resolver); err != nil {
				return err
			}
		}
		return nil
	}

	return rowFilter, nil
}
