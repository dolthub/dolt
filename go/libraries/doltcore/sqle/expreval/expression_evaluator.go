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

package expreval

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"gopkg.in/src-d/go-errors.v1"
)

var errUnsupportedComparisonType = errors.NewKind("Unsupported Comparison Type.")
var errUnknownColumn = errors.NewKind("Column %s not found.")
var errNotImplemented = errors.NewKind("Not Implemented: %s")

// ExpressionFunc is a function that takes a sql.Row and returns whether some set of criteria are true for
// the row's values.
type ExpressionFunc func(ctx *sql.Context, row sql.Row) (bool, error)

// ExpressionFuncFromSQLExpressions returns an ExpressionFunc which represents the slice of sql.Expressions passed in.
// The schema defines column names, types, and positions for row lookups.
func ExpressionFuncFromSQLExpressions(ctx *sql.Context, sch sql.Schema, expressions []sql.Expression) (ExpressionFunc, error) {
	var root ExpressionFunc
	for _, exp := range expressions {
		expFunc, err := getExpFunc(ctx, sch, exp)

		if err != nil {
			return nil, err
		}

		if root == nil {
			root = expFunc
		} else {
			root = newAndFunc(root, expFunc)
		}
	}

	if root == nil {
		root = func(ctx *sql.Context, row sql.Row) (bool, error) {
			return true, nil
		}
	}

	return root, nil
}

func getExpFunc(ctx *sql.Context, sch sql.Schema, exp sql.Expression) (ExpressionFunc, error) {
	switch typedExpr := exp.(type) {
	case *expression.Equals:
		return newComparisonFunc(ctx, EqualsOp{}, typedExpr, sch)
	case *expression.GreaterThan:
		return newComparisonFunc(ctx, GreaterOp{}, typedExpr, sch)
	case *expression.GreaterThanOrEqual:
		return newComparisonFunc(ctx, GreaterEqualOp{}, typedExpr, sch)
	case *expression.LessThan:
		return newComparisonFunc(ctx, LessOp{}, typedExpr, sch)
	case *expression.LessThanOrEqual:
		return newComparisonFunc(ctx, LessEqualOp{}, typedExpr, sch)
	case *expression.Or:
		leftFunc, err := getExpFunc(ctx, sch, typedExpr.Left())

		if err != nil {
			return nil, err
		}

		rightFunc, err := getExpFunc(ctx, sch, typedExpr.Right())

		if err != nil {
			return nil, err
		}

		return newOrFunc(leftFunc, rightFunc), nil
	case *expression.And:
		leftFunc, err := getExpFunc(ctx, sch, typedExpr.Left())

		if err != nil {
			return nil, err
		}

		rightFunc, err := getExpFunc(ctx, sch, typedExpr.Right())

		if err != nil {
			return nil, err
		}

		return newAndFunc(leftFunc, rightFunc), nil
	case *expression.InTuple:
		return newComparisonFunc(ctx, EqualsOp{}, typedExpr, sch)
	case *expression.Not:
		expFunc, err := getExpFunc(ctx, sch, typedExpr.Child)
		if err != nil {
			return nil, err
		}
		return newNotFunc(expFunc), nil
	case *expression.IsNull:
		return newComparisonFunc(ctx, EqualsOp{}, expression.NewNullSafeEquals(typedExpr.Child, expression.NewLiteral(nil, gmstypes.Null)), sch)
	}

	return nil, errNotImplemented.New(exp.Type().String())
}

func newOrFunc(left ExpressionFunc, right ExpressionFunc) ExpressionFunc {
	return func(ctx *sql.Context, row sql.Row) (b bool, err error) {
		lRes, err := left(ctx, row)

		if err != nil {
			return false, err
		}

		if lRes {
			return true, nil
		}

		return right(ctx, row)
	}
}

func newAndFunc(left ExpressionFunc, right ExpressionFunc) ExpressionFunc {
	return func(ctx *sql.Context, row sql.Row) (b bool, err error) {
		lRes, err := left(ctx, row)

		if err != nil {
			return false, err
		}

		if !lRes {
			return false, nil
		}

		return right(ctx, row)
	}
}

func newNotFunc(exp ExpressionFunc) ExpressionFunc {
	return func(ctx *sql.Context, row sql.Row) (b bool, err error) {
		res, err := exp(ctx, row)
		if err != nil {
			return false, err
		}

		return !res, nil
	}
}

type ComparisonType int

const (
	InvalidCompare ComparisonType = iota
	VariableConstCompare
	VariableVariableCompare
	VariableInLiteralList
	ConstConstCompare
)

// GetComparisonType looks at a go-mysql-server BinaryExpression classifies the left and right arguments
// as variables or constants.
func GetComparisonType(be expression.BinaryExpression) ([]*expression.GetField, []*expression.Literal, ComparisonType, error) {
	var variables []*expression.GetField
	var consts []*expression.Literal

	for _, curr := range []sql.Expression{be.Left(), be.Right()} {
		// need to remove this and handle properly
		if conv, ok := curr.(*expression.Convert); ok {
			curr = conv.Child
		}

		switch v := curr.(type) {
		case *expression.GetField:
			variables = append(variables, v)
		case *expression.Literal:
			consts = append(consts, v)
		case expression.Tuple:
			children := v.Children()
			for _, currChild := range children {
				lit, ok := currChild.(*expression.Literal)
				if !ok {
					return nil, nil, InvalidCompare, errUnsupportedComparisonType.New()
				}
				consts = append(consts, lit)
			}
		default:
			return nil, nil, InvalidCompare, errUnsupportedComparisonType.New()
		}
	}

	var compType ComparisonType
	if len(variables) == 2 {
		compType = VariableVariableCompare
	} else if len(variables) == 1 {
		if len(consts) == 1 {
			compType = VariableConstCompare
		} else if len(consts) > 1 {
			compType = VariableInLiteralList
		}
	} else if len(consts) == 2 {
		compType = ConstConstCompare
	}

	return variables, consts, compType, nil
}

var trueFunc = func(ctx *sql.Context, row sql.Row) (b bool, err error) { return true, nil }
var falseFunc = func(ctx *sql.Context, row sql.Row) (b bool, err error) { return false, nil }

// findSchemaCol finds a column in a sql.Schema by name (case-insensitive), returning its index and column.
func findSchemaCol(name string, sch sql.Schema) (int, *sql.Column, bool) {
	for i, col := range sch {
		if strings.EqualFold(col.Name, name) {
			return i, col, true
		}
	}
	return -1, nil, false
}

func newComparisonFunc(ctx *sql.Context, op CompareOp, exp expression.BinaryExpression, sch sql.Schema) (ExpressionFunc, error) {
	vars, consts, compType, err := GetComparisonType(exp)

	if err != nil {
		return nil, err
	}

	if compType == ConstConstCompare {
		n, err := compareLiterals(ctx, consts[0], consts[1])
		if err != nil {
			return nil, err
		}
		if op.ApplyCmp(n) {
			return trueFunc, nil
		}
		return falseFunc, nil
	} else if compType == VariableConstCompare {
		colName := vars[0].Name()
		colIdx, col, ok := findSchemaCol(colName, sch)

		if !ok {
			return nil, errUnknownColumn.New(colName)
		}

		sqlType := col.Type
		litIsNull := consts[0].Value() == nil

		var sqlLitVal interface{}
		if !litIsNull {
			sqlLitVal, _, err = sqlType.Convert(ctx, consts[0].Value())
			if err != nil {
				return nil, err
			}
		}

		return func(ctx *sql.Context, row sql.Row) (b bool, err error) {
			val := row[colIdx]
			if val != nil {
				if litIsNull {
					return false, nil
				}
				n, err := sqlType.Compare(ctx, val, sqlLitVal)
				if err != nil {
					return false, err
				}
				return op.ApplyCmp(n), nil
			}
			return op.CompareToNil(litIsNull), nil
		}, nil
	} else if compType == VariableVariableCompare {
		col1Name := vars[0].Name()
		col1Idx, col1, ok := findSchemaCol(col1Name, sch)

		if !ok {
			return nil, errUnknownColumn.New(col1Name)
		}

		col2Name := vars[1].Name()
		col2Idx, _, ok := findSchemaCol(col2Name, sch)

		if !ok {
			return nil, errUnknownColumn.New(col2Name)
		}

		sqlType := col1.Type
		return func(ctx *sql.Context, row sql.Row) (b bool, err error) {
			v1 := row[col1Idx]
			v2 := row[col2Idx]

			if v1 == nil {
				return op.CompareToNil(v2 == nil), nil
			}
			if v2 == nil {
				return false, nil
			}
			n, err := sqlType.Compare(ctx, v1, v2)
			if err != nil {
				return false, err
			}
			return op.ApplyCmp(n), nil
		}, nil
	} else if compType == VariableInLiteralList {
		colName := vars[0].Name()
		colIdx, col, ok := findSchemaCol(colName, sch)

		if !ok {
			return nil, errUnknownColumn.New(colName)
		}

		sqlType := col.Type

		// Pre-convert all literal values to the column's SQL type
		sqlVals := make([]interface{}, len(consts))
		nullFlags := make([]bool, len(consts))
		for i, c := range consts {
			if c.Value() == nil {
				nullFlags[i] = true
			} else {
				sqlVals[i], _, err = sqlType.Convert(ctx, c.Value())
				if err != nil {
					return nil, err
				}
			}
		}

		return func(ctx *sql.Context, row sql.Row) (b bool, err error) {
			val := row[colIdx]
			colIsNull := val == nil

			for i, sv := range sqlVals {
				var result bool
				if colIsNull {
					result = op.CompareToNil(nullFlags[i])
				} else if nullFlags[i] {
					result = false
				} else {
					n, err := sqlType.Compare(ctx, val, sv)
					if err != nil {
						return false, err
					}
					result = op.ApplyCmp(n)
				}

				if result {
					return true, nil
				}
			}

			return false, nil
		}, nil
	} else {
		return nil, errUnsupportedComparisonType.New()
	}
}
