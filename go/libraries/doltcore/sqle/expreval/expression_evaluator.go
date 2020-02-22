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

package expreval

import (
	"context"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var errUnsupportedComparisonType = errors.NewKind("Unsupported Comparison Type.")
var errUnknownColumn = errors.NewKind("Column %s not found.")
var errInvalidConversion = errors.NewKind("Could not convert %s from %s to %s.")
var errNotImplemented = errors.NewKind("Not Implemented: %s")

// ExpressionFunc is a function that takes a map of tag to value and returns whether some set of criteria are true for
// the set of values
type ExpressionFunc func(ctx context.Context, vals map[uint64]types.Value) (bool, error)

// ExpressionFuncFromSQLExpressions returns an ExpressionFunc which represents the slice of sql.Expressions passed in
func ExpressionFuncFromSQLExpressions(nbf *types.NomsBinFormat, sch schema.Schema, expressions []sql.Expression) (ExpressionFunc, error) {
	var root ExpressionFunc
	for _, exp := range expressions {
		expFunc, err := getExpFunc(nbf, sch, exp)

		if err != nil {
			return nil, err
		}

		if root == nil {
			root = expFunc
		} else {
			root = newAndFunc(root, expFunc)
		}
	}

	return root, nil
}

func getExpFunc(nbf *types.NomsBinFormat, sch schema.Schema, exp sql.Expression) (ExpressionFunc, error) {
	switch typedExpr := exp.(type) {
	case *expression.Equals:
		return newCamparisonFunc(EqualsOp{}, typedExpr.BinaryExpression, sch)
	case *expression.GreaterThan:
		return newCamparisonFunc(GreaterOp{nbf}, typedExpr.BinaryExpression, sch)
	case *expression.GreaterThanOrEqual:
		return newCamparisonFunc(GreaterEqualOp{nbf}, typedExpr.BinaryExpression, sch)
	case *expression.LessThan:
		return newCamparisonFunc(LessOp{nbf}, typedExpr.BinaryExpression, sch)
	case *expression.LessThanOrEqual:
		return newCamparisonFunc(LessEqualOp{nbf}, typedExpr.BinaryExpression, sch)
	case *expression.Or:
		leftFunc, err := getExpFunc(nbf, sch, typedExpr.Left)

		if err != nil {
			return nil, err
		}

		rightFunc, err := getExpFunc(nbf, sch, typedExpr.Right)

		if err != nil {
			return nil, err
		}

		return newOrFunc(leftFunc, rightFunc), nil
	case *expression.And:
		leftFunc, err := getExpFunc(nbf, sch, typedExpr.Left)

		if err != nil {
			return nil, err
		}

		rightFunc, err := getExpFunc(nbf, sch, typedExpr.Right)

		if err != nil {
			return nil, err
		}

		return newAndFunc(leftFunc, rightFunc), nil
	}

	return nil, errNotImplemented.New(exp.Type().String())
}

func newOrFunc(left ExpressionFunc, right ExpressionFunc) ExpressionFunc {
	return func(ctx context.Context, vals map[uint64]types.Value) (b bool, err error) {
		lRes, err := left(ctx, vals)

		if err != nil {
			return false, err
		}

		if lRes {
			return true, nil
		}

		return right(ctx, vals)
	}
}

func newAndFunc(left ExpressionFunc, right ExpressionFunc) ExpressionFunc {
	return func(ctx context.Context, vals map[uint64]types.Value) (b bool, err error) {
		lRes, err := left(ctx, vals)

		if err != nil {
			return false, err
		}

		if !lRes {
			return false, nil
		}

		return right(ctx, vals)
	}
}

type ComparisonType int

const (
	InvalidCompare ComparisonType = iota
	VariableConstCompare
	VariableVariableCompare
	ConstConstCompare
)

func GetComparisonType(be expression.BinaryExpression) ([]*expression.GetField, []*expression.Literal, ComparisonType, error) {
	var variables []*expression.GetField
	var consts []*expression.Literal

	for _, curr := range []sql.Expression{be.Left, be.Right} {
		// need to remove this and handle properly
		if conv, ok := curr.(*expression.Convert); ok {
			curr = conv.Child
		}

		switch v := curr.(type) {
		case *expression.GetField:
			variables = append(variables, v)
		case *expression.Literal:
			consts = append(consts, v)
		default:
			return nil, nil, InvalidCompare, errUnsupportedComparisonType.New()
		}
	}

	var compType ComparisonType
	if len(consts) == 2 {
		compType = ConstConstCompare
	} else if len(variables) == 2 {
		compType = VariableVariableCompare
	} else {
		compType = VariableConstCompare
	}

	return variables, consts, compType, nil
}

var trueFunc = func(ctx context.Context, vals map[uint64]types.Value) (b bool, err error) { return true, nil }
var falseFunc = func(ctx context.Context, vals map[uint64]types.Value) (b bool, err error) { return false, nil }

func newCamparisonFunc(op CompareOp, exp expression.BinaryExpression, sch schema.Schema) (ExpressionFunc, error) {
	vars, consts, compType, err := GetComparisonType(exp)

	if err != nil {
		return nil, err
	}

	if compType == ConstConstCompare {
		res, err := op.CompareLiterals(consts[0], consts[1])

		if err != nil {
			return nil, err
		}

		if res {
			return trueFunc, nil
		} else {
			return falseFunc, nil
		}
	} else if compType == VariableConstCompare {
		colName := vars[0].Name()
		col, ok := sch.GetAllCols().GetByNameCaseInsensitive(colName)

		if !ok {
			return nil, errUnknownColumn.New(colName)
		}

		tag := col.Tag
		nomsVal, err := LiteralToNomsValue(col.Kind, consts[0])

		if err != nil {
			return nil, err
		}

		compareNomsValues := op.CompareNomsValues
		compareToNull := op.CompareToNull

		return func(ctx context.Context, vals map[uint64]types.Value) (b bool, err error) {
			colVal, ok := vals[tag]

			if ok && !types.IsNull(colVal) {
				return compareNomsValues(colVal, nomsVal)
			} else {
				return compareToNull(nomsVal)
			}
		}, nil
	} else {
		col1Name := vars[0].Name()
		col1, ok := sch.GetAllCols().GetByNameCaseInsensitive(col1Name)

		if !ok {
			return nil, errUnknownColumn.New(col1Name)
		}

		col2Name := vars[1].Name()
		col2, ok := sch.GetAllCols().GetByNameCaseInsensitive(col2Name)

		if !ok {
			return nil, errUnknownColumn.New(col2Name)
		}

		compareNomsValues := op.CompareNomsValues
		compareToNull := op.CompareToNull

		tag1, tag2 := col1.Tag, col2.Tag
		return func(ctx context.Context, vals map[uint64]types.Value) (b bool, err error) {
			v1 := vals[tag1]
			v2 := vals[tag2]

			if types.IsNull(v1) {
				return compareToNull(v2)
			} else {
				return compareNomsValues(v1, v2)
			}
		}, nil
	}
}
