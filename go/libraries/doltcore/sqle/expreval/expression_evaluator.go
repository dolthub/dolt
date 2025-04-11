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
	"context"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

var errUnsupportedComparisonType = errors.NewKind("Unsupported Comparison Type.")
var errUnknownColumn = errors.NewKind("Column %s not found.")
var errInvalidConversion = errors.NewKind("Could not convert %s from %s to %s.")
var errNotImplemented = errors.NewKind("Not Implemented: %s")

// ExpressionFunc is a function that takes a map of tag to value and returns whether some set of criteria are true for
// the set of values
type ExpressionFunc func(ctx context.Context, vals map[uint64]types.Value) (bool, error)

// ExpressionFuncFromSQLExpressions returns an ExpressionFunc which represents the slice of sql.Expressions passed in
func ExpressionFuncFromSQLExpressions(vr types.ValueReader, sch schema.Schema, expressions []sql.Expression) (ExpressionFunc, error) {
	var root ExpressionFunc
	for _, exp := range expressions {
		expFunc, err := getExpFunc(vr, sch, exp)

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
		root = func(ctx context.Context, vals map[uint64]types.Value) (bool, error) {
			return true, nil
		}
	}

	return root, nil
}

func getExpFunc(vr types.ValueReader, sch schema.Schema, exp sql.Expression) (ExpressionFunc, error) {
	switch typedExpr := exp.(type) {
	case *expression.Equals:
		return newComparisonFunc(EqualsOp{}, typedExpr, sch)
	case *expression.GreaterThan:
		return newComparisonFunc(GreaterOp{vr}, typedExpr, sch)
	case *expression.GreaterThanOrEqual:
		return newComparisonFunc(GreaterEqualOp{vr}, typedExpr, sch)
	case *expression.LessThan:
		return newComparisonFunc(LessOp{vr}, typedExpr, sch)
	case *expression.LessThanOrEqual:
		return newComparisonFunc(LessEqualOp{vr}, typedExpr, sch)
	case *expression.Or:
		leftFunc, err := getExpFunc(vr, sch, typedExpr.Left())

		if err != nil {
			return nil, err
		}

		rightFunc, err := getExpFunc(vr, sch, typedExpr.Right())

		if err != nil {
			return nil, err
		}

		return newOrFunc(leftFunc, rightFunc), nil
	case *expression.And:
		leftFunc, err := getExpFunc(vr, sch, typedExpr.Left())

		if err != nil {
			return nil, err
		}

		rightFunc, err := getExpFunc(vr, sch, typedExpr.Right())

		if err != nil {
			return nil, err
		}

		return newAndFunc(leftFunc, rightFunc), nil
	case *expression.InTuple:
		return newComparisonFunc(EqualsOp{}, typedExpr, sch)
	case *expression.Not:
		expFunc, err := getExpFunc(vr, sch, typedExpr.Child)
		if err != nil {
			return nil, err
		}
		return newNotFunc(expFunc), nil
	case *expression.IsNull:
		return newComparisonFunc(EqualsOp{}, expression.NewNullSafeEquals(typedExpr.Child, expression.NewLiteral(nil, gmstypes.Null)), sch)
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

func newNotFunc(exp ExpressionFunc) ExpressionFunc {
	return func(ctx context.Context, vals map[uint64]types.Value) (b bool, err error) {
		res, err := exp(ctx, vals)
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

var trueFunc = func(ctx context.Context, vals map[uint64]types.Value) (b bool, err error) { return true, nil }
var falseFunc = func(ctx context.Context, vals map[uint64]types.Value) (b bool, err error) { return false, nil }

func newComparisonFunc(op CompareOp, exp expression.BinaryExpression, sch schema.Schema) (ExpressionFunc, error) {
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
		compareToNil := op.CompareToNil

		return func(ctx context.Context, vals map[uint64]types.Value) (b bool, err error) {
			colVal, ok := vals[tag]

			if ok && !types.IsNull(colVal) {
				return compareNomsValues(ctx, colVal, nomsVal)
			} else {
				return compareToNil(nomsVal)
			}
		}, nil
	} else if compType == VariableVariableCompare {
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
		compareToNull := op.CompareToNil

		tag1, tag2 := col1.Tag, col2.Tag
		return func(ctx context.Context, vals map[uint64]types.Value) (b bool, err error) {
			v1 := vals[tag1]
			v2 := vals[tag2]

			if types.IsNull(v1) {
				return compareToNull(v2)
			} else {
				return compareNomsValues(ctx, v1, v2)
			}
		}, nil
	} else if compType == VariableInLiteralList {
		colName := vars[0].Name()
		col, ok := sch.GetAllCols().GetByNameCaseInsensitive(colName)

		if !ok {
			return nil, errUnknownColumn.New(colName)
		}

		tag := col.Tag

		// Get all the noms values
		nomsVals := make([]types.Value, len(consts))
		for i, c := range consts {
			nomsVal, err := LiteralToNomsValue(col.Kind, c)
			if err != nil {
				return nil, err
			}
			nomsVals[i] = nomsVal
		}

		compareNomsValues := op.CompareNomsValues
		compareToNil := op.CompareToNil

		return func(ctx context.Context, vals map[uint64]types.Value) (b bool, err error) {
			colVal, ok := vals[tag]

			for _, nv := range nomsVals {
				var lb bool
				if ok && !types.IsNull(colVal) {
					lb, err = compareNomsValues(ctx, colVal, nv)
				} else {
					lb, err = compareToNil(nv)
				}

				if err != nil {
					return false, err
				}
				if lb {
					return true, nil
				}
			}

			return false, nil
		}, nil
	} else {
		return nil, errUnsupportedComparisonType.New()
	}
}
