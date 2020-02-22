package sqle

import (
	"context"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/expreval"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/setalgebra"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"strings"
)

var eqOp = expreval.EqualsOp{}

func setForEqOp(nbf *types.NomsBinFormat, val types.Value) (setalgebra.Set, error) {
	return setalgebra.NewFiniteSet(nbf, val)
}

var gtOp = expreval.GreaterOp{}

func setForGtOp(nbf *types.NomsBinFormat, val types.Value) (setalgebra.Set, error) {
	return setalgebra.NewInterval(nbf, &setalgebra.IntervalEndpoint{Val: val, Inclusive: false}, nil), nil
}

var gteOp = expreval.GreaterEqualOp{}

func setForGteOp(nbf *types.NomsBinFormat, val types.Value) (setalgebra.Set, error) {
	return setalgebra.NewInterval(nbf, &setalgebra.IntervalEndpoint{Val: val, Inclusive: true}, nil), nil
}

var ltOp = expreval.LessOp{}

func setForLtOp(nbf *types.NomsBinFormat, val types.Value) (setalgebra.Set, error) {
	return setalgebra.NewInterval(nbf, nil, &setalgebra.IntervalEndpoint{Val: val, Inclusive: false}), nil
}

var lteOp = expreval.LessEqualOp{}

func setForLteOp(nbf *types.NomsBinFormat, val types.Value) (setalgebra.Set, error) {
	return setalgebra.NewInterval(nbf, nil, &setalgebra.IntervalEndpoint{Val: val, Inclusive: true}), nil
}

func getSetForKeyColumn(nbf *types.NomsBinFormat, col schema.Column, filter sql.Expression) (setalgebra.Set, error) {
	switch typedExpr := filter.(type) {
	case *expression.Or:
		return getOrSet(nbf, col, typedExpr.Left, typedExpr.Right)
	case *expression.And:
		return getAndSet(nbf, col, typedExpr.Left, typedExpr.Right)
	case *expression.Equals:
		return setForComparisonExp(nbf, col, typedExpr.BinaryExpression, eqOp, setForEqOp)
	case *expression.GreaterThan:
		gtOp.NBF = nbf
		return setForComparisonExp(nbf, col, typedExpr.BinaryExpression, gtOp, setForGtOp)
	case *expression.GreaterThanOrEqual:
		gteOp.NBF = nbf
		return setForComparisonExp(nbf, col, typedExpr.BinaryExpression, gteOp, setForGteOp)
	case *expression.LessThan:
		ltOp.NBF = nbf
		return setForComparisonExp(nbf, col, typedExpr.BinaryExpression, ltOp, setForLtOp)
	case *expression.LessThanOrEqual:
		lteOp.NBF = nbf
		return setForComparisonExp(nbf, col, typedExpr.BinaryExpression, lteOp, setForLteOp)
	case *expression.In:
	}

	return setalgebra.UniversalSet{}, nil
}

func getOrSet(nbf *types.NomsBinFormat, col schema.Column, left, right sql.Expression) (setalgebra.Set, error) {
	leftSet, err := getSetForKeyColumn(nbf, col, left)

	if err != nil {
		return nil, err
	}

	rightSet, err := getSetForKeyColumn(nbf, col, right)

	if err != nil {
		return nil, err
	}

	return leftSet.Union(rightSet)
}

func getAndSet(nbf *types.NomsBinFormat, col schema.Column, left, right sql.Expression) (setalgebra.Set, error) {
	leftSet, err := getSetForKeyColumn(nbf, col, left)

	if err != nil {
		return nil, err
	}

	rightSet, err := getSetForKeyColumn(nbf, col, right)

	if err != nil {
		return nil, err
	}

	return leftSet.Intersect(rightSet)
}

func setForComparisonExp(
	nbf *types.NomsBinFormat,
	col schema.Column,
	be expression.BinaryExpression,
	op expreval.CompareOp,
	createSet func(nbf *types.NomsBinFormat, val types.Value) (setalgebra.Set, error)) (setalgebra.Set, error) {

	variables, literals, compType, err := expreval.GetComparisonType(be)

	if err != nil {
		return nil, err
	}

	switch compType {
	case expreval.ConstConstCompare:
		res, err := op.CompareLiterals(literals[0], literals[1])

		if err != nil {
			return nil, err
		}

		if res {
			return setalgebra.UniversalSet{}, nil
		} else {
			return setalgebra.EmptySet{}, nil
		}

	case expreval.VariableConstCompare:
		if strings.EqualFold(variables[0].Name(), col.Name) {
			val, err := expreval.LiteralToNomsValue(col.Kind, literals[0])

			if err != nil {
				return nil, err
			}

			return createSet(nbf, val)
		} else {
			return setalgebra.UniversalSet{}, nil
		}

	case expreval.VariableVariableCompare:
		return setalgebra.UniversalSet{}, nil
	}

	panic("Unexpected case value")
}

func MapReaderLimitedByExpressions(ctx context.Context, m types.Map, tblSch schema.Schema, filters []sql.Expression) (table.TableReadCloser, error) {
	pkCols := tblSch.GetPKCols()
	nbf := m.Format()
	if pkCols.Size() == 1 {
		var keySet setalgebra.Set = setalgebra.UniversalSet{}
		var err error
		for _, filter := range filters {
			var setForFilter setalgebra.Set
			setForFilter, err = getSetForKeyColumn(nbf, pkCols.GetByIndex(0), filter)

			if err != nil {
				break
			}

			keySet, err = keySet.Intersect(setForFilter)

			if err != nil {
				break
			}
		}

		if err != nil {
			// should probably log this to some debug logger
		} else {
			return getIteratorForKeySet(ctx, keySet, tblSch, m)
		}
	}

	return noms.NewNomsMapReader(ctx, m, tblSch)
}

func finiteSetToKeySlice(nbf *types.NomsBinFormat, tag types.Uint, fs setalgebra.FiniteSet) ([]types.Tuple, error) {
	keys := make([]types.Tuple, len(fs.HashToVal))

	i := 0
	var err error
	for _, v := range fs.HashToVal {
		keys[i], err = types.NewTuple(nbf, tag, v)
		i++

		if err != nil {
			return nil, err
		}
	}

	return keys, nil
}

func rangeForInterval(nbf *types.NomsBinFormat, tag types.Uint, in setalgebra.Interval) (*noms.ReadRange, error) {
	var inclusive bool
	var startVal types.Value
	var reverse bool
	var check noms.InRangeCheck

	if in.Start != nil {
		startVal = in.Start.Val
		inclusive = in.Start.Inclusive

		if in.End == nil {
			check = func(t types.Tuple) (b bool, err error) {
				return true, nil
			}
		} else if in.End.Inclusive {
			check = func(t types.Tuple) (b bool, err error) {
				keyVal, err := t.Get(1)

				if err != nil {
					return false, err
				}

				eq := keyVal.Equals(in.End.Val)

				if eq {
					return true, nil
				}

				return keyVal.Less(nbf, in.End.Val)
			}
		} else {
			check = func(t types.Tuple) (b bool, err error) {
				keyVal, err := t.Get(1)

				if err != nil {
					return false, err
				}

				return keyVal.Less(nbf, in.End.Val)
			}
		}
	} else {
		startVal = in.End.Val
		inclusive = in.End.Inclusive
		reverse = true

		check = func(tuple types.Tuple) (b bool, err error) {
			return true, nil
		}
	}

	startKey, err := types.NewTuple(nbf, tag, startVal)

	if err != nil {
		return nil, err
	}

	return &noms.ReadRange{Start: startKey, Inclusive: inclusive, Reverse: reverse, Check: check}, nil
}

func getIteratorForKeySet(ctx context.Context, keySet setalgebra.Set, sch schema.Schema, m types.Map) (table.TableReadCloser, error) {
	pkCols := sch.GetPKCols()

	if pkCols.Size() != 1 {
		panic("not implemented yet")
	}

	col := pkCols.GetByIndex(0)

	switch typedSet := keySet.(type) {
	case setalgebra.EmptySet:
		return noms.NewNomsMapReaderForKeys(m, sch, []types.Tuple{}), nil

	case setalgebra.FiniteSet:
		keys, err := finiteSetToKeySlice(m.Format(), types.Uint(col.Tag), typedSet)

		if err != nil {
			return nil, err
		}

		return noms.NewNomsMapReaderForKeys(m, sch, keys), nil

	case setalgebra.Interval:
		r, err := rangeForInterval(m.Format(), types.Uint(col.Tag), typedSet)

		if err != nil {
			return nil, err
		}

		return noms.NewNomsRangeReader(sch, m, []*noms.ReadRange{r}), nil

	case setalgebra.CompositeSet:
		var ranges []*noms.ReadRange
		for _, interval := range typedSet.Intervals {
			r, err := rangeForInterval(m.Format(), types.Uint(col.Tag), interval)

			if err != nil {
				return nil, err
			}

			ranges = append(ranges, r)
		}

		var readers = []table.TableReadCloser{noms.NewNomsRangeReader(sch, m, ranges)}

		if len(typedSet.Set.HashToVal) > 0 {
			keys, err := finiteSetToKeySlice(m.Format(), types.Uint(col.Tag), typedSet.Set)

			if err != nil {
				return nil, err
			}

			rd := noms.NewNomsMapReaderForKeys(m, sch, keys)
			readers = append(readers, rd)
		}

		return table.NewCompositeTableReader(readers)

	default:
		return noms.NewNomsMapReader(ctx, m, sch)
	}
}
