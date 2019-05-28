package sql

import (
	"context"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/resultset"
	"github.com/xwb1989/sqlparser"
	"strconv"
)

// binaryNomsOperation knows how to combine two noms values into a single one, e.g. addition
type binaryNomsOperation func(left, right types.Value) types.Value
type unaryNomsOperation func(val types.Value) types.Value

type valGetterKind uint8

const (
	COLNAME valGetterKind = iota
	SQL_VAL
	BOOL_VAL
)

// RowValGetter knows how to retrieve a Value from a Row.
type RowValGetter struct {
	// The kind of this val getter.
	Kind valGetterKind
	// The value type returned by this getter.
	NomsKind types.NomsKind
	// The kind of the value that this getter's result will be compared against, filled in elsewhere.
	CmpKind types.NomsKind
	// Init() returns any error that would be caused by calling Get() on this row.
	Init func() error
	// Get() returns the value for this getter for the row given
	Get func(r row.Row) types.Value
	// CachedVal is a handy place to put a pre-computed value for getters that deal with constants or literals
	CachedVal types.Value
}

// Returns a comparison value getter for the expression given, which could be a column value or a literal
func getterFor(expr sqlparser.Expr, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (*RowValGetter, error) {
	switch e := expr.(type) {
	case *sqlparser.NullVal:
		getter := RowValGetter{Kind: SQL_VAL}
		getter.Init = func() error { return nil }
		getter.Get = func(r row.Row) types.Value { return nil }
		return &getter, nil
	case *sqlparser.ColName:
		colNameStr := getColumnNameString(e)

		qc, err := resolveColumn(colNameStr, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		tableSch, ok := inputSchemas[qc.TableName]
		if !ok {
			return nil, errFmt("Unresolved table %v", qc.TableName)
		}

		column, ok := tableSch.GetAllCols().GetByName(qc.ColumnName)
		if !ok {
			return nil, errFmt(UnknownColumnErrFmt, colNameStr)
		}
		resultSetTag := rss.Mapping(tableSch).SrcToDest[column.Tag]

		getter := RowValGetter{Kind: COLNAME, NomsKind: column.Kind}
		getter.Init = func() error {
			return nil
		}
		getter.Get = func(r row.Row) types.Value {
			value, _ := r.GetColVal(resultSetTag)
			return value
		}

		return &getter, nil
	case *sqlparser.SQLVal:
		getter := RowValGetter{Kind: SQL_VAL}

		getter.Init = func() error {
			val, err := extractNomsValueFromSQLVal(e, getter.CmpKind)
			if err != nil {
				return err
			}
			getter.CachedVal = val
			return nil
		}
		getter.Get = func(r row.Row) types.Value {
			return getter.CachedVal
		}

		return &getter, nil
	case sqlparser.BoolVal:
		val := types.Bool(bool(e))
		getter := RowValGetter{Kind: BOOL_VAL, NomsKind: types.BoolKind}

		getter.Init = func() error {
			switch getter.CmpKind {
			case types.BoolKind:
				return nil
			default:
				return errFmt("Type mismatch: boolean value but non-numeric column: %v", nodeToString(e))
			}
		}
		getter.Get = func(r row.Row) types.Value {
			return val
		}

		return &getter, nil
	case sqlparser.ValTuple:
		getter := RowValGetter{Kind: SQL_VAL}

		getter.Init = func() error {
			vals := make([]types.Value, len(e))
			for i, item := range e {
				switch v := item.(type) {
				case *sqlparser.SQLVal:
					if val, err := extractNomsValueFromSQLVal(v, getter.CmpKind); err != nil {
						return err
					} else {
						vals[i] = val
					}
				default:
					return errFmt("Unsupported list literal: %v", nodeToString(v))
				}
			}

			// TODO: surely there is a better way to do this without resorting to interface{}
			ts := &chunks.TestStorage{}
			vs := types.NewValueStore(ts.NewView())
			set := types.NewSet(context.Background(), vs, vals...)

			getter.CachedVal = set
			return nil
		}
		getter.Get = func(r row.Row) types.Value {
			return getter.CachedVal
		}

		return &getter, nil
	case *sqlparser.BinaryExpr:
		getter, err := getterForBinaryExpr(e, inputSchemas, aliases, rss)
		if err != nil {
			return nil, err
		}
		return getter, nil
	case *sqlparser.UnaryExpr:
		getter, err := getterForUnaryExpr(e, inputSchemas, aliases, rss)
		if err != nil {
			return nil, err
		}
		return getter, nil
	default:
		return nil, errFmt("Unsupported type %v", nodeToString(e))
	}
}

// getterForUnaryExpr returns a getter for the given unary expression, where calls to Get() evaluates the full
// expression for the row given
func getterForUnaryExpr(e *sqlparser.UnaryExpr, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (*RowValGetter, error) {
	getter, err := getterFor(e.Expr, inputSchemas, aliases, rss)
	if err != nil {
		return nil, err
	}

	var opFn unaryNomsOperation
	switch e.Operator {
	case sqlparser.UPlusStr:
		switch getter.NomsKind {
		case types.IntKind, types.FloatKind:
			// fine, nothing to do
		default:
			return nil, errFmt("Unsupported type for unary + operation: %v", types.KindToString[getter.NomsKind])
		}
		opFn = func(val types.Value) types.Value {
			return val
		}
	case sqlparser.UMinusStr:
		switch getter.NomsKind {
		case types.IntKind:
			opFn = func(val types.Value) types.Value {
				if types.IsNull(val) {
					return nil
				}
				return types.Int(-1 * val.(types.Int))
			}
		case types.FloatKind:
			opFn = func(val types.Value) types.Value {
				if types.IsNull(val) {
					return nil
				}
				return types.Float(-1 * val.(types.Float))
			}
		case types.UintKind:
			// TODO: this alters the type of the expression returned relative to the column's.
			//  This probably causes some problems.
			opFn = func(val types.Value) types.Value {
				if types.IsNull(val) {
					return nil
				}
				return types.Int(-1 * int64(val.(types.Uint)))
			}
		default:
			return nil, errFmt("Unsupported type for unary - operation: %v", types.KindToString[getter.NomsKind])
		}
	case sqlparser.BangStr:
		switch getter.NomsKind {
		case types.BoolKind:
			opFn = func(val types.Value) types.Value {
				return types.Bool(!val.(types.Bool))
			}
		default:
			return nil, errFmt("Unsupported type for unary ! operation: %v", types.KindToString[getter.NomsKind])
		}
	default:
		return nil, errFmt("Unsupported unary operation: %v", e.Operator)
	}

	unaryGetter := RowValGetter{}

	unaryGetter.Init = func() error {
		// Already did type checking explicitly
		return nil
	}

	unaryGetter.Get = func(r row.Row) types.Value {
		return opFn(getter.Get(r))
	}

	return &unaryGetter, nil
}

// getterForBinaryExpr returns a getter for the given binary expression, where calls to Get() evaluates the full
// expression for the row given
func getterForBinaryExpr(e *sqlparser.BinaryExpr, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (*RowValGetter, error) {
	leftGetter, err := getterFor(e.Left, inputSchemas, aliases, rss)
	if err != nil {
		return nil, err
	}
	rightGetter, err := getterFor(e.Right, inputSchemas, aliases, rss)
	if err != nil {
		return nil, err
	}

	// Fill in target noms kinds for SQL_VAL fields if possible
	if leftGetter.Kind == SQL_VAL && rightGetter.Kind != SQL_VAL {
		leftGetter.NomsKind = rightGetter.NomsKind
	}
	if rightGetter.Kind == SQL_VAL && leftGetter.Kind != SQL_VAL {
		rightGetter.NomsKind = leftGetter.NomsKind
	}

	if rightGetter.NomsKind != leftGetter.NomsKind {
		return nil, errFmt("Unsupported binary operation types: %v, %v", types.KindToString[leftGetter.NomsKind], types.KindToString[rightGetter.NomsKind])
	}

	// Fill in comparison kinds before doing error checking
	rightGetter.CmpKind, leftGetter.CmpKind = leftGetter.NomsKind, rightGetter.NomsKind

	// Initialize the getters. This uses the type hints from above to enforce type constraints between columns and
	// literals.
	if err := leftGetter.Init(); err != nil {
		return nil, err
	}
	if err := rightGetter.Init(); err != nil {
		return nil, err
	}

	getter := RowValGetter{Kind: SQL_VAL, NomsKind: leftGetter.NomsKind, CmpKind: rightGetter.NomsKind}

	// All the operations differ only in their filter logic
	var opFn binaryNomsOperation
	switch e.Operator {
	case sqlparser.PlusStr:
		switch getter.NomsKind {
		case types.UintKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Uint(uint64(left.(types.Int)) + uint64(right.(types.Int)))
			}
		case types.IntKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Int(int64(left.(types.Int)) + int64(right.(types.Int)))
			}
		case types.FloatKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Float(float64(left.(types.Float)) + float64(right.(types.Float)))
			}
		default:
			return nil, errFmt("Unsupported type for + operation: %v", types.KindToString[getter.NomsKind])
		}
	case sqlparser.MinusStr:
		switch getter.NomsKind {
		case types.UintKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Uint(uint64(left.(types.Int)) - uint64(right.(types.Int)))
			}
		case types.IntKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Int(int64(left.(types.Int)) - int64(right.(types.Int)))
			}
		case types.FloatKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Float(float64(left.(types.Float)) - float64(right.(types.Float)))
			}
		default:
			return nil, errFmt("Unsupported type for - operation: %v", types.KindToString[getter.NomsKind])
		}
	case sqlparser.MultStr:
		switch getter.NomsKind {
		case types.UintKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Uint(uint64(left.(types.Int)) * uint64(right.(types.Int)))
			}
		case types.IntKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Int(int64(left.(types.Int)) * int64(right.(types.Int)))
			}
		case types.FloatKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Float(float64(left.(types.Float)) * float64(right.(types.Float)))
			}
		default:
			return nil, errFmt("Unsupported type for * operation: %v", types.KindToString[getter.NomsKind])
		}
	case sqlparser.DivStr:
		switch getter.NomsKind {
		case types.UintKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Uint(uint64(left.(types.Int)) / uint64(right.(types.Int)))
			}
		case types.IntKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Int(int64(left.(types.Int)) / int64(right.(types.Int)))
			}
		case types.FloatKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Float(float64(left.(types.Float)) / float64(right.(types.Float)))
			}
		default:
			return nil, errFmt("Unsupported type for / operation: %v", types.KindToString[getter.NomsKind])
		}
	case sqlparser.ModStr:
		switch getter.NomsKind {
		case types.UintKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Uint(uint64(left.(types.Int)) % uint64(right.(types.Int)))
			}
		case types.IntKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Int(int64(left.(types.Int)) % int64(right.(types.Int)))
			}
		default:
			return nil, errFmt("Unsupported type for %% operation: %v", types.KindToString[getter.NomsKind])
		}
	default:
		return nil, errFmt("Unsupported binary operation: %v", e.Operator)
	}

	getter.Init = func() error {
		// Already did type checking explicitly
		return nil
	}

	getter.Get = func(r row.Row) types.Value {
		leftVal := leftGetter.Get(r)
		rightVal := rightGetter.Get(r)
		if types.IsNull(leftVal) || types.IsNull(rightVal) {
			return nil
		}
		return opFn(leftVal, rightVal)
	}

	return &getter, nil
}

// extractNomsValueFromSQLVal extracts a noms value from the given SQLVal, using type info in the dolt column given as
// a hint and for type-checking
func extractNomsValueFromSQLVal(val *sqlparser.SQLVal, kind types.NomsKind) (types.Value, error) {
	switch val.Type {
	// Integer-like values
	case sqlparser.HexVal, sqlparser.HexNum, sqlparser.IntVal, sqlparser.BitVal:
		intVal, err := strconv.ParseInt(string(val.Val), 0, 64)
		if err != nil {
			return nil, err
		}
		switch kind {
		case types.IntKind:
			return types.Int(intVal), nil
		case types.FloatKind:
			return types.Float(intVal), nil
		case types.UintKind:
			return types.Uint(intVal), nil
		default:
			return nil, errFmt("Type mismatch: numeric value but non-numeric column: %v", nodeToString(val))
		}
	// Float values
	case sqlparser.FloatVal:
		floatVal, err := strconv.ParseFloat(string(val.Val), 64)
		if err != nil {
			return nil, err
		}
		switch kind {
		case types.FloatKind:
			return types.Float(floatVal), nil
		default:
			return nil, errFmt("Type mismatch: float value but non-float column: %v", nodeToString(val))
		}
	// Strings, which can be coerced into lots of other types
	case sqlparser.StrVal:
		strVal := string(val.Val)
		switch kind {
		case types.StringKind:
			return types.String(strVal), nil
		case types.UUIDKind:
			id, err := uuid.Parse(strVal)
			if err != nil {
				return nil, errFmt("Type mismatch: string value but non-string column: %v", nodeToString(val))
			}
			return types.UUID(id), nil
		default:
			return nil, errFmt("Type mismatch: string value but non-string column: %v", nodeToString(val))
		}
	case sqlparser.ValArg:
		return nil, errFmt("Value args not supported")
	default:
		return nil, errFmt("Unrecognized SQLVal type %v", val.Type)
	}
}

// extractNomsValueFromUnaryExpr extracts a noms value from the given expression, using the type info given as
// a hint and for type-checking. The underlying expression must be a SQLVal
func extractNomsValueFromUnaryExpr(expr *sqlparser.UnaryExpr, kind types.NomsKind) (types.Value, error) {
	sqlVal, ok := expr.Expr.(*sqlparser.SQLVal)
	if !ok {
		return nil, errFmt("Only SQL values are supported in unary expressions: %v", nodeToString(expr))
	}

	val, err := extractNomsValueFromSQLVal(sqlVal, kind)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.UPlusStr:
		switch kind {
		case types.UintKind, types.IntKind, types.FloatKind:
			return val, nil
		default:
			return nil, errFmt("Unsupported type for unary + operator: %v", nodeToString(expr))
		}
	case sqlparser.UMinusStr:
		switch kind {
		case types.UintKind:
			return nil, errFmt("Cannot use unary - with for an unsigned value: %v", nodeToString(expr))
		case types.IntKind:
			return types.Int(-1 * val.(types.Int)), nil
		case types.FloatKind:
			return types.Float(-1 * val.(types.Float)), nil
		default:
			return nil, errFmt("Unsupported type for unary - operator: %v", nodeToString(expr))
		}
	case sqlparser.BangStr:
		switch kind {
		case types.BoolKind:
			return types.Bool(!val.(types.Bool)), nil
		default:
			return nil, errFmt("Unsupported type for unary ! operator: '%v'", nodeToString(expr))
		}
	default:
		return nil, errFmt("Unsupported unary operator %v in expression: '%v'", expr.Operator, nodeToString(expr))
	}
}
