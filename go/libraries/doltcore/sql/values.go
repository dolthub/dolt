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

// RowValGetter knows how to retrieve a Value from a Row.
type RowValGetter struct {
	// The value type returned by this getter. Types are approximate and may need to be coerced, e.g. Float -> Int.
	NomsKind types.NomsKind
	// Get() returns the value for this getter for the row given
	Get func(r row.Row) types.Value
}

// Returns a new RowValGetter with default values filled in.
func RowValGetterForKind(kind types.NomsKind) *RowValGetter {
	return &RowValGetter{
		NomsKind: kind,
	}
}

// Returns a new RowValGetter that wraps the given one, converting to the appropriate type as necessary. Returns an
// error if no conversion between the types is possible.
func ConversionValueGetter(getter *RowValGetter, destKind types.NomsKind) (*RowValGetter, error) {
	if getter.NomsKind == destKind {
		return getter, nil
	}

	converterFn := GetTypeConversionFn(getter.NomsKind, destKind)
	if converterFn == nil {
		return nil, errFmt("Type mismatch: cannot convert from %v to %v",
			DoltToSQLType[getter.NomsKind], DoltToSQLType[destKind])
	}

	return &RowValGetter{
		NomsKind: destKind,
		Get: func(r row.Row) types.Value {
			val := getter.Get(r)
			return converterFn(val)
		},
	}, nil
}

// Returns a new RowValGetter for the literal value given.
func LiteralValueGetter(value types.Value) *RowValGetter {
	return &RowValGetter{
		NomsKind: value.Kind(),
		Get: func(r row.Row) types.Value {
			return value
		},
	}
}

// Returns a comparison value getter for the expression given, which could be a column value or a literal
func getterFor(expr sqlparser.Expr, inputSchemas map[string]schema.Schema, aliases *Aliases, rss *resultset.ResultSetSchema) (*RowValGetter, error) {
	switch e := expr.(type) {
	case *sqlparser.NullVal:
		getter := RowValGetterForKind(types.NullKind)
		getter.Get = func(r row.Row) types.Value { return nil }
		return getter, nil

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

		getter := RowValGetterForKind(column.Kind)
		getter.Get = func(r row.Row) types.Value {
			value, _ := r.GetColVal(resultSetTag)
			return value
		}

		return getter, nil

	case *sqlparser.SQLVal:
		val, err := divineNomsValueFromSQLVal(e)
		if err != nil {
			return nil, err
		}

		return LiteralValueGetter(val), nil

	case sqlparser.BoolVal:
		val := types.Bool(bool(e))
		return LiteralValueGetter(val), nil

	case sqlparser.ValTuple:
		vals := make([]types.Value, len(e))
		var kind types.NomsKind
		for i, item := range e {
			switch v := item.(type) {
			case *sqlparser.SQLVal:
				if val, err := divineNomsValueFromSQLVal(v); err != nil {
					return nil, err
				} else {
					if i > 0 && kind != val.Kind() {
						return nil, errFmt("Type mismatch: mixed types in list literal '%v'", nodeToString(e))
					}
					vals[i] = val
					kind = val.Kind()
				}
			default:
				return nil, errFmt("Unsupported list literal: %v", nodeToString(v))
			}
		}

		// TODO: surely there is a better way to do this without resorting to interface{}
		ts := &chunks.TestStorage{}
		vs := types.NewValueStore(ts.NewView())
		set := types.NewSet(context.Background(), vs, vals...)

		// TODO: better type checking (set type is not literally the underlying type)
		getter := LiteralValueGetter(set)
		getter.NomsKind = kind
		return getter, nil

	case *sqlparser.BinaryExpr:
		return getterForBinaryExpr(e, inputSchemas, aliases, rss)
	case *sqlparser.UnaryExpr:
		return getterForUnaryExpr(e, inputSchemas, aliases, rss)
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
			return nil, errFmt("Unsupported type for unary + operation: %v", DoltToSQLType[getter.NomsKind])
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
			return nil, errFmt("Unsupported type for unary - operation: %v", DoltToSQLType[getter.NomsKind])
		}
	case sqlparser.BangStr:
		switch getter.NomsKind {
		case types.BoolKind:
			opFn = func(val types.Value) types.Value {
				return types.Bool(!val.(types.Bool))
			}
		default:
			return nil, errFmt("Unsupported type for unary ! operation: %v", DoltToSQLType[getter.NomsKind])
		}
	default:
		return nil, errFmt("Unsupported unary operation: %v", e.Operator)
	}

	unaryGetter := RowValGetterForKind(getter.NomsKind)
	unaryGetter.Get = func(r row.Row) types.Value {
		return opFn(getter.Get(r))
	}

	return unaryGetter, nil
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

	// TODO: support type conversion
	if rightGetter.NomsKind != leftGetter.NomsKind {
		return nil, errFmt("Type mismatch evaluating expression '%v': cannot compare %v, %v",
			nodeToString(e), DoltToSQLType[leftGetter.NomsKind], DoltToSQLType[rightGetter.NomsKind])
	}

	// All the operations differ only in their filter logic
	var opFn binaryNomsOperation
	switch e.Operator {
	case sqlparser.PlusStr:
		switch leftGetter.NomsKind {
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
			return nil, errFmt("Unsupported type for + operation: %v", DoltToSQLType[leftGetter.NomsKind])
		}
	case sqlparser.MinusStr:
		switch leftGetter.NomsKind {
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
			return nil, errFmt("Unsupported type for - operation: %v", DoltToSQLType[leftGetter.NomsKind])
		}
	case sqlparser.MultStr:
		switch leftGetter.NomsKind {
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
			return nil, errFmt("Unsupported type for * operation: %v", DoltToSQLType[leftGetter.NomsKind])
		}
	case sqlparser.DivStr:
		switch leftGetter.NomsKind {
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
			return nil, errFmt("Unsupported type for / operation: %v", DoltToSQLType[leftGetter.NomsKind])
		}
	case sqlparser.ModStr:
		switch leftGetter.NomsKind {
		case types.UintKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Uint(uint64(left.(types.Int)) % uint64(right.(types.Int)))
			}
		case types.IntKind:
			opFn = func(left, right types.Value) types.Value {
				return types.Int(int64(left.(types.Int)) % int64(right.(types.Int)))
			}
		default:
			return nil, errFmt("Unsupported type for %% operation: %v", DoltToSQLType[leftGetter.NomsKind])
		}
	default:
		return nil, errFmt("Unsupported binary operation: %v", e.Operator)
	}

	getter := RowValGetterForKind(leftGetter.NomsKind)
	getter.Get = func(r row.Row) types.Value {
		leftVal := leftGetter.Get(r)
		rightVal := rightGetter.Get(r)
		if types.IsNull(leftVal) || types.IsNull(rightVal) {
			return nil
		}
		return opFn(leftVal, rightVal)
	}

	return getter, nil
}

// Attempts to divine a value and type from the given SQLVal expression. Returns the value or an error.
// The most specific possible type is returned, e.g. Float over Int. Unsigned values are never returned.
func divineNomsValueFromSQLVal(val *sqlparser.SQLVal) (types.Value, error) {
	switch val.Type {
	// Integer-like values
	case sqlparser.HexVal, sqlparser.HexNum, sqlparser.IntVal, sqlparser.BitVal:
		intVal, err := strconv.ParseInt(string(val.Val), 0, 64)
		if err != nil {
			return nil, err
		}
		return types.Int(intVal), nil
	// Float values
	case sqlparser.FloatVal:
		floatVal, err := strconv.ParseFloat(string(val.Val), 64)
		if err != nil {
			return nil, err
		}
		return types.Float(floatVal), nil
	// Strings, which can be coerced into UUIDs
	case sqlparser.StrVal:
		strVal := string(val.Val)
		if id, err := uuid.Parse(strVal); err == nil {
			return types.UUID(id), nil
		} else {
			return types.String(strVal), nil
		}
	case sqlparser.ValArg:
		return nil, errFmt("Value args not supported")
	default:
		return nil, errFmt("Unrecognized SQLVal type %v", val.Type)
	}
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
	// Strings, which can be coerced into UUIDs
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
