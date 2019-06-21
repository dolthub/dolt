package sql

import (
	"context"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/xwb1989/sqlparser"
	"strconv"
)

// binaryNomsOperation knows how to combine two noms values into a single one, e.g. addition
type binaryNomsOperation func(left, right types.Value) types.Value

// predicate function for two noms values, e.g. <
type binaryNomsPredicate func(left, right types.Value) bool

// unaryNomsOperation knows how to turn a single noms value into another one, e.g. negation
type unaryNomsOperation func(val types.Value) types.Value

// TagResolver knows how to find a tag number for a qualified table in a result set.
type TagResolver interface {
	ResolveTag(tableName string, columnName string) (uint64, error)
}

// InitValue is a value type that knows how to initialize itself before the value is retrieved.
type InitValue interface {
	// Init() resolves late-bound information for this getter, like the tag number of a column in a result set. Returns
	// any error in initialization. Init() must be called before other methods on an object.
	Init(TagResolver) error
}

// Composes zero or more InitValue into a new Init() function, where Init() is called on each InitValue in turn,
// returning any error encountered.
func ComposeInits(ivs ...InitValue) func(TagResolver) error {
	return func(resolver TagResolver) error {
		for _, iv := range ivs {
			if err := iv.Init(resolver); err != nil {
				return err
			}
		}
		return nil
	}
}

// GetValue is a value type that know how to retrieve a value from a row.
type GetValue interface {
	// Get() returns a value from the row given (which needn't actually be a value from that row).
	Get(row.Row) types.Value
}

// RowValGetter knows how to retrieve a Value from a Row.
type RowValGetter struct {
	// The value type returned by this getter. Types are approximate and may need to be coerced, e.g. Float -> Int.
	NomsKind types.NomsKind
	// initFn performs whatever logic necessary to initialize the getter, and returns any errors in the initialization
	// process. Leave unset to perform no initialization logic. Client should call the interface method Init() rather than
	// calling this method directly.
	initFn func(TagResolver) error
	// getFn returns the value for this getter for the row given. Clients should call the interface method Get() rather
	// than calling this method directly.
	getFn func(r row.Row) types.Value
	// Whether this value has been initialized.
	inited bool
	// Clients should use these interface methods, rather than getFn and initFn directly.
	InitValue
	GetValue
}

func (rvg *RowValGetter) Init(resolver TagResolver) error {
	rvg.inited = true
	if rvg.initFn != nil {
		return rvg.initFn(resolver)
	}
	return nil
}

func (rvg *RowValGetter) Get(r row.Row) types.Value {
	// TODO: find a way to not impede performance with this check
	if !rvg.inited {
		panic("Get() called without Init(). This is a bug.")
	}
	return rvg.getFn(r)
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
		getFn: func(r row.Row) types.Value {
			val := getter.Get(r)
			return converterFn(val)
		},
		initFn: func(resolver TagResolver) error {
			return getter.Init(resolver)
		},
	}, nil
}

// Returns a new RowValGetter for the literal value given.
func LiteralValueGetter(value types.Value) *RowValGetter {
	return &RowValGetter{
		NomsKind: value.Kind(),
		getFn: func(r row.Row) types.Value {
			return value
		},
	}
}

// Returns a RowValGetter for the column given, or an error
func getterForColumn(qc QualifiedColumn, inputSchemas map[string]schema.Schema) (*RowValGetter, error) {
	tableSch, ok := inputSchemas[qc.TableName]
	if !ok {
		return nil, errFmt("Unresolved table %v", qc.TableName)
	}

	column, ok := tableSch.GetAllCols().GetByName(qc.ColumnName)
	if !ok {
		return nil, errFmt(UnknownColumnErrFmt, qc.ColumnName)
	}

	getter := RowValGetterForKind(column.Kind)

	var tag uint64
	getter.initFn = func(resolver TagResolver) error {
		var err error
		tag, err = resolver.ResolveTag(qc.TableName, qc.ColumnName)
		return err
	}
	getter.getFn = func(r row.Row) types.Value {
		value, _ := r.GetColVal(tag)
		return value
	}

	return getter, nil
}

// nullSafeBoolOp applies null checking semantics to a binary expression, so that if either of the two operands are
// null, the expression is null. Callers supply left and right RowValGetters and a predicate function for the extracted
// non-null row values.
func nullSafeBoolOp(left, right *RowValGetter, fn binaryNomsPredicate) func(r row.Row) types.Value {
	return func(r row.Row) types.Value {
		leftVal := left.Get(r)
		rightVal := right.Get(r)
		if types.IsNull(leftVal) || types.IsNull(rightVal) {
			return nil
		}
		return types.Bool(fn(leftVal, rightVal))
	}
}

// Returns RowValGetter for the expression given, or an error
func getterFor(expr sqlparser.Expr, inputSchemas map[string]schema.Schema, aliases *Aliases) (*RowValGetter, error) {
	switch e := expr.(type) {
	case *sqlparser.NullVal:
		getter := RowValGetterForKind(types.NullKind)
		getter.getFn = func(r row.Row) types.Value { return nil }
		return getter, nil

	case *sqlparser.ColName:
		colNameStr := getColumnNameString(e)

		if getter, err := resolveColumnAlias(colNameStr, aliases); err != nil {
			return nil, err
		} else if getter != nil {
			return getter, nil
		}

		qc, err := resolveColumn(colNameStr, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}

		return getterForColumn(qc, inputSchemas)
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

		// TODO: combine with CreateFilterForWhere
	case *sqlparser.ComparisonExpr:

		leftGetter, err := getterFor(e.Left, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		rightGetter, err := getterFor(e.Right, inputSchemas, aliases)
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

		getter := RowValGetterForKind(types.BoolKind)
		var predicate binaryNomsPredicate
		switch e.Operator {
		case sqlparser.EqualStr:
			predicate = func(left, right types.Value) bool {
				return left.Equals(right)
			}
		case sqlparser.LessThanStr:
			predicate = func(left, right types.Value) bool {
				return left.Less(right)
			}
		case sqlparser.GreaterThanStr:
			predicate = func(left, right types.Value) bool {
				return right.Less(left)
			}
		case sqlparser.LessEqualStr:
			predicate = func(left, right types.Value) bool {
				return left.Less(right) || left.Equals(right)
			}
		case sqlparser.GreaterEqualStr:
			predicate = func(left, right types.Value) bool {
				return right.Less(left) || right.Equals(left)
			}
		case sqlparser.NotEqualStr:
			predicate = func(left, right types.Value) bool {
				return !left.Equals(right)
			}
		case sqlparser.InStr:
			predicate = func(left, right types.Value) bool {
				set := right.(types.Set)
				return set.Has(context.Background(), left)
			}
		case sqlparser.NotInStr:
			predicate = func(left, right types.Value) bool {
				set := right.(types.Set)
				return !set.Has(context.Background(), left)
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

		getter.getFn = nullSafeBoolOp(leftGetter, rightGetter, predicate)
		getter.initFn = ComposeInits(leftGetter, rightGetter)
		return getter, nil

	case *sqlparser.AndExpr:
		leftGetter, err := getterFor(e.Left, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		rightGetter, err := getterFor(e.Right, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}

		getter := RowValGetterForKind(types.BoolKind)
		getter.getFn = nullSafeBoolOp(leftGetter, rightGetter, func(left, right types.Value) bool {
			return bool(left.(types.Bool) && right.(types.Bool))
		})
		getter.initFn = ComposeInits(leftGetter, rightGetter)
		return getter, nil

	case *sqlparser.OrExpr:
		leftGetter, err := getterFor(e.Left, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}
		rightGetter, err := getterFor(e.Right, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}

		getter := RowValGetterForKind(types.BoolKind)
		getter.getFn = nullSafeBoolOp(leftGetter, rightGetter, func(left, right types.Value) bool {
			return bool(left.(types.Bool) || right.(types.Bool))
		})
		getter.initFn = ComposeInits(leftGetter, rightGetter)
		return getter, nil

	case *sqlparser.IsExpr:
		exprGetter, err := getterFor(e.Expr, inputSchemas, aliases)
		if err != nil {
			return nil, err
		}

		getter := RowValGetterForKind(types.BoolKind)
		getter.initFn = ComposeInits(exprGetter)

		op := e.Operator
		switch op {
		case sqlparser.IsNullStr, sqlparser.IsNotNullStr:
			getter.getFn = func(r row.Row) types.Value {
				val := exprGetter.Get(r)
				if (types.IsNull(val) && op == sqlparser.IsNullStr) || (!types.IsNull(val) && op == sqlparser.IsNotNullStr) {
					return types.Bool(true)
				}
				return types.Bool(false)
			}

		case sqlparser.IsTrueStr, sqlparser.IsNotTrueStr, sqlparser.IsFalseStr, sqlparser.IsNotFalseStr:
			if exprGetter.NomsKind != types.BoolKind {
				return nil, errFmt("Type mismatch: cannot use expression %v as boolean", nodeToString(expr))
			}

			getter.getFn = func(r row.Row) types.Value {
				val := exprGetter.Get(r)
				if types.IsNull(val) {
					return types.Bool(false)
				}
				// TODO: this may not be the correct nullness semantics for "is not" comparisons
				if val.Equals(types.Bool(true)) {
					return types.Bool(op == sqlparser.IsTrueStr || op == sqlparser.IsNotFalseStr)
				} else {
					return types.Bool(op == sqlparser.IsFalseStr || op == sqlparser.IsNotTrueStr)
				}
			}

		default:
			return nil, errFmt("Unrecognized is comparison: %v", e.Operator)
		}

		return getter, nil

	case *sqlparser.BinaryExpr:
		return getterForBinaryExpr(e, inputSchemas, aliases)
	case *sqlparser.UnaryExpr:
		return getterForUnaryExpr(e, inputSchemas, aliases)
	default:
		return nil, errFmt("Unsupported expression: '%v'", nodeToString(e))
	}
}

// getterForUnaryExpr returns a getter for the given unary expression, where calls to Get() evaluates the full
// expression for the row given
func getterForUnaryExpr(e *sqlparser.UnaryExpr, inputSchemas map[string]schema.Schema, aliases *Aliases) (*RowValGetter, error) {
	getter, err := getterFor(e.Expr, inputSchemas, aliases)
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
	unaryGetter.getFn = func(r row.Row) types.Value {
		return opFn(getter.Get(r))
	}
	unaryGetter.initFn = func(resolver TagResolver) error {
		return getter.Init(resolver)
	}

	return unaryGetter, nil
}

// getterForBinaryExpr returns a getter for the given binary expression, where calls to Get() evaluates the full
// expression for the row given
func getterForBinaryExpr(e *sqlparser.BinaryExpr, inputSchemas map[string]schema.Schema, aliases *Aliases) (*RowValGetter, error) {
	leftGetter, err := getterFor(e.Left, inputSchemas, aliases)
	if err != nil {
		return nil, err
	}
	rightGetter, err := getterFor(e.Right, inputSchemas, aliases)
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
	getter.getFn = func(r row.Row) types.Value {
		leftVal := leftGetter.Get(r)
		rightVal := rightGetter.Get(r)
		if types.IsNull(leftVal) || types.IsNull(rightVal) {
			return nil
		}
		return opFn(leftVal, rightVal)
	}
	getter.initFn = func(resolver TagResolver) error {
		if err := leftGetter.Init(resolver); err != nil {
			return err
		}
		if err := rightGetter.Init(resolver); err != nil {
			return err
		}
		return nil
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
