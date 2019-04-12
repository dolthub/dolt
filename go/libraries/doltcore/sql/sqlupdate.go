package sql

import (
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/xwb1989/sqlparser"
	"io"
	"strconv"
)

type UpdateResult struct {
	Root             *doltdb.RootValue
	NumRowsUpdated   int
	NumErrorsIgnored int
	NumRowsUnchanged int
	// TODO: update ignore not supported by parser yet
}

func ExecuteUpdate(db *doltdb.DoltDB, root *doltdb.RootValue, s *sqlparser.Update, query string)  (*UpdateResult, error) {
	tableExprs := s.TableExprs
	if len(tableExprs) != 1 {
		return errUpdate("Exactly one table to update must be specified")
	}

	var tableName string
	tableExpr := tableExprs[0]
	switch t := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch e := t.Expr.(type) {
		case sqlparser.TableName:
			tableName = e.Name.String()
		case *sqlparser.Subquery:
			return errUpdate("Subqueries are not supported: %v.", query)
		default:
			return errUpdate("Unrecognized expression: %v", nodeToString(e))
		}
	case *sqlparser.ParenTableExpr:
		return errUpdate("Only simple table expression are supported")
	case *sqlparser.JoinTableExpr:
		return errUpdate("Joins are not supported")
	default:
		return errUpdate("Unsupported update statement %v", query)
	}

	if !root.HasTable(tableName) {
		return errUpdate("Unknown table '%s'", tableName)
	}
	table, _:= root.GetTable(tableName)
	tableSch := table.GetSchema()

	// map of column tag to value
	setVals := make(map[uint64]types.Value)

	for _, update := range s.Exprs {
		colName := update.Name.Name.String()
		column, ok := tableSch.GetAllCols().GetByName(colName)
		if !ok {
			return errUpdate("Unknown column %v", colName)
		}
		if _, ok = setVals[column.Tag]; ok {
			return errUpdate("Repeated column %v", colName)
		}

		switch val := update.Expr.(type) {
		case *sqlparser.SQLVal:
			switch val.Type {
			// Integer-like values
			case sqlparser.HexVal, sqlparser.HexNum, sqlparser.IntVal, sqlparser.BitVal:
				intVal, err := strconv.ParseInt(string(val.Val), 0, 64)
				if err != nil {
					return nil, err
				}
				switch column.Kind {
				case types.IntKind:
					setVals[column.Tag] = types.Int(intVal)
				case types.FloatKind:
					setVals[column.Tag] = types.Float(intVal)
				case types.UintKind:
					setVals[column.Tag] = types.Uint(intVal)
				default:
					return errUpdate("Type mismatch: numeric value but non-numeric column: %v", nodeToString(val))
				}
			case sqlparser.FloatVal:
				floatVal, err := strconv.ParseFloat(string(val.Val), 64)
				if err != nil {
					return nil, err
				}
				switch column.Kind {
				case types.FloatKind:
					setVals[column.Tag] = types.Float(floatVal)
				default:
					return errUpdate("Type mismatch: float value but non-float column: %v", nodeToString(val))
				}
			case sqlparser.StrVal:
				strVal := string(val.Val)
				switch column.Kind {
				case types.StringKind:
					setVals[column.Tag] = types.String(strVal)
				case types.UUIDKind:
					id, err := uuid.Parse(strVal)
					if err != nil {
						return nil, err
					}
					setVals[column.Tag] = types.UUID(id)
				default:
					return errUpdate("Type mismatch: string value but non-string column: %v", nodeToString(val))
				}
			case sqlparser.ValArg:
				return errUpdate("Value args not supported in update statements")
			default:
				return errUpdate("Unrecognized SQLVal type %v", val.Type)
			}
		case sqlparser.BoolVal:
			if column.Kind != types.BoolKind {
				return errUpdate("Type mismatch: boolean value but non-boolean column: %v", nodeToString(val))
			}
			setVals[column.Tag] = types.Bool(val)
		case *sqlparser.NullVal:
			setVals[column.Tag] = nil

		case *sqlparser.ColName:
			return errUpdate("Column name expressions not supported: %v", nodeToString(val))
		case *sqlparser.AndExpr:
			return errUpdate("And expressions not supported: %v", nodeToString(val))
		case *sqlparser.OrExpr:
			return errUpdate("Or expressions not supported: %v", nodeToString(val))
		case *sqlparser.NotExpr:
			return errUpdate("Not expressions not supported: %v", nodeToString(val))
		case *sqlparser.ParenExpr:
			return errUpdate("Parenthetical expressions not supported: %v", nodeToString(val))
		case *sqlparser.RangeCond:
			return errUpdate("Range expressions not supported: %v", nodeToString(val))
		case *sqlparser.IsExpr:
			return errUpdate("Is expressions not supported: %v", nodeToString(val))
		case *sqlparser.ExistsExpr:
			return errUpdate("Exists expressions not supported: %v", nodeToString(val))
		case *sqlparser.ValTuple:
			return errUpdate("Tuple expressions not supported: %v", nodeToString(val))
		case *sqlparser.Subquery:
			return errUpdate("Subquery expressions not supported: %v", nodeToString(val))
		case *sqlparser.ListArg:
			return errUpdate("List expressions not supported: %v", nodeToString(val))
		case *sqlparser.BinaryExpr:
			return errUpdate("Binary expressions not supported: %v", nodeToString(val))
		case *sqlparser.UnaryExpr:
			return errUpdate("Unary expressions not supported: %v", nodeToString(val))
		case *sqlparser.IntervalExpr:
			return errUpdate("Interval expressions not supported: %v", nodeToString(val))
		case *sqlparser.CollateExpr:
			return errUpdate("Collate expressions not supported: %v", nodeToString(val))
		case *sqlparser.FuncExpr:
			return errUpdate("Function expressions not supported: %v", nodeToString(val))
		case *sqlparser.CaseExpr:
			return errUpdate("Case expressions not supported: %v", nodeToString(val))
		case *sqlparser.ValuesFuncExpr:
			return errUpdate("Values func expressions not supported: %v", nodeToString(val))
		case *sqlparser.ConvertExpr:
			return errUpdate("Conversion expressions not supported: %v", nodeToString(val))
		case *sqlparser.SubstrExpr:
			return errUpdate("Substr expressions not supported: %v", nodeToString(val))
		case *sqlparser.ConvertUsingExpr:
			return errUpdate("Convert expressions not supported: %v", nodeToString(val))
		case *sqlparser.MatchExpr:
			return errUpdate("Match expressions not supported: %v", nodeToString(val))
		case *sqlparser.GroupConcatExpr:
			return errUpdate("Group concat expressions not supported: %v", nodeToString(val))
		case *sqlparser.Default:
			return errUpdate("Unrecognized expression: %v", nodeToString(val))
		default:
			return errUpdate("Unrecognized expression: %v", nodeToString(val))
		}
	}

	whereClause := s.Where
	if whereClause != nil && whereClause.Type != sqlparser.WhereStr {
		return errUpdate("Having clause not supported in update statements")
	}

	var filter filterFn
	if whereClause == nil {
		filter = func(r row.Row) bool {
			return true
		}
	} else {
		switch expr := whereClause.Expr.(type) {
		case *sqlparser.ComparisonExpr:

			left := expr.Left
			right := expr.Right
			op := expr.Operator

			colExpr := left
			valExpr := right

			// Swap the column and value expr as necessary
			colName, ok := colExpr.(*sqlparser.ColName)
			if !ok {
				colExpr = right
				valExpr = left
			}

			colName, ok = colExpr.(*sqlparser.ColName)
			if !ok {
				return errUpdate("Only column names and value literals are supported")
			}

			colNameStr := colName.Name.String()

			var sqlVal string
			switch r := valExpr.(type) {
			case *sqlparser.SQLVal:
				switch r.Type {
				// String-like values will print with quotes or other markers by default, so use the naked asci
				// bytes coerced into a string for them
				case sqlparser.HexVal, sqlparser.BitVal, sqlparser.StrVal:
					sqlVal = string(r.Val)
				default:
					// Default is to use the string value of the SQL node and hope it works
					sqlVal = nodeToString(valExpr)
				}
			default:
				// Default is to use the string value of the SQL node and hope it works
				sqlVal = nodeToString(valExpr)
			}

			col, ok := tableSch.GetAllCols().GetByName(colNameStr)
			if !ok {
				return errUpdate("%v is not a known column", colNameStr)
			}

			tag := col.Tag
			convFunc := doltcore.GetConvFunc(types.StringKind, col.Kind)
			comparisonVal, err := convFunc(types.String(string(sqlVal)))

			if err != nil {
				return errUpdate("Couldn't convert column to string: %v", err.Error())
			}

			// All the operations differ only in their filter logic
			switch op {
			case sqlparser.EqualStr:
				filter = func(r row.Row) bool {
					rowVal, ok := r.GetColVal(tag)
					if !ok {
						return false
					}
					return comparisonVal.Equals(rowVal)
				}
			case sqlparser.LessThanStr:
				filter = func(r row.Row) bool {
					rowVal, ok := r.GetColVal(tag)
					if !ok {
						return false
					}
					return rowVal.Less(comparisonVal)
				}
			case sqlparser.GreaterThanStr:
				filter = func(r row.Row) bool {
					rowVal, ok := r.GetColVal(tag)
					if !ok {
						return false
					}
					return comparisonVal.Less(rowVal)
				}
			case sqlparser.LessEqualStr:
				filter = func(r row.Row) bool {
					rowVal, ok := r.GetColVal(tag)
					if !ok {
						return false
					}
					return rowVal.Less(comparisonVal) || rowVal.Equals(comparisonVal)
				}
			case sqlparser.GreaterEqualStr:
				filter = func(r row.Row) bool {
					rowVal, ok := r.GetColVal(tag)
					if !ok {
						return false
					}
					return comparisonVal.Less(rowVal) || comparisonVal.Equals(rowVal)
				}
			case sqlparser.NotEqualStr:
				filter = func(r row.Row) bool {
					rowVal, ok := r.GetColVal(tag)
					if !ok {
						return false
					}
					return !comparisonVal.Equals(rowVal)
				}
			case sqlparser.NullSafeEqualStr:
				return errUpdate("null safe equal operation not supported")
			case sqlparser.InStr:
				return errUpdate("in keyword not supported")
			case sqlparser.NotInStr:
				return errUpdate("in keyword not supported")
			case sqlparser.LikeStr:
				return errUpdate("like keyword not supported")
			case sqlparser.NotLikeStr:
				return errUpdate("like keyword not supported")
			case sqlparser.RegexpStr:
				return errUpdate("regular expressions not supported")
			case sqlparser.NotRegexpStr:
				return errUpdate("regular expressions not supported")
			case sqlparser.JSONExtractOp:
				return errUpdate("json not supported")
			case sqlparser.JSONUnquoteExtractOp:
				return errUpdate("json not supported")
			}
		case *sqlparser.AndExpr:
			return errUpdate("And expressions not supported: %v", nodeToString(expr))
		case *sqlparser.OrExpr:
			return errUpdate("Or expressions not supported: %v", nodeToString(expr))
		case *sqlparser.NotExpr:
			return errUpdate("Not expressions not supported: %v", nodeToString(expr))
		case *sqlparser.ParenExpr:
			return errUpdate("Parenthetical expressions not supported: %v", nodeToString(expr))
		case *sqlparser.RangeCond:
			return errUpdate("Range expressions not supported: %v", nodeToString(expr))
		case *sqlparser.IsExpr:
			return errUpdate("Is expressions not supported: %v", nodeToString(expr))
		case *sqlparser.ExistsExpr:
			return errUpdate("Exists expressions not supported: %v", nodeToString(expr))
		case *sqlparser.SQLVal:
			return errUpdate("Literal expressions not supported: %v", nodeToString(expr))
		case *sqlparser.NullVal:
			return errUpdate("NULL expressions not supported: %v", nodeToString(expr))
		case *sqlparser.BoolVal:
			return errUpdate("Bool expressions not supported: %v", nodeToString(expr))
		case *sqlparser.ColName:
			return errUpdate("Column name expressions not supported: %v", nodeToString(expr))
		case *sqlparser.ValTuple:
			return errUpdate("Tuple expressions not supported: %v", nodeToString(expr))
		case *sqlparser.Subquery:
			return errUpdate("Subquery expressions not supported: %v", nodeToString(expr))
		case *sqlparser.ListArg:
			return errUpdate("List expressions not supported: %v", nodeToString(expr))
		case *sqlparser.BinaryExpr:
			return errUpdate("Binary expressions not supported: %v", nodeToString(expr))
		case *sqlparser.UnaryExpr:
			return errUpdate("Unary expressions not supported: %v", nodeToString(expr))
		case *sqlparser.IntervalExpr:
			return errUpdate("Interval expressions not supported: %v", nodeToString(expr))
		case *sqlparser.CollateExpr:
			return errUpdate("Collate expressions not supported: %v", nodeToString(expr))
		case *sqlparser.FuncExpr:
			return errUpdate("Function expressions not supported: %v", nodeToString(expr))
		case *sqlparser.CaseExpr:
			return errUpdate("Case expressions not supported: %v", nodeToString(expr))
		case *sqlparser.ValuesFuncExpr:
			return errUpdate("Values func expressions not supported: %v", nodeToString(expr))
		case *sqlparser.ConvertExpr:
			return errUpdate("Conversion expressions not supported: %v", nodeToString(expr))
		case *sqlparser.SubstrExpr:
			return errUpdate("Substr expressions not supported: %v", nodeToString(expr))
		case *sqlparser.ConvertUsingExpr:
			return errUpdate("Convert expressions not supported: %v", nodeToString(expr))
		case *sqlparser.MatchExpr:
			return errUpdate("Match expressions not supported: %v", nodeToString(expr))
		case *sqlparser.GroupConcatExpr:
			return errUpdate("Group concat expressions not supported: %v", nodeToString(expr))
		case *sqlparser.Default:
			return errUpdate("Unrecognized expression: %v", nodeToString(expr))
		default:
			return errUpdate("Unrecognized expression: %v", nodeToString(expr))
		}
	}

	// Perform the update
	var result UpdateResult
	rowData := table.GetRowData()
	me := rowData.Edit()
	rowReader := noms.NewNomsMapReader(rowData, tableSch)

	for {
		r, err := rowReader.ReadRow()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		
		if !filter(r) {
			continue
		}

		var primaryKeyColChanged bool
		var anyColChanged bool

		for tag, val := range setVals {
			// We need to know if a primay key changed values to correctly enforce key constraints (avoid overwriting
			// existing rows that are keyed to the updated value)
			currVal, _ := r.GetColVal(tag)
			column, _ := tableSch.GetAllCols().GetByTag(tag)

			if (currVal == nil && val != nil) || (currVal != nil && !currVal.Equals(val)) {
				anyColChanged = true
				if column.IsPartOfPK {
					primaryKeyColChanged = true
				}
			}

			r, err = r.SetColVal(tag, val, tableSch)
			if err != nil {
				return nil, err
			}
		}

		if !row.IsValid(r, tableSch) {
			return nil, ErrConstraintFailure
		}

		key := r.NomsMapKey(tableSch)
		// map editor reaches into the underlying table if there isn't an edit with this key
		// this logic isn't correct for all possible queries, but works for now
		if primaryKeyColChanged && me.Get(key) != nil {
			return errUpdate("Update results in duplicate primary key %v", key)
		}
		if anyColChanged {
			result.NumRowsUpdated += 1
		} else {
			result.NumRowsUnchanged += 1
		}

		me.Set(key, r.NomsMapValue(tableSch))
	}
	table = table.UpdateRows(me.Map())

	result.Root = root.PutTable(db, tableName, table)
	return &result, nil
}

func errUpdate(errorFmt string, args... interface{})  (*UpdateResult, error) {
	return nil, errors.New(fmt.Sprintf(errorFmt, args...))
}