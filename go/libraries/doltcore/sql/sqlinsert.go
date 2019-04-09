package sql

import (
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/xwb1989/sqlparser"
	"strconv"
)

type InsertResult struct {
	Root             *doltdb.RootValue
	NumRowsInserted  int
	NumRowsUpdated   int
	NumErrorsIgnored int
}

var ErrMissingPrimaryKeys = errors.New("one or more primary key columns missing from insert statement")
var ErrConstraintFailure = errors.New("row constraint failed")

// ExecuteSelect executes the given select query and returns the resultant rows accompanied by their output schema.
func ExecuteInsert(db *doltdb.DoltDB, root *doltdb.RootValue, s *sqlparser.Insert, query string)  (*InsertResult, error) {
	tableName := s.Table.Name.String()
	if !root.HasTable(tableName) {
		return errInsert("Unknown table %v", tableName)
	}
	table, _ := root.GetTable(tableName)
	tableSch := table.GetSchema()

	// Parser supports overwrite on insert with both the replace keyword (from MySQL) as well as the ignore keyword
	replace := s.Action == sqlparser.ReplaceStr
	ignore := s.Ignore != ""

	cols := make([]schema.Column, len(s.Columns))
	for i, colName := range s.Columns {
		col, ok := tableSch.GetAllCols().GetByName(colName.String())
		if !ok {
			return errInsert("Unknown column %v", colName)
		}
		cols[i] = col
	}

	var rows []row.Row // your boat

	switch queryRows := s.Rows.(type) {
	case sqlparser.Values:
		var err error
		rows, err = prepareInsertVals(cols, &queryRows, tableSch)
		if err != nil {
			return &InsertResult{}, err
		}
	case *sqlparser.Select:
		return errInsert("Insert as select not supported")
	case *sqlparser.ParenSelect:
		return errInsert("Parenthesized select expressions in insert not supported")
	case *sqlparser.Union:
		return errInsert("Union not supported")
	default:
		return errInsert("Unrecognized type for insertRows: %v", queryRows)
	}

	// Perform the insert
	rowData := table.GetRowData()
	me := rowData.Edit()
	var result InsertResult

	for _, r := range rows {
		if !row.IsValid(r, tableSch) {
			if ignore {
				result.NumErrorsIgnored += 1
				continue
			} else {
				return nil, ErrConstraintFailure
			}
		}

		key := r.NomsMapKey(tableSch)

		rowExists := rowData.Get(key) != nil || me.Get(key) != nil
		if rowExists {
			if replace {
				result.NumRowsUpdated += 1
			} else if ignore {
				result.NumErrorsIgnored += 1
				continue
			} else {
				return errInsert("cannot insert existing row %v", r)
			}
		} else {
			result.NumRowsInserted += 1
		}

		me.Set(key, r.NomsMapValue(tableSch))
	}
	table = table.UpdateRows(me.Map())

	result.Root = root.PutTable(db, tableName, table)
	return &result, nil
}

// Returns rows to insert from the set of values given
func prepareInsertVals(cols []schema.Column, values *sqlparser.Values, tableSch schema.Schema) ([]row.Row, error) {

	// Lack of primary keys is its own special kind of failure that we can detect before creating any rows
	allKeysFound := true
	tableSch.GetPKCols().Iter(func(tag uint64, col schema.Column) bool {
		var foundCol bool
		for _, col := range cols {
			if col.Tag == tag {
				foundCol = true
				break
			}
		}

		allKeysFound = allKeysFound && foundCol
		return foundCol
	})
	if !allKeysFound {
		return nil, ErrMissingPrimaryKeys
	}

	rows := make([]row.Row, len(*values))

	for i, valTuple := range *values {
		r, err := makeRow(cols, tableSch, valTuple)
		if err != nil {
			return nil, err
		}
		rows[i] = r
	}

	return rows, nil
}

func makeRow(columns []schema.Column, tableSch schema.Schema, tuple sqlparser.ValTuple) (row.Row, error) {
	if len(columns) != len(tuple) {
		return errInsertRow("Wrong number of values for tuple %v", nodeToString(tuple))
	}

	taggedVals := make(row.TaggedValues)
	for i, expr := range tuple {
		column := columns[i]
		switch val := expr.(type) {
		case *sqlparser.SQLVal:
			switch val.Type {
			// Integer-like values
			// TODO: support uint as well
			case sqlparser.HexVal, sqlparser.HexNum, sqlparser.IntVal, sqlparser.BitVal:
				intVal, err := strconv.ParseInt(string(val.Val), 0, 64)
				if err != nil {
					return nil, err
				}
				if column.Kind == types.IntKind {
					taggedVals[column.Tag] = types.Int(intVal)
				} else if column.Kind == types.FloatKind {
					taggedVals[column.Tag] = types.Float(intVal)
				}
			case sqlparser.FloatVal:
				floatVal, err := strconv.ParseFloat(string(val.Val), 64)
				if err != nil {
					return nil, err
				}
				taggedVals[column.Tag] = types.Float(floatVal)
			case sqlparser.StrVal:
				strVal := string(val.Val)
				taggedVals[column.Tag] = types.String(strVal)
			case sqlparser.ValArg:
				return errInsertRow("Value args not supported in insert statements")
			default:
				return errInsertRow("Unrecognized SQLVal type %v", val.Type)
			}
		case *sqlparser.NullVal:
			// nothing to do, just don't set a tagged value for this column
		case sqlparser.BoolVal:
			taggedVals[column.Tag] = types.Bool(val)

		// Many of these shouldn't be possible in the grammar, but all cases included for completeness
		case *sqlparser.ComparisonExpr:
			return errInsertRow("Comparison expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.AndExpr:
			return errInsertRow("And expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.OrExpr:
			return errInsertRow("Or expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.NotExpr:
			return errInsertRow("Not expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.ParenExpr:
			return errInsertRow("Parenthetical expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.RangeCond:
			return errInsertRow("Range expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.IsExpr:
			return errInsertRow("Is expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.ExistsExpr:
			return errInsertRow("Exists expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.ColName:
			return errInsertRow("Column name expressions not supported in insert values: %v", nodeToString(tuple))
		case sqlparser.ValTuple:
			return errInsertRow("Tuple expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.Subquery:
			return errInsertRow("Subquery expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.ListArg:
			return errInsertRow("List expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.BinaryExpr:
			return errInsertRow("Binary expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.UnaryExpr:
			return errInsertRow("Unary expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.IntervalExpr:
			return errInsertRow("Interval expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.CollateExpr:
			return errInsertRow("Collate expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.FuncExpr:
			return errInsertRow("Function expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.CaseExpr:
			return errInsertRow("Case expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.ValuesFuncExpr:
			return errInsertRow("Values func expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.ConvertExpr:
			return errInsertRow("Conversion expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.SubstrExpr:
			return errInsertRow("Substr expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.ConvertUsingExpr:
			return errInsertRow("Convert expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.MatchExpr:
			return errInsertRow("Match expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.GroupConcatExpr:
			return errInsertRow("Group concat expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.Default:
			return errInsertRow("Unrecognized expression: %v", nodeToString(tuple))
		default:
			return errInsertRow("Unrecognized expression: %v", nodeToString(tuple))
		}
	}

	return row.New(tableSch, taggedVals), nil
}

// Returns an error result with return type to match ExecuteInsert
func errInsert(errorFmt string, args... interface{})  (*InsertResult, error) {
	return nil, errors.New(fmt.Sprintf(errorFmt, args...))
}

// Returns an error result with return type to match ExecuteInsert
func errInsertRow(errorFmt string, args... interface{})  (row.Row, error) {
	return nil, errors.New(fmt.Sprintf(errorFmt, args...))
}