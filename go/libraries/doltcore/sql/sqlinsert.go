package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
	"github.com/xwb1989/sqlparser"
	"strings"
)

type InsertResult struct {
	Root             *doltdb.RootValue
	NumRowsInserted  int
	NumRowsUpdated   int
	NumErrorsIgnored int
}

var ErrMissingPrimaryKeys = errors.New("One or more primary key columns missing from insert statement")
var ConstraintFailedFmt = "Constraint failed for column '%v': %v"

// ExecuteSelect executes the given select query and returns the resultant rows accompanied by their output schema.
func ExecuteInsert(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, s *sqlparser.Insert, query string) (*InsertResult, error) {
	tableName := s.Table.Name.String()
	if !root.HasTable(ctx, tableName) {
		return errInsert("Unknown table %v", tableName)
	}
	table, _ := root.GetTable(ctx, tableName)
	tableSch := table.GetSchema(ctx)

	// Parser supports overwrite on insert with both the replace keyword (from MySQL) as well as the ignore keyword
	replace := s.Action == sqlparser.ReplaceStr
	ignore := s.Ignore != ""

	// Get the list of columns to insert into. We support both naked inserts (no column list specified) as well as
	// explicit column lists.
	var cols []schema.Column
	if s.Columns == nil || len(s.Columns) == 0 {
		cols = tableSch.GetAllCols().GetColumns()
	} else {
		cols = make([]schema.Column, len(s.Columns))
		for i, colName := range s.Columns {
			for _, c := range cols {
				if c.Name == colName.String() {
					return errInsert("Repeated column: '%v'", c.Name)
				}
			}

			col, ok := tableSch.GetAllCols().GetByName(colName.String())
			if !ok {
				return errInsert(UnknownColumnErrFmt, colName)
			}
			cols[i] = col
		}
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
	rowData := table.GetRowData(ctx)
	me := rowData.Edit()
	var result InsertResult

	insertedPKHashes := make(map[hash.Hash]struct{})
	for _, r := range rows {
		if !row.IsValid(r, tableSch) {
			if ignore {
				result.NumErrorsIgnored += 1
				continue
			} else {
				col, constraint := row.GetInvalidConstraint(r, tableSch)
				return nil, errFmt(ConstraintFailedFmt, col.Name, constraint)
			}
		}

		key := r.NomsMapKey(tableSch).Value(ctx)

		rowExists := rowData.Get(ctx, key) != nil
		_, rowInserted := insertedPKHashes[key.Hash()]

		if rowExists || rowInserted {
			if replace {
				result.NumRowsUpdated += 1
			} else if ignore {
				result.NumErrorsIgnored += 1
				continue
			} else {
				return errInsert("Duplicate primary key: '%v'", getPrimaryKeyString(r, tableSch))
			}
		}
		me.Set(key, r.NomsMapValue(tableSch))

		insertedPKHashes[key.Hash()] = struct{}{}
	}
	newMap := me.Map(ctx)
	table = table.UpdateRows(ctx, newMap)

	result.NumRowsInserted = int(newMap.Len() - rowData.Len())
	result.Root = root.PutTable(ctx, db, tableName, table)
	return &result, nil
}

// Returns a primary key summary of the row given
func getPrimaryKeyString(r row.Row, tableSch schema.Schema) string {
	var sb strings.Builder
	first := true
	tableSch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		if !first {
			sb.WriteString(", ")
		}
		sb.WriteString(col.Name)
		sb.WriteString(": ")
		val, ok := r.GetColVal(tag)
		if ok {
			sb.WriteString(fmt.Sprintf("%v", val))
		} else {
			sb.WriteString("null")
		}

		first = false
		return false
	})

	return sb.String()
}

// Returns rows to insert from the set of values given
func prepareInsertVals(cols []schema.Column, values *sqlparser.Values, tableSch schema.Schema) ([]row.Row, error) {

	// Lack of primary keys is its own special kind of failure that we can detect before creating any rows
	allKeysFound := true
	tableSch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		for _, insertCol := range cols {
			if insertCol.Tag == tag {
				return false
			}
		}
		allKeysFound = false
		return true
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
			nomsVal, err := extractNomsValueFromSQLVal(val, column.Kind)
			if err != nil {
				return nil, err
			}
			taggedVals[column.Tag] = nomsVal
		case *sqlparser.NullVal:
			// nothing to do, just don't set a tagged value for this column
		case sqlparser.BoolVal:
			if column.Kind != types.BoolKind {
				return errInsertRow("Type mismatch: boolean value but non-boolean column: %v", nodeToString(val))
			}
			taggedVals[column.Tag] = types.Bool(val)
		case *sqlparser.UnaryExpr:
			nomsVal, err := extractNomsValueFromUnaryExpr(val, column.Kind)
			if err != nil {
				return nil, err
			}
			taggedVals[column.Tag] = nomsVal

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
			// unquoted strings are interpreted by the parser as column names, give a hint
			return errInsertRow("Column name expressions not supported in insert values. Did you forget to quote a string? %v", nodeToString(tuple))
		case sqlparser.ValTuple:
			return errInsertRow("Tuple expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.Subquery:
			return errInsertRow("Subquery expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.ListArg:
			return errInsertRow("List expressions not supported in insert values: %v", nodeToString(tuple))
		case *sqlparser.BinaryExpr:
			return errInsertRow("Binary expressions not supported in insert values: %v", nodeToString(tuple))
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
func errInsert(errorFmt string, args ...interface{}) (*InsertResult, error) {
	return nil, errors.New(fmt.Sprintf(errorFmt, args...))
}

// Returns an error result with return type to match ExecuteInsert
func errInsertRow(errorFmt string, args ...interface{}) (row.Row, error) {
	return nil, errors.New(fmt.Sprintf(errorFmt, args...))
}
