// Copyright 2019 Liquidata, Inc.
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

package sql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type InsertResult struct {
	Root             *doltdb.RootValue
	NumRowsInserted  int
	NumRowsUpdated   int
	NumErrorsIgnored int
}

var ErrMissingPrimaryKeys = errors.New("One or more primary key columns missing from insert statement")
var ConstraintFailedFmt = "Constraint failed for column '%v': %v"

// ExecuteInsertBatch executes the given insert statement in batch mode and returns the result. The table is not changed
// until the batch is Committed. The InsertResult returned similarly doesn't have a Root set, since the root isn't
// modified by this function.
func ExecuteBatchInsert(
	ctx context.Context,
	root *doltdb.RootValue,
	s *sqlparser.Insert,
	batcher *SqlBatcher,
) (*InsertResult, error) {

	tableName := s.Table.Name.String()
	tableSch, err := batcher.GetSchema(ctx, tableName)
	if err != nil {
		return nil, err
	}

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
					return nil, fmt.Errorf("Repeated column: '%v'", c.Name)
				}
			}

			col, ok := tableSch.GetAllCols().GetByName(colName.String())
			if !ok {
				return nil, fmt.Errorf(UnknownColumnErrFmt, colName)
			}
			cols[i] = col
		}
	}

	var rows []row.Row // your boat

	switch queryRows := s.Rows.(type) {
	case sqlparser.Values:
		var err error
		rows, err = prepareInsertVals(root.VRW().Format(), cols, &queryRows, tableSch)
		if err != nil {
			return nil, err
		}
	case *sqlparser.Select:
		return nil, fmt.Errorf("Insert as select not supported")
	case *sqlparser.ParenSelect:
		return nil, fmt.Errorf("Parenthesized select expressions in insert not supported")
	case *sqlparser.Union:
		return nil, fmt.Errorf("Union not supported")
	default:
		return nil, fmt.Errorf("Unrecognized type for insert: %v", queryRows)
	}

	// Perform the insert
	var result InsertResult
	opt := InsertOptions{replace}
	for _, r := range rows {
		if has, err := row.IsValid(r, tableSch); err != nil {
			return nil, err
		} else if !has {
			if ignore {
				result.NumErrorsIgnored += 1
				continue
			} else {
				col, constraint, err := row.GetInvalidConstraint(r, tableSch)

				if err != nil {
					return nil, fmt.Errorf(ConstraintFailedFmt, "unknown", "unknown")
				}

				return nil, fmt.Errorf(ConstraintFailedFmt, col.Name, constraint)
			}
		}

		insertResult, err := batcher.Insert(ctx, tableName, r, opt)
		if err != nil {
			if ignore {
				result.NumErrorsIgnored += 1
				continue
			} else {
				return nil, err
			}
		}

		if insertResult.RowInserted {
			result.NumRowsInserted++
		}
		if insertResult.RowUpdated {
			result.NumRowsUpdated++
		}
	}

	return &result, nil
}

// ExecuteInsert executes the given select insert statement and returns the result.
func ExecuteInsert(
	ctx context.Context,
	db *doltdb.DoltDB,
	root *doltdb.RootValue,
	s *sqlparser.Insert,
) (*InsertResult, error) {

	batcher := NewSqlBatcher(db, root)
	insertResult, err := ExecuteBatchInsert(ctx, root, s, batcher)
	if err != nil {
		return nil, err
	}

	newRoot, err := batcher.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return &InsertResult{
		Root:             newRoot,
		NumRowsInserted:  insertResult.NumRowsInserted,
		NumRowsUpdated:   insertResult.NumRowsUpdated,
		NumErrorsIgnored: insertResult.NumErrorsIgnored,
	}, nil
}

// Returns a primary key summary of the row given
func getPrimaryKeyString(r row.Row, tableSch schema.Schema) string {
	var sb strings.Builder
	first := true
	err := tableSch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
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
		return false, nil
	})


	// TODO: fix panics
	if err != nil {
		panic(err)
	}

	return sb.String()
}

// Returns rows to insert from the set of values given
func prepareInsertVals(nbf *types.NomsBinFormat, cols []schema.Column, values *sqlparser.Values, tableSch schema.Schema) ([]row.Row, error) {

	// Lack of primary keys is its own special kind of failure that we can detect before creating any rows
	allKeysFound := true
	err := tableSch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		for _, insertCol := range cols {
			if insertCol.Tag == tag {
				return false, nil
			}
		}
		allKeysFound = false
		return true, nil
	})

	if err != nil {
		return nil, err
	}

	if !allKeysFound {
		return nil, ErrMissingPrimaryKeys
	}

	rows := make([]row.Row, len(*values))

	for i, valTuple := range *values {
		r, err := makeRow(nbf, cols, tableSch, valTuple)
		if err != nil {
			return nil, err
		}
		rows[i] = r
	}

	return rows, nil
}

func makeRow(nbf *types.NomsBinFormat, columns []schema.Column, tableSch schema.Schema, tuple sqlparser.ValTuple) (row.Row, error) {
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

	return row.New(nbf, tableSch, taggedVals)
}

// Returns an error result with return type to match ExecuteInsert
func errInsert(errorFmt string, args ...interface{}) (*InsertResult, error) {
	return nil, errors.New(fmt.Sprintf(errorFmt, args...))
}

// Returns an error result with return type to match ExecuteInsert
func errInsertRow(errorFmt string, args ...interface{}) (row.Row, error) {
	return nil, errors.New(fmt.Sprintf(errorFmt, args...))
}
