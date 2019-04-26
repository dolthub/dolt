package sql

import (
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/resultset"
	"github.com/xwb1989/sqlparser"
	"io"
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
			nomsVal, err := extractNomsValueFromSQLVal(val, column.Kind)
			if err != nil {
				return nil, err
			}
			setVals[column.Tag] = nomsVal
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

	// TODO: support aliases in update
	filter, err := createFilterForWhere(s.Where, map[string]schema.Schema{tableName: tableSch}, NewAliases(), resultset.Identity(tableSch))
	if err != nil {
		return errUpdate(err.Error())
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