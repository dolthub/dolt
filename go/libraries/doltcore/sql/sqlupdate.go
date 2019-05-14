package sql

import (
	"context"
	"errors"
	"fmt"
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

func ExecuteUpdate(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, s *sqlparser.Update, query string) (*UpdateResult, error) {
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

	if !root.HasTable(ctx, tableName) {
		return errUpdate("Unknown table '%s'", tableName)
	}
	table, _ := root.GetTable(ctx, tableName)
	tableSch := table.GetSchema(ctx)

	setVals := make(map[uint64]*valGetter)
	schemas := map[string]schema.Schema{tableName: tableSch}
	aliases := NewAliases()
	rss := resultset.Identity(tableSch)

	for _, update := range s.Exprs {
		colName := update.Name.Name.String()
		column, ok := tableSch.GetAllCols().GetByName(colName)
		if !ok {
			return errUpdate(UnknownColumnErrFmt, colName)
		}
		if _, ok = setVals[column.Tag]; ok {
			return errUpdate("Repeated column '%v'", colName)
		}

		// TODO: support aliases, multiple table updates
		getter, err := getterFor(update.Expr, schemas, aliases, rss)
		if err != nil {
			return nil, err
		}

		// Fill in comparison kinds before doing error checking
		if getter.Kind == SQL_VAL {
			getter.NomsKind = column.Kind
		}
		getter.CmpKind = column.Kind

		// Initialize the getters. This uses the type hints from above to enforce type constraints between columns and
		// set values.
		if err := getter.Init(); err != nil {
			return nil, err
		}

		setVals[column.Tag] = getter
	}

	// TODO: support aliases in where clauses
	filter, err := createFilterForWhere(s.Where, schemas, aliases, rss)
	if err != nil {
		return errUpdate(err.Error())
	}

	// Perform the update
	var result UpdateResult
	rowData := table.GetRowData(ctx)
	me := rowData.Edit()
	rowReader := noms.NewNomsMapReader(ctx, rowData, tableSch)

	for {
		r, err := rowReader.ReadRow(ctx)
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

		for tag, getter := range setVals {
			// We need to know if a primary key changed values to correctly enforce key constraints (avoid overwriting
			// existing rows that are keyed to the updated value)
			currVal, _ := r.GetColVal(tag)
			column, _ := tableSch.GetAllCols().GetByTag(tag)
			val := getter.Get(r)

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
		if primaryKeyColChanged && rowData.Get(ctx, key.Value(ctx)) != nil {
			return errUpdate("Update results in duplicate primary key %v", key)
		}
		if anyColChanged {
			result.NumRowsUpdated += 1
		} else {
			result.NumRowsUnchanged += 1
		}

		me.Set(key, r.NomsMapValue(tableSch))
	}
	table = table.UpdateRows(ctx, me.Map(ctx))

	result.Root = root.PutTable(ctx, db, tableName, table)
	return &result, nil
}

func errUpdate(errorFmt string, args ...interface{}) (*UpdateResult, error) {
	return nil, errors.New(fmt.Sprintf(errorFmt, args...))
}
